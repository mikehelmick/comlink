// Copyright 2026 the comlink authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package psync

import (
	"errors"
	"fmt"
	"slices"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// ErrMissingParents is returned by Graph.Insert when one or more
// causal predecessors of the envelope are not yet in the graph. The
// envelope is NOT inserted; the caller is expected to fetch the
// missing parents (lost-message protocol) and retry Insert.
var ErrMissingParents = errors.New("psync: missing parents")

// ErrAlreadyPresent is returned by Graph.Insert when an envelope
// with the same MessageID is already in the graph. The graph is
// unchanged.
var ErrAlreadyPresent = errors.New("psync: envelope already in graph")

// ErrUnknownSender is returned when an envelope's sender is not in
// the membership view.
var ErrUnknownSender = errors.New("psync: sender not in membership")

// ErrMalformedVector is returned when an envelope's vector clock
// length does not match the membership view, or its sender slot is
// out of range, etc.
var ErrMalformedVector = errors.New("psync: malformed vector clock")

// Node is one envelope's record in the context graph. Parents are
// the immediate causal predecessors (one per slot that has a non-
// zero, non-self-tail value); Children are populated as later
// messages cite this node as a parent.
//
// The fields Envelope, SenderSeq, SenderSlot, and Wave are stable
// after Insert returns. Parents and Children may not be mutated by
// callers.
type Node struct {
	Envelope   *pb.Envelope
	SenderSeq  uint64
	SenderSlot int
	// Wave is the wave number this node belongs to, defined as
	// max(vector_clock). Two nodes with the same Wave may or may
	// not be concurrent (see vector.go). Wave-completion semantics
	// land in stability.go (Phase 1(c)).
	Wave uint64

	Parents  []*Node
	Children []*Node
}

// MissingParent identifies a causal predecessor that the receiver
// does not yet have locally. The caller will request these via the
// lost-message protocol.
type MissingParent struct {
	Sender *pb.ReplicaID
	Seq    uint64
}

// Graph is the in-memory context-graph DAG for a single
// conversation. Not safe for concurrent use; the owning
// Conversation GenServer serializes access.
//
// Phase 1 assumes a fixed membership shape — the Membership passed
// to NewGraph is used unchanged for the graph's lifetime. Phase 3
// (Membership) introduces reshape, at which point this type will
// gain shape-evolution methods.
type Graph struct {
	membership *Membership
	// bySender[string(senderReplicaBytes)][senderSeq] = node
	bySender map[string]map[uint64]*Node
	// byWave[wave] = nodes belonging to that wave
	byWave map[uint64][]*Node
	count  int
}

// NewGraph returns an empty Graph operating against the given
// membership view.
func NewGraph(m *Membership) *Graph {
	return &Graph{
		membership: m,
		bySender:   make(map[string]map[uint64]*Node),
		byWave:     make(map[uint64][]*Node),
	}
}

// Membership returns the Membership view this graph was constructed
// against. Useful for callers that need to convert MessageIDs to
// (sender, sender_seq) tuples.
func (g *Graph) Membership() *Membership { return g.membership }

// Size returns the number of nodes in the graph.
func (g *Graph) Size() int { return g.count }

// Has reports whether the graph contains a node for (sender, seq).
func (g *Graph) Has(sender []byte, seq uint64) bool {
	bySeq, ok := g.bySender[string(sender)]
	if !ok {
		return false
	}
	_, ok = bySeq[seq]
	return ok
}

// Lookup returns the node for (sender, seq), or nil if absent.
func (g *Graph) Lookup(sender []byte, seq uint64) *Node {
	bySeq, ok := g.bySender[string(sender)]
	if !ok {
		return nil
	}
	return bySeq[seq]
}

// Insert adds env to the graph. The envelope's vector clock is used
// to derive the immediate causal parents; if any parent is not yet
// in the graph, ErrMissingParents is returned along with the list,
// and the envelope is NOT inserted.
//
// Vector length tolerance (PLAN §2.10.1):
//   - Shorter than membership: lazy-pad with zero at higher slots
//     (an "old-era" message that predates a MemberAdd).
//   - Longer than membership: the message is from a future era
//     (the sender has applied a MemberAdd we haven't). Insert
//     returns ErrMissingParents requesting the sender's previous
//     message; following that predecessor chain pulls in the
//     MemberAdd via the standard lost-message protocol, after
//     which our Membership grows and a re-Insert succeeds.
//
// Other errors:
//   - ErrAlreadyPresent: an envelope with the same (sender,
//     sender_seq) is already in the graph; the existing node is
//     returned alongside.
//   - ErrUnknownSender / ErrMalformedVector: structural problems
//     with the envelope — no insertion.
func (g *Graph) Insert(env *pb.Envelope) (*Node, []MissingParent, error) {
	id := env.GetId()
	if id == nil || id.GetSender() == nil {
		return nil, nil, fmt.Errorf("%w: nil id or sender", ErrMalformedVector)
	}
	senderSlot := g.membership.SlotOf(id.GetSender())
	if senderSlot < 0 {
		return nil, nil, fmt.Errorf("%w: %x", ErrUnknownSender, id.GetSender().GetValue())
	}
	vc := id.GetVectorClock()
	memLen := g.membership.Len()
	// senderSlot must be within the message's vector — otherwise
	// we can't extract the sender's seq.
	if senderSlot >= len(vc) {
		return nil, nil, fmt.Errorf("%w: sender slot %d not in vector_clock (len %d)", ErrMalformedVector, senderSlot, len(vc))
	}
	senderSeq := vc[senderSlot]
	if senderSeq == 0 {
		return nil, nil, fmt.Errorf("%w: sender's own slot must be > 0", ErrMalformedVector)
	}

	// Already present?
	if existing := g.Lookup(id.GetSender().GetValue(), senderSeq); existing != nil {
		return existing, nil, ErrAlreadyPresent
	}

	// Future-era message (vector longer than our membership): the
	// sender has applied a MemberAdd we haven't. Return the
	// sender's previous message as a missing parent — the
	// predecessor chain leads back through the MemberAdd, which
	// the lost-message protocol will pull in. Once we apply
	// MemberAdd our membership grows and re-Insert succeeds with
	// matching shape.
	if len(vc) > memLen {
		// Sender's prior message is the only synthetic predecessor
		// we can name without knowing the future-slot replica IDs.
		// If sender just started (senderSeq == 1), we cannot
		// name any parent — bail with a malformed-vector error.
		if senderSeq <= 1 {
			return nil, nil, fmt.Errorf("%w: future-era message with no derivable predecessor", ErrMalformedVector)
		}
		return nil, []MissingParent{{
			Sender: proto.Clone(id.GetSender()).(*pb.ReplicaID),
			Seq:    senderSeq - 1,
		}}, ErrMissingParents
	}

	// Derive parents and detect missing. We iterate up to memLen
	// (lazy-padding any vector slots beyond vc with zero).
	parents := make([]*Node, 0, memLen)
	var missing []MissingParent
	for i := 0; i < memLen; i++ {
		var depSeq uint64
		if i < len(vc) {
			depSeq = vc[i]
		}
		var requiredSeq uint64
		if i == senderSlot {
			if senderSeq <= 1 {
				continue
			}
			requiredSeq = senderSeq - 1
		} else {
			if depSeq == 0 {
				continue
			}
			requiredSeq = depSeq
		}
		parentReplica := g.membership.Replica(i)
		parentNode := g.Lookup(parentReplica.GetValue(), requiredSeq)
		if parentNode == nil {
			missing = append(missing, MissingParent{
				Sender: parentReplica,
				Seq:    requiredSeq,
			})
			continue
		}
		parents = append(parents, parentNode)
	}
	if len(missing) > 0 {
		return nil, missing, ErrMissingParents
	}

	wave := waveOf(Vector(vc))
	n := &Node{
		Envelope:   env,
		SenderSeq:  senderSeq,
		SenderSlot: senderSlot,
		Wave:       wave,
		Parents:    parents,
	}
	for _, p := range parents {
		p.Children = append(p.Children, n)
	}
	bySeq, ok := g.bySender[string(id.GetSender().GetValue())]
	if !ok {
		bySeq = make(map[uint64]*Node)
		g.bySender[string(id.GetSender().GetValue())] = bySeq
	}
	bySeq[senderSeq] = n
	g.byWave[wave] = append(g.byWave[wave], n)
	g.count++
	return n, nil, nil
}

// MessagesInWave returns the nodes belonging to the given wave
// number. The slice is read-only — do not mutate.
func (g *Graph) MessagesInWave(w uint64) []*Node {
	return g.byWave[w]
}

// Waves returns the set of wave numbers present in the graph, in
// ascending order.
func (g *Graph) Waves() []uint64 {
	out := make([]uint64, 0, len(g.byWave))
	for w := range g.byWave {
		out = append(out, w)
	}
	slices.Sort(out)
	return out
}

// Leaves returns the set of nodes that have no children — i.e., the
// current "tips" of the DAG. The restart protocol uses this set
// (paper §2.3: "transmits its current set of leaf nodes to the
// process that generated the restart message").
func (g *Graph) Leaves() []*Node {
	var out []*Node
	for _, bySeq := range g.bySender {
		for _, n := range bySeq {
			if len(n.Children) == 0 {
				out = append(out, n)
			}
		}
	}
	return out
}

// LatestSeq returns the highest seq from sender currently in the
// graph, or 0 if no message from sender is present.
func (g *Graph) LatestSeq(sender []byte) uint64 {
	bySeq, ok := g.bySender[string(sender)]
	if !ok {
		return 0
	}
	var max uint64
	for s := range bySeq {
		if s > max {
			max = s
		}
	}
	return max
}

// waveOf returns the wave number for a vector clock: the maximum
// over its entries.
func waveOf(v Vector) uint64 {
	var m uint64
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}
