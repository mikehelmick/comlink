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

// StabilityChecker decides whether a context-graph node is stable.
//
// PLAN §2.10 (and paper §4.2.2) explicitly call out that there are
// TWO definitions of stability in Consul:
//
//   - The standard one (paper §2.3, this file's StandardChecker):
//     M is stable iff every other ACTIVE participant has sent a
//     message in M's context.
//
//   - The membership-only one (paper §4.2.2): M is stable iff
//     every participant NOT IN SuspectDownList has sent a message
//     in M's context. Used internally by the Membership protocol
//     (Phase 3) so it can make progress while some members are
//     suspected-down.
//
// We expose this as an interface so Membership can plug in its own
// implementation without us conflating the two definitions in
// the rest of Psync.
type StabilityChecker interface {
	// IsStable reports whether node is stable per this checker's
	// rule. Pure-function: must depend only on the graph and node,
	// not on any external state.
	IsStable(g *Graph, node *Node) bool
}

// StandardChecker implements the paper §2.3 stability rule: a node
// is stable iff every active (non-frozen, non-self) participant has
// produced a message whose vector clock acknowledges the node's
// sender at or past the node's sender_seq.
type StandardChecker struct{}

// IsStable implements StabilityChecker.
func (StandardChecker) IsStable(g *Graph, n *Node) bool {
	if n == nil {
		return false
	}
	m := g.Membership()
	for slot := 0; slot < m.Len(); slot++ {
		if slot == n.SenderSlot {
			continue
		}
		if m.IsFrozen(slot) {
			continue
		}
		latest := latestVectorAtSlot(g, m, slot)
		if latest == nil {
			return false
		}
		// latest's vector at sender's slot must be >= node's seq.
		if latest[n.SenderSlot] < n.SenderSeq {
			return false
		}
	}
	return true
}

// IsStable is a convenience for the standard checker.
func IsStable(g *Graph, n *Node) bool {
	return StandardChecker{}.IsStable(g, n)
}

// StableNodes returns every node in the graph that the checker
// considers stable.
func StableNodes(g *Graph, sc StabilityChecker) []*Node {
	var out []*Node
	for _, w := range g.Waves() {
		for _, n := range g.MessagesInWave(w) {
			if sc.IsStable(g, n) {
				out = append(out, n)
			}
		}
	}
	return out
}

// WaveComplete reports whether wave w is complete by the paper's
// §2.3 sufficient condition: some message in wave w is stable.
//
// Caveat: in deployments where replicas send strictly one message
// per logical step (the model the paper implicitly assumes), this
// is also necessary. In the more general case, a wave may contain
// messages that are not pairwise concurrent — see graph.go's wave
// numbering note. SemOrder's exact wave-completion needs are
// handled by Phase 2 on top of this primitive.
func WaveComplete(g *Graph, w uint64, sc StabilityChecker) bool {
	for _, n := range g.MessagesInWave(w) {
		if sc.IsStable(g, n) {
			return true
		}
	}
	return false
}

// latestVectorAtSlot returns the highest-seq message's vector clock
// from the participant at slot, or nil if the participant has sent
// nothing in the graph.
func latestVectorAtSlot(g *Graph, m *Membership, slot int) Vector {
	r := m.Replica(slot)
	latestSeq := g.LatestSeq(r.GetValue())
	if latestSeq == 0 {
		return nil
	}
	n := g.Lookup(r.GetValue(), latestSeq)
	if n == nil {
		return nil
	}
	return Vector(n.Envelope.GetId().GetVectorClock())
}
