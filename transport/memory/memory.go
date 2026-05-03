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

// Package memory provides a deterministic in-process transport for
// tests. A single Scheduler routes messages between any number of
// connected Networks, with controllable loss/reorder/partition
// injection.
//
// Determinism: all sends queue messages on the Scheduler synchronously
// (Send returns immediately without involving goroutines). Test code
// then drives delivery by calling Step or RunAll. Given a fixed seed,
// the same test produces the exact same interleaving across runs —
// which is what makes Psync's correctness invariants (causal order
// under loss/reorder) testable.
package memory

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	"google.golang.org/protobuf/proto"
)

// PartitionRule reports whether a message from -> to should be
// dropped (true = blocked).
type PartitionRule func(from, to *pb.ReplicaID) bool

// Scheduler is the central router for an in-process transport. Build
// per-replica Networks via Connect, then drive delivery via Step or
// RunAll.
type Scheduler struct {
	mu         sync.Mutex
	rng        *rand.Rand
	pending    []pendingMsg
	networks   map[string]*memNetwork
	partitions []PartitionRule
	dropProb   float64
	reorder    bool
	closed     bool
}

type pendingMsg struct {
	from    *pb.ReplicaID
	to      *pb.ReplicaID
	payload []byte
}

// NewScheduler returns a new Scheduler seeded for deterministic
// reproducibility.
func NewScheduler(seed uint64) *Scheduler {
	return &Scheduler{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		networks: make(map[string]*memNetwork),
	}
}

// Connect registers replica and returns its Network handle. Connecting
// the same replica twice returns an error.
func (s *Scheduler) Connect(replica *pb.ReplicaID) (transport.Network, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, transport.ErrClosed
	}
	key := string(replica.GetValue())
	if _, exists := s.networks[key]; exists {
		return nil, errors.New("memory: replica already connected")
	}
	n := &memNetwork{
		sched:   s,
		replica: proto.Clone(replica).(*pb.ReplicaID),
		recv:    make(chan transport.Inbound, 1024),
	}
	s.networks[key] = n
	return n, nil
}

// Pending returns the number of queued, undelivered messages.
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// SetDropProb sets the per-message drop probability in [0, 1]. The
// drop check is evaluated when a message is delivered (Step), not
// when it is sent.
func (s *Scheduler) SetDropProb(p float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropProb = p
}

// SetReorder controls FIFO vs random selection in Step.
func (s *Scheduler) SetReorder(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reorder = enabled
}

// AddPartition installs a rule that drops matching messages on
// delivery. Multiple rules are OR'd.
func (s *Scheduler) AddPartition(rule PartitionRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions = append(s.partitions, rule)
}

// ClearPartitions removes all partition rules (e.g. to model a heal).
func (s *Scheduler) ClearPartitions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions = nil
}

// Step delivers (or drops) a single queued message and returns true.
// Returns false when the queue is empty.
func (s *Scheduler) Step() bool {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return false
	}
	idx := 0
	if s.reorder && len(s.pending) > 1 {
		idx = s.rng.IntN(len(s.pending))
	}
	msg := s.pending[idx]
	s.pending = append(s.pending[:idx], s.pending[idx+1:]...)
	dropped := s.shouldDropLocked(msg)
	target := s.networks[string(msg.to.GetValue())]
	s.mu.Unlock()

	if dropped || target == nil {
		return true
	}
	// Non-blocking send: if the receiver hasn't drained, drop on the
	// floor rather than block the scheduler. Tests that depend on
	// delivery should keep up with Recv.
	select {
	case target.recv <- transport.Inbound{From: proto.Clone(msg.from).(*pb.ReplicaID), Payload: msg.payload}:
	default:
	}
	return true
}

// RunAll delivers every queued message until the queue empties. New
// messages enqueued during delivery (e.g. by other goroutines) are
// also processed.
func (s *Scheduler) RunAll() {
	for s.Step() {
	}
}

// shouldDropLocked applies dropProb and partition rules. Caller must
// hold s.mu.
func (s *Scheduler) shouldDropLocked(msg pendingMsg) bool {
	if s.dropProb > 0 && s.rng.Float64() < s.dropProb {
		return true
	}
	for _, rule := range s.partitions {
		if rule(msg.from, msg.to) {
			return true
		}
	}
	return false
}

// Close shuts down the scheduler and closes every connected
// Network's Recv.
func (s *Scheduler) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	pending := s.pending
	s.pending = nil
	networks := s.networks
	s.networks = nil
	s.mu.Unlock()
	_ = pending
	for _, n := range networks {
		n.markClosed()
	}
	return nil
}

// memNetwork is a per-replica handle issued by Connect.
type memNetwork struct {
	sched    *Scheduler
	replica  *pb.ReplicaID
	recv     chan transport.Inbound
	closeMu  sync.Mutex
	isClosed bool
}

// Local returns this network's replica id.
func (n *memNetwork) Local() *pb.ReplicaID {
	return proto.Clone(n.replica).(*pb.ReplicaID)
}

// Send queues payload for delivery to peer.
func (n *memNetwork) Send(ctx context.Context, peer *pb.ReplicaID, payload []byte) error {
	n.closeMu.Lock()
	if n.isClosed {
		n.closeMu.Unlock()
		return transport.ErrClosed
	}
	n.closeMu.Unlock()

	if peer == nil {
		return transport.ErrUnknownPeer
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	clonedPayload := bytes.Clone(payload)
	n.sched.mu.Lock()
	if n.sched.closed {
		n.sched.mu.Unlock()
		return transport.ErrClosed
	}
	if _, ok := n.sched.networks[string(peer.GetValue())]; !ok {
		n.sched.mu.Unlock()
		return transport.ErrUnknownPeer
	}
	n.sched.pending = append(n.sched.pending, pendingMsg{
		from:    proto.Clone(n.replica).(*pb.ReplicaID),
		to:      proto.Clone(peer).(*pb.ReplicaID),
		payload: clonedPayload,
	})
	n.sched.mu.Unlock()
	return nil
}

// Recv returns the inbound channel.
func (n *memNetwork) Recv() <-chan transport.Inbound {
	return n.recv
}

// Close detaches this Network from the Scheduler and closes Recv.
func (n *memNetwork) Close() error {
	n.markClosed()
	n.sched.mu.Lock()
	delete(n.sched.networks, string(n.replica.GetValue()))
	n.sched.mu.Unlock()
	return nil
}

func (n *memNetwork) markClosed() {
	n.closeMu.Lock()
	defer n.closeMu.Unlock()
	if n.isClosed {
		return
	}
	n.isClosed = true
	close(n.recv)
}
