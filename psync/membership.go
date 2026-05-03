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
	"bytes"
	"fmt"
	"slices"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Membership is the ordered participant set for a conversation.
// Slot order is the sort order of ReplicaID byte values, fixed at
// the time the membership was last reshaped. Vector clocks (vector.go)
// are indexed by this slot order.
//
// PLAN §2.10.1: vectors only ever grow over the lifetime of a
// conversation — Add inserts at the sorted position and shifts
// subsequent slots; Freeze marks a slot as no-longer-receiving but
// keeps it in the order so vector indexing stays stable for in-
// flight messages.
//
// Membership is not safe for concurrent mutation; the owning
// Conversation GenServer serializes access.
type Membership struct {
	// replicas in sorted order; stable across reads.
	replicas []*pb.ReplicaID
	// frozen[i] reports whether slot i is frozen (member removed).
	frozen []bool
}

// NewMembership returns a Membership initialized from replicas.
// Input is copied and sorted; the original slice is not mutated.
func NewMembership(replicas []*pb.ReplicaID) *Membership {
	m := &Membership{
		replicas: make([]*pb.ReplicaID, len(replicas)),
		frozen:   make([]bool, len(replicas)),
	}
	for i, r := range replicas {
		m.replicas[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	slices.SortFunc(m.replicas, func(a, b *pb.ReplicaID) int {
		return bytes.Compare(a.GetValue(), b.GetValue())
	})
	return m
}

// Len returns the number of slots (including frozen ones).
func (m *Membership) Len() int { return len(m.replicas) }

// Replica returns a clone of the ReplicaID at slot i.
func (m *Membership) Replica(i int) *pb.ReplicaID {
	return proto.Clone(m.replicas[i]).(*pb.ReplicaID)
}

// IsFrozen reports whether the slot at index i is frozen.
func (m *Membership) IsFrozen(i int) bool { return m.frozen[i] }

// SlotOf returns the slot index for replica r, or -1 if r is not in
// the membership.
func (m *Membership) SlotOf(r *pb.ReplicaID) int {
	val := r.GetValue()
	idx, found := slices.BinarySearchFunc(m.replicas, val, func(a *pb.ReplicaID, target []byte) int {
		return bytes.Compare(a.GetValue(), target)
	})
	if !found {
		return -1
	}
	return idx
}

// Add inserts a new ReplicaID into the sorted slot order, returning
// the slot index it was inserted at. Returns an error if the
// replica is already present.
//
// Per PLAN §2.10.1, callers (the membership protocol in Phase 3)
// must ensure that all replicas process the corresponding
// MemberAdd message at the same partial-order point so vector
// reshapes happen consistently.
func (m *Membership) Add(r *pb.ReplicaID) (int, error) {
	val := r.GetValue()
	idx, found := slices.BinarySearchFunc(m.replicas, val, func(a *pb.ReplicaID, target []byte) int {
		return bytes.Compare(a.GetValue(), target)
	})
	if found {
		return -1, fmt.Errorf("psync: Add: replica %x already present at slot %d", val, idx)
	}
	cloned := proto.Clone(r).(*pb.ReplicaID)
	m.replicas = slices.Insert(m.replicas, idx, cloned)
	m.frozen = slices.Insert(m.frozen, idx, false)
	return idx, nil
}

// Freeze marks the slot for replica r as frozen (no further
// messages from that replica will be accepted; its slot remains in
// the order so existing vector clocks stay valid). Returns an error
// if r is not present or is already frozen.
func (m *Membership) Freeze(r *pb.ReplicaID) error {
	idx := m.SlotOf(r)
	if idx < 0 {
		return fmt.Errorf("psync: Freeze: replica %x not in membership", r.GetValue())
	}
	if m.frozen[idx] {
		return fmt.Errorf("psync: Freeze: replica %x already frozen at slot %d", r.GetValue(), idx)
	}
	m.frozen[idx] = true
	return nil
}

// Replicas returns clones of the active (non-frozen) replicas in
// slot order.
func (m *Membership) Replicas() []*pb.ReplicaID {
	out := make([]*pb.ReplicaID, 0, len(m.replicas))
	for i, r := range m.replicas {
		if m.frozen[i] {
			continue
		}
		out = append(out, proto.Clone(r).(*pb.ReplicaID))
	}
	return out
}

// AllReplicas returns clones of all slots, frozen or not.
func (m *Membership) AllReplicas() []*pb.ReplicaID {
	out := make([]*pb.ReplicaID, len(m.replicas))
	for i, r := range m.replicas {
		out[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	return out
}

// SenderSeq extracts the sender's own seq from a MessageID's vector
// clock — i.e. vector[SlotOf(sender)]. Returns -1 (and an error) if
// the sender is not in this membership view.
//
// This is the value the MessageLog needs as its `senderSeq`
// argument; per PLAN §2.10 the log itself does not know slot order,
// so this conversion happens at the Psync layer.
func (m *Membership) SenderSeq(id *pb.MessageID) (uint64, error) {
	slot := m.SlotOf(id.GetSender())
	if slot < 0 {
		return 0, fmt.Errorf("psync: SenderSeq: sender %x not in membership", id.GetSender().GetValue())
	}
	vc := id.GetVectorClock()
	if slot >= len(vc) {
		return 0, fmt.Errorf("psync: SenderSeq: vector_clock has %d entries; sender slot %d out of range (PLAN §2.10.1: vector shorter than membership = malformed message)", len(vc), slot)
	}
	return vc[slot], nil
}
