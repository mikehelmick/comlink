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
	"fmt"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Membership is the ordered participant set for a conversation.
// Slot order is **insertion order** (PLAN §2.10.1): the original
// Members in input order, then each successful MemberAdd appended
// at the end. New slots always append, never insert in the middle.
// This keeps vector clocks shape-coordination-safe: an old shorter
// vector is just a prefix of the new shape and lazy zero-padding
// at the end is correct.
//
// Vector clocks (vector.go) are indexed by this slot order.
// Freeze marks a slot as no-longer-receiving but keeps it in
// place so vector indexing stays stable.
//
// Membership is not safe for concurrent mutation; the owning
// Conversation GenServer serializes access.
type Membership struct {
	replicas []*pb.ReplicaID
	frozen   []bool
	// slotIndex maps string(ReplicaID.value) -> slot index for
	// O(1) SlotOf.
	slotIndex map[string]int
}

// NewMembership returns a Membership initialized from replicas.
// Slots are assigned in INPUT order (replicas[0] -> slot 0,
// replicas[1] -> slot 1, ...). The input slice is copied; the
// original is not mutated.
func NewMembership(replicas []*pb.ReplicaID) *Membership {
	m := &Membership{
		replicas:  make([]*pb.ReplicaID, len(replicas)),
		frozen:    make([]bool, len(replicas)),
		slotIndex: make(map[string]int, len(replicas)),
	}
	for i, r := range replicas {
		m.replicas[i] = proto.Clone(r).(*pb.ReplicaID)
		m.slotIndex[string(r.GetValue())] = i
	}
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
	idx, ok := m.slotIndex[string(r.GetValue())]
	if !ok {
		return -1
	}
	return idx
}

// Add appends a new ReplicaID at the end of the slot order,
// returning the new slot index. Returns an error if the replica
// is already present.
//
// PLAN §2.10.1 invariant: VoteIn ordering through the membership
// protocol guarantees every replica's local Add happens at the
// same logical point, so all replicas agree on slot order.
func (m *Membership) Add(r *pb.ReplicaID) (int, error) {
	val := r.GetValue()
	if existing, present := m.slotIndex[string(val)]; present {
		return -1, fmt.Errorf("psync: Add: replica %x already present at slot %d", val, existing)
	}
	cloned := proto.Clone(r).(*pb.ReplicaID)
	idx := len(m.replicas)
	m.replicas = append(m.replicas, cloned)
	m.frozen = append(m.frozen, false)
	m.slotIndex[string(val)] = idx
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

// Clone returns a deep copy of this Membership — same slot
// order, same frozen flags. Safe for the caller to mutate
// independently. Used by Conversation.Membership to hand out
// snapshots from the genserver.
func (m *Membership) Clone() *Membership {
	out := &Membership{
		replicas:  make([]*pb.ReplicaID, len(m.replicas)),
		slotIndex: make(map[string]int, len(m.slotIndex)),
		frozen:    make([]bool, len(m.frozen)),
	}
	for i, r := range m.replicas {
		out.replicas[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	for k, v := range m.slotIndex {
		out.slotIndex[k] = v
	}
	copy(out.frozen, m.frozen)
	return out
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
