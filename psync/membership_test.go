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

package psync_test

import (
	"bytes"
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
)

func r(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// TestNewMembershipPreservesInsertionOrder: PLAN §2.10.1 — slot
// order is the input order, NOT sorted-by-ReplicaID. New slots
// from later Add calls append to the end.
func TestNewMembershipPreservesInsertionOrder(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("zoe"), r("alice"), r("bob"), r("carol")})
	want := []string{"zoe", "alice", "bob", "carol"}
	for i, name := range want {
		got := m.Replica(i).GetValue()
		if !bytes.HasPrefix(got, []byte(name)) {
			t.Errorf("slot %d = %q, want prefix %q", i, got, name)
		}
	}
	if m.Len() != 4 {
		t.Errorf("Len = %d, want 4", m.Len())
	}
}

func TestSlotOf(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	cases := map[string]int{"alice": 0, "bob": 1, "carol": 2, "ghost": -1}
	for tag, want := range cases {
		if got := m.SlotOf(r(tag)); got != want {
			t.Errorf("SlotOf(%q) = %d, want %d", tag, got, want)
		}
	}
}

// TestAddAppendsAtEnd: PLAN §2.10.1 — new slots always append at
// the end (insertion order), never insert in the middle.
func TestAddAppendsAtEnd(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("carol")})
	idx, err := m.Add(r("bob"))
	if err != nil {
		t.Fatal(err)
	}
	if idx != 2 {
		t.Fatalf("Add(bob) returned slot %d, want 2 (append at end)", idx)
	}
	if m.Len() != 3 {
		t.Fatalf("Len = %d, want 3", m.Len())
	}
	if got := m.SlotOf(r("alice")); got != 0 {
		t.Errorf("alice slot = %d, want 0 (unchanged)", got)
	}
	if got := m.SlotOf(r("carol")); got != 1 {
		t.Errorf("carol slot = %d, want 1 (unchanged)", got)
	}
	if got := m.SlotOf(r("bob")); got != 2 {
		t.Errorf("bob slot = %d, want 2 (appended)", got)
	}
}

func TestAddRejectsDuplicate(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice")})
	if _, err := m.Add(r("alice")); err == nil {
		t.Fatal("Add(alice) into membership already containing alice succeeded; want error")
	}
}

func TestFreezeMarksSlot(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if err := m.Freeze(r("bob")); err != nil {
		t.Fatal(err)
	}
	if !m.IsFrozen(1) {
		t.Errorf("bob slot not marked frozen")
	}
	// Slot index of carol must still be 2 (Freeze does not shift).
	if got := m.SlotOf(r("carol")); got != 2 {
		t.Errorf("carol slot after Freeze(bob) = %d, want 2 (Freeze must not renumber)", got)
	}
	// Replicas() (non-frozen view) excludes bob.
	active := m.Replicas()
	if len(active) != 2 {
		t.Fatalf("Replicas() returned %d, want 2", len(active))
	}
	for _, r := range active {
		if bytes.HasPrefix(r.GetValue(), []byte("bob")) {
			t.Errorf("Replicas() included frozen bob")
		}
	}
	// AllReplicas() still includes bob.
	all := m.AllReplicas()
	if len(all) != 3 {
		t.Fatalf("AllReplicas() returned %d, want 3", len(all))
	}
}

func TestFreezeRejectsUnknown(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice")})
	if err := m.Freeze(r("ghost")); err == nil {
		t.Fatal("Freeze(unknown) succeeded; want error")
	}
}

func TestFreezeRejectsAlreadyFrozen(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("bob")})
	_ = m.Freeze(r("alice"))
	if err := m.Freeze(r("alice")); err == nil {
		t.Fatal("double Freeze succeeded; want error")
	}
}

func TestSenderSeq(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	id := &pb.MessageID{
		Sender:      r("bob"),
		VectorClock: []uint64{2, 5, 1},
	}
	got, err := m.SenderSeq(id)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("SenderSeq = %d, want 5 (bob's slot is 1, vec[1] = 5)", got)
	}
}

func TestSenderSeqUnknownSender(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice")})
	id := &pb.MessageID{
		Sender:      r("ghost"),
		VectorClock: []uint64{1},
	}
	if _, err := m.SenderSeq(id); err == nil {
		t.Fatal("SenderSeq with unknown sender returned no error")
	}
}

func TestSenderSeqVectorTooShort(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	id := &pb.MessageID{
		Sender:      r("carol"),
		VectorClock: []uint64{1, 2}, // too short — carol's slot is 2
	}
	if _, err := m.SenderSeq(id); err == nil {
		t.Fatal("SenderSeq with vector shorter than membership returned no error")
	}
}

// TestAddDoesNotShiftExistingSlots: PLAN §2.10.1 — insertion-order
// means new slots append; existing slot indices are immutable.
// This is the property that lets old-era vectors be lazy-padded
// with zeros at the end.
func TestAddDoesNotShiftExistingSlots(t *testing.T) {
	m := psync.NewMembership([]*pb.ReplicaID{r("a"), r("c")})
	if got := m.SlotOf(r("c")); got != 1 {
		t.Fatalf("pre-add: c slot = %d, want 1", got)
	}
	if _, err := m.Add(r("b")); err != nil {
		t.Fatal(err)
	}
	if got := m.SlotOf(r("c")); got != 1 {
		t.Fatalf("post-add: c slot = %d, want 1 (unchanged — Add appends)", got)
	}
	if got := m.SlotOf(r("b")); got != 2 {
		t.Fatalf("post-add: b slot = %d, want 2 (appended)", got)
	}
}
