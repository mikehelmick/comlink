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
	"errors"
	"slices"
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
)

// envelope builds a synthetic Envelope for graph tests. The membership
// must already include sender at the slot implied by the vector
// clock; tests pass in vectors of the right length.
func envelope(sender *pb.ReplicaID, vc []uint64) *pb.Envelope {
	return &pb.Envelope{
		Id: &pb.MessageID{
			Sender:      sender,
			VectorClock: vc,
		},
	}
}

// twoReplicaGraph returns an empty graph for the canonical
// 2-replica conversation alice (slot 0) and bob (slot 1).
func twoReplicaGraph() (*psync.Graph, *pb.ReplicaID, *pb.ReplicaID) {
	alice := r("alice")
	bob := r("bob")
	m := psync.NewMembership([]*pb.ReplicaID{alice, bob})
	return psync.NewGraph(m), alice, bob
}

func TestInsertFirstMessageNoParents(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	n, missing, err := g.Insert(envelope(alice, []uint64{1, 0}))
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if missing != nil {
		t.Fatalf("Insert reported missing parents: %v", missing)
	}
	if len(n.Parents) != 0 {
		t.Errorf("first message had %d parents, want 0", len(n.Parents))
	}
	if n.SenderSeq != 1 {
		t.Errorf("SenderSeq = %d, want 1", n.SenderSeq)
	}
	if n.SenderSlot != 0 {
		t.Errorf("SenderSlot = %d, want 0", n.SenderSlot)
	}
	if n.Wave != 1 {
		t.Errorf("Wave = %d, want 1", n.Wave)
	}
	if g.Size() != 1 {
		t.Errorf("Size = %d, want 1", g.Size())
	}
}

func TestInsertWithSelfPredecessor(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	a2, missing, err := g.Insert(envelope(alice, []uint64{2, 0}))
	if err != nil {
		t.Fatalf("Insert second: %v", err)
	}
	if missing != nil {
		t.Fatalf("Insert reported missing: %v", missing)
	}
	if len(a2.Parents) != 1 || a2.Parents[0] != a1 {
		t.Fatalf("a2.Parents = %v, want [a1]", a2.Parents)
	}
	if len(a1.Children) != 1 || a1.Children[0] != a2 {
		t.Fatalf("a1.Children = %v, want [a2]", a1.Children)
	}
}

func TestInsertWithCrossReplicaPredecessor(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	// Bob sends after seeing alice's first.
	b1, missing, err := g.Insert(envelope(bob, []uint64{1, 1}))
	if err != nil {
		t.Fatalf("Insert b1: %v", err)
	}
	if missing != nil {
		t.Fatalf("missing: %v", missing)
	}
	if len(b1.Parents) != 1 || b1.Parents[0] != a1 {
		t.Fatalf("b1.Parents = %v, want [a1]", b1.Parents)
	}
	if len(a1.Children) != 1 || a1.Children[0] != b1 {
		t.Fatalf("a1.Children = %v, want [b1]", a1.Children)
	}
}

func TestInsertReportsMissingParents(t *testing.T) {
	g, _, bob := twoReplicaGraph()
	// Bob sends a message claiming to depend on alice's seq-3, but
	// the graph contains no alice messages at all.
	_, missing, err := g.Insert(envelope(bob, []uint64{3, 1}))
	if !errors.Is(err, psync.ErrMissingParents) {
		t.Fatalf("Insert err = %v, want ErrMissingParents", err)
	}
	if len(missing) != 1 {
		t.Fatalf("missing = %v, want 1 entry", missing)
	}
	if missing[0].Seq != 3 {
		t.Errorf("missing seq = %d, want 3", missing[0].Seq)
	}
	if g.Size() != 0 {
		t.Errorf("graph Size after failed Insert = %d, want 0", g.Size())
	}
}

func TestInsertReportsAllMissingParents(t *testing.T) {
	// 3-replica scenario; insert a message that depends on parents
	// at multiple slots, none of which are present.
	a, b, c := r("alice"), r("bob"), r("carol")
	m := psync.NewMembership([]*pb.ReplicaID{a, b, c})
	g := psync.NewGraph(m)

	// Carol sends with vector [2, 1, 1]: depends on alice's seq-2
	// and bob's seq-1, neither present.
	_, missing, err := g.Insert(envelope(c, []uint64{2, 1, 1}))
	if !errors.Is(err, psync.ErrMissingParents) {
		t.Fatalf("err = %v, want ErrMissingParents", err)
	}
	if len(missing) != 2 {
		t.Fatalf("missing = %v, want 2 entries", missing)
	}
}

func TestInsertSelfPredecessorMissing(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	// Alice's seq-2 needs alice's seq-1 as a parent.
	_, missing, err := g.Insert(envelope(alice, []uint64{2, 0}))
	if !errors.Is(err, psync.ErrMissingParents) {
		t.Fatalf("err = %v, want ErrMissingParents", err)
	}
	if len(missing) != 1 {
		t.Fatalf("missing = %v, want 1 entry (alice's seq 1)", missing)
	}
	if missing[0].Seq != 1 {
		t.Errorf("missing seq = %d, want 1", missing[0].Seq)
	}
}

func TestInsertAlreadyPresent(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	first, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	again, missing, err := g.Insert(envelope(alice, []uint64{1, 0}))
	if !errors.Is(err, psync.ErrAlreadyPresent) {
		t.Fatalf("err = %v, want ErrAlreadyPresent", err)
	}
	if again != first {
		t.Errorf("re-Insert returned different node")
	}
	if missing != nil {
		t.Errorf("re-Insert reported missing: %v", missing)
	}
	if g.Size() != 1 {
		t.Errorf("graph Size = %d, want 1", g.Size())
	}
}

func TestInsertUnknownSender(t *testing.T) {
	g, _, _ := twoReplicaGraph()
	_, _, err := g.Insert(envelope(r("ghost"), []uint64{1, 0}))
	if !errors.Is(err, psync.ErrUnknownSender) {
		t.Fatalf("err = %v, want ErrUnknownSender", err)
	}
}

func TestInsertVectorWrongLength(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	_, _, err := g.Insert(envelope(alice, []uint64{1}))
	if !errors.Is(err, psync.ErrMalformedVector) {
		t.Fatalf("err = %v, want ErrMalformedVector", err)
	}
	_, _, err = g.Insert(envelope(alice, []uint64{1, 0, 0}))
	if !errors.Is(err, psync.ErrMalformedVector) {
		t.Fatalf("err = %v, want ErrMalformedVector", err)
	}
}

func TestInsertSenderSeqZero(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	// Alice's slot is 0; setting it to 0 means "this isn't really
	// my message" — should be rejected.
	_, _, err := g.Insert(envelope(alice, []uint64{0, 1}))
	if !errors.Is(err, psync.ErrMalformedVector) {
		t.Fatalf("err = %v, want ErrMalformedVector", err)
	}
}

func TestLookup(t *testing.T) {
	g, alice, _ := twoReplicaGraph()
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	if got := g.Lookup(alice.GetValue(), 1); got != a1 {
		t.Fatalf("Lookup = %v, want a1", got)
	}
	if got := g.Lookup(alice.GetValue(), 2); got != nil {
		t.Fatalf("Lookup of missing = %v, want nil", got)
	}
	if !g.Has(alice.GetValue(), 1) {
		t.Fatalf("Has(alice, 1) = false, want true")
	}
}

func TestLatestSeq(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	g.Insert(envelope(alice, []uint64{1, 0}))
	g.Insert(envelope(alice, []uint64{2, 0}))
	g.Insert(envelope(bob, []uint64{0, 1}))
	if got := g.LatestSeq(alice.GetValue()); got != 2 {
		t.Errorf("LatestSeq(alice) = %d, want 2", got)
	}
	if got := g.LatestSeq(bob.GetValue()); got != 1 {
		t.Errorf("LatestSeq(bob) = %d, want 1", got)
	}
	if got := g.LatestSeq(r("ghost").GetValue()); got != 0 {
		t.Errorf("LatestSeq(absent) = %d, want 0", got)
	}
}

func TestLeaves(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	b1, _, _ := g.Insert(envelope(bob, []uint64{1, 1}))
	// a1 is no longer a leaf (b1 cites it as a parent).
	leaves := g.Leaves()
	if len(leaves) != 1 || leaves[0] != b1 {
		t.Fatalf("Leaves = %v, want [b1]", leaves)
	}
	_ = a1

	// After bob -> alice round trip, a2 depends on b1 -> single leaf is a2.
	a2, _, _ := g.Insert(envelope(alice, []uint64{2, 1}))
	leaves = g.Leaves()
	if len(leaves) != 1 || leaves[0] != a2 {
		t.Fatalf("Leaves after a2 = %v, want [a2]", leaves)
	}
}

func TestWaveAssignment(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	b1, _, _ := g.Insert(envelope(bob, []uint64{0, 1}))
	a2, _, _ := g.Insert(envelope(alice, []uint64{2, 1}))
	b2, _, _ := g.Insert(envelope(bob, []uint64{1, 2}))

	if a1.Wave != 1 {
		t.Errorf("a1.Wave = %d, want 1", a1.Wave)
	}
	if b1.Wave != 1 {
		t.Errorf("b1.Wave = %d, want 1", b1.Wave)
	}
	if a2.Wave != 2 {
		t.Errorf("a2.Wave = %d, want 2", a2.Wave)
	}
	if b2.Wave != 2 {
		t.Errorf("b2.Wave = %d, want 2", b2.Wave)
	}

	wave1 := g.MessagesInWave(1)
	if len(wave1) != 2 {
		t.Errorf("wave 1 has %d messages, want 2", len(wave1))
	}
	wave2 := g.MessagesInWave(2)
	if len(wave2) != 2 {
		t.Errorf("wave 2 has %d messages, want 2", len(wave2))
	}

	if got := g.Waves(); !slices.Equal(got, []uint64{1, 2}) {
		t.Errorf("Waves = %v, want [1 2]", got)
	}
}

// TestRetryAfterMissingParentsFetched models the lost-message-
// protocol caller pattern: Insert reports missing, caller fetches
// them, inserts them, then retries the original Insert.
func TestRetryAfterMissingParentsFetched(t *testing.T) {
	g, alice, bob := twoReplicaGraph()

	// Bob's seq-1 depends on alice's seq-1 — which we haven't seen.
	_, missing, err := g.Insert(envelope(bob, []uint64{1, 1}))
	if !errors.Is(err, psync.ErrMissingParents) {
		t.Fatalf("first Insert: err = %v, want ErrMissingParents", err)
	}
	if len(missing) != 1 || missing[0].Seq != 1 {
		t.Fatalf("missing = %v, want [{alice, 1}]", missing)
	}

	// Caller fetches and inserts the missing parent.
	if _, _, err := g.Insert(envelope(alice, []uint64{1, 0})); err != nil {
		t.Fatal(err)
	}

	// Retry now succeeds.
	b1, missing, err := g.Insert(envelope(bob, []uint64{1, 1}))
	if err != nil {
		t.Fatalf("retry Insert: %v", err)
	}
	if missing != nil {
		t.Fatalf("retry missing: %v", missing)
	}
	if len(b1.Parents) != 1 {
		t.Fatalf("b1.Parents = %v, want 1 parent", b1.Parents)
	}
}

// TestParentsAcrossThreeReplicas exercises the case where a message
// has parents at multiple non-self slots.
func TestParentsAcrossThreeReplicas(t *testing.T) {
	a, b, c := r("alice"), r("bob"), r("carol")
	m := psync.NewMembership([]*pb.ReplicaID{a, b, c})
	g := psync.NewGraph(m)

	// Each replica sends its first message independently.
	a1, _, _ := g.Insert(envelope(a, []uint64{1, 0, 0}))
	b1, _, _ := g.Insert(envelope(b, []uint64{0, 1, 0}))
	_, _, _ = g.Insert(envelope(c, []uint64{0, 0, 1}))

	// Alice sends seq-2 having seen bob and carol.
	a2, missing, err := g.Insert(envelope(a, []uint64{2, 1, 1}))
	if err != nil {
		t.Fatalf("a2 Insert: %v", err)
	}
	if missing != nil {
		t.Fatalf("missing: %v", missing)
	}
	if len(a2.Parents) != 3 {
		t.Fatalf("a2.Parents has %d entries, want 3 (a1, b1, c1)", len(a2.Parents))
	}
	// Verify a1 is among parents (the self-predecessor).
	foundA1 := false
	for _, p := range a2.Parents {
		if p == a1 {
			foundA1 = true
		}
	}
	if !foundA1 {
		t.Fatalf("a2 missing a1 as self-predecessor parent")
	}
	_ = b1
}
