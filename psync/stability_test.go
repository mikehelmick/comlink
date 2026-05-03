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
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
)

func TestStabilityRequiresAckFromAllOthers(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	// Alice sends seq 1; bob has not yet acknowledged.
	a1, _, _ := g.Insert(envelope(alice, []uint64{1, 0}))
	if psync.IsStable(g, a1) {
		t.Fatal("a1 should not be stable before bob acks")
	}
	// Bob sends a message whose vector includes alice's seq 1.
	g.Insert(envelope(bob, []uint64{1, 1}))
	if !psync.IsStable(g, a1) {
		t.Fatal("a1 should be stable after bob's reply")
	}
}

func TestStabilityIgnoresSelf(t *testing.T) {
	// In a 1-replica conversation alice's messages are immediately
	// stable (no other participants need to ack).
	alice := r("alice")
	m := psync.NewMembership([]*pb.ReplicaID{alice})
	g := psync.NewGraph(m)
	a1, _, _ := g.Insert(envelope(alice, []uint64{1}))
	if !psync.IsStable(g, a1) {
		t.Fatal("a1 in 1-replica conversation should be immediately stable")
	}
}

func TestStabilityWithThreeReplicas(t *testing.T) {
	a, b, c := r("alice"), r("bob"), r("carol")
	m := psync.NewMembership([]*pb.ReplicaID{a, b, c})
	g := psync.NewGraph(m)

	a1, _, _ := g.Insert(envelope(a, []uint64{1, 0, 0}))
	if psync.IsStable(g, a1) {
		t.Fatal("a1 stable with no acks")
	}
	g.Insert(envelope(b, []uint64{1, 1, 0}))
	if psync.IsStable(g, a1) {
		t.Fatal("a1 stable after only one of two other replicas acks")
	}
	g.Insert(envelope(c, []uint64{1, 0, 1}))
	if !psync.IsStable(g, a1) {
		t.Fatal("a1 should be stable after both bob and carol ack")
	}
}

func TestStabilityViaTransitiveAck(t *testing.T) {
	// In a 3-replica conversation, alice's seq 1 can be considered
	// stable when carol's only message comes via a chain that
	// includes a higher-seq alice message.
	a, b, c := r("alice"), r("bob"), r("carol")
	m := psync.NewMembership([]*pb.ReplicaID{a, b, c})
	g := psync.NewGraph(m)

	g.Insert(envelope(a, []uint64{1, 0, 0})) // a1
	g.Insert(envelope(a, []uint64{2, 0, 0})) // a2 — alice's monotonic chain
	g.Insert(envelope(b, []uint64{2, 1, 0})) // bob acks alice's seq 2
	g.Insert(envelope(c, []uint64{2, 0, 1})) // carol acks alice's seq 2

	a1 := g.Lookup(a.GetValue(), 1)
	if !psync.IsStable(g, a1) {
		t.Fatal("a1 should be stable: bob and carol both have vectors with alice slot >= 1")
	}
	a2 := g.Lookup(a.GetValue(), 2)
	if !psync.IsStable(g, a2) {
		t.Fatal("a2 should be stable: bob and carol both have vectors with alice slot >= 2")
	}
}

func TestStableNodes(t *testing.T) {
	a, b := r("alice"), r("bob")
	m := psync.NewMembership([]*pb.ReplicaID{a, b})
	g := psync.NewGraph(m)
	g.Insert(envelope(a, []uint64{1, 0})) // a1
	g.Insert(envelope(a, []uint64{2, 0})) // a2 (no bob ack yet)
	g.Insert(envelope(b, []uint64{1, 1})) // b1 acks a1 only
	stable := psync.StableNodes(g, psync.StandardChecker{})
	// a1 is stable; a2 is not (bob's vector has alice slot = 1, < 2);
	// b1 is not stable (alice has not acked it past seq 0).
	if len(stable) != 1 {
		t.Fatalf("StableNodes returned %d, want 1", len(stable))
	}
	if stable[0].SenderSeq != 1 {
		t.Fatalf("stable node SenderSeq = %d, want 1", stable[0].SenderSeq)
	}
}

func TestWaveCompleteWhenSomeStable(t *testing.T) {
	g, alice, bob := twoReplicaGraph()
	g.Insert(envelope(alice, []uint64{1, 0}))
	if psync.WaveComplete(g, 1, psync.StandardChecker{}) {
		t.Fatal("wave 1 should be incomplete with no acks")
	}
	g.Insert(envelope(bob, []uint64{1, 1}))
	if !psync.WaveComplete(g, 1, psync.StandardChecker{}) {
		t.Fatal("wave 1 should be complete after bob acks alice's wave-1 message")
	}
}

func TestStabilityIgnoresFrozenSlots(t *testing.T) {
	// A frozen replica's lack of acknowledgment shouldn't block
	// stability — that's the whole point of freezing a removed
	// member (PLAN §2.10.1).
	a, b, c := r("alice"), r("bob"), r("carol")
	m := psync.NewMembership([]*pb.ReplicaID{a, b, c})
	g := psync.NewGraph(m)
	a1, _, _ := g.Insert(envelope(a, []uint64{1, 0, 0}))
	g.Insert(envelope(b, []uint64{1, 1, 0})) // bob acks a1
	if psync.IsStable(g, a1) {
		t.Fatal("a1 should not be stable: carol has not acked")
	}
	if err := m.Freeze(c); err != nil {
		t.Fatal(err)
	}
	if !psync.IsStable(g, a1) {
		t.Fatal("a1 should be stable after carol is frozen — only bob's ack matters now")
	}
}
