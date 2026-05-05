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

package membership_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/membership"
)

// TestVoteInTwoPhaseAddsAtSlotEnd confirms the new two-phase
// flow: quorum gate (VoteIn/VoteInAck) followed by commit
// (MemberAdd) which actually grows ML. Insertion-order means
// the new replica's slot is at the end.
func TestVoteInTwoPhaseAddsAtSlotEnd(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 41, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	alice, bob, carol := f.mgrs[0], f.mgrs[1], f.mgrs[2]

	stop := runScheduler(t, f)
	defer stop()

	// alice initiates VoteIn(dave). Two-phase: alice's own Ack +
	// (at least) one other peer's Ack to reach quorum (2 of 3).
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := alice.VoteIn(ctx, r("dave"), "127.0.0.1:9000"); err != nil {
		t.Fatalf("VoteIn: %v", err)
	}

	// dave appears at the end of every replica's ML.
	for i, mgr := range []*membership.Manager{alice, bob, carol} {
		if !waitFor(2*time.Second, func() bool {
			members := mgr.Members()
			return len(members) == 4 && bytes.Equal(members[3].GetValue(), r("dave").GetValue())
		}) {
			members := mgr.Members()
			t.Fatalf("replica %d ML did not get dave at slot 3; got %d members", i, len(members))
			for j, m := range members {
				t.Logf("  slot %d = %x", j, m.GetValue())
			}
		}
	}
}

// TestVoteInLateMemberAddCatchup is the key scenario the
// two-phase design enables: a peer that's partitioned during
// the VoteIn AND MemberAdd broadcast catches up later via the
// standard lost-message protocol.
//
// Setup: 3-replica conv. Block bob<->everyone during the vote.
// Alice + carol vote in dave (quorum reached without bob).
// Heal partition. Bob should eventually have dave in its ML
// once it processes the MemberAdd via lost-message catch-up.
func TestVoteInLateMemberAddCatchup(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 71, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 30 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	alice, bob, carol := f.mgrs[0], f.mgrs[1], f.mgrs[2]

	stop := runScheduler(t, f)
	defer stop()

	// Block bob's bidirectional traffic.
	bobV := r("bob").GetValue()
	f.sched.AddPartition(func(from, to *pb.ReplicaID) bool {
		return bytes.Equal(from.GetValue(), bobV) || bytes.Equal(to.GetValue(), bobV)
	})

	// alice + carol form a 2-of-3 quorum without bob.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := alice.VoteIn(ctx, r("dave"), "127.0.0.1:9001"); err != nil {
		t.Fatalf("VoteIn: %v", err)
	}

	// alice and carol have dave; bob doesn't yet.
	if !waitFor(2*time.Second, func() bool {
		return len(alice.Members()) == 4 && len(carol.Members()) == 4
	}) {
		t.Fatalf("alice or carol did not add dave; alice=%d carol=%d",
			len(alice.Members()), len(carol.Members()))
	}
	if len(bob.Members()) != 3 {
		t.Fatalf("bob unexpectedly has %d members during partition", len(bob.Members()))
	}

	// Heal partition.
	f.sched.ClearPartitions()

	// alice sends an app message — its vector clock is length-4
	// (post-MemberAdd). bob receives, can't interpret, requests
	// missing predecessors via lost-message. The MemberAdd is in
	// the predecessor chain; bob fetches it and applies, growing
	// his own ML to length 4.
	if _, err := alice.SendApp([]byte("post-add")); err != nil {
		t.Fatal(err)
	}

	if !waitFor(5*time.Second, func() bool {
		return len(bob.Members()) == 4
	}) {
		t.Fatalf("bob did not catch up to dave via lost-message; len(bob.Members) = %d",
			len(bob.Members()))
	}
}
