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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mikehelmick/comlink/membership"
)

// TestMajorityPartitionAllowsVoteOut: in a 5-replica conversation
// with 3 active members (still strict majority of 5), VoteOut is
// permitted.
func TestMajorityPartitionAllowsVoteOut(t *testing.T) {
	// 5-replica conversation; alice + bob + carol form a majority
	// (3 > 5/2). Pretend dave and eve are gone but still in ML.
	cfg := membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
		InitialGroupSize:  5,
	}
	f := setup(t, []string{"alice", "bob", "carol"}, 1, cfg)
	alice := f.mgrs[0]

	stop := runScheduler(t, f)
	defer stop()

	// alice is in a 3-of-5 majority partition (per InitialGroupSize=5).
	if !alice.InMajorityPartition() {
		t.Fatalf("alice should be in majority partition (3 of 5)")
	}

	// VoteOut(carol) is permitted (returns the regular Nack since
	// bob still sees carol as alive, but at least the partition
	// guard didn't refuse).
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := alice.VoteOut(ctx, r("carol"))
	if errors.Is(err, membership.ErrPartitionMinority) {
		t.Fatalf("VoteOut refused due to minority partition; alice should be in majority")
	}
}

// TestMinorityPartitionRefusesVoteOut: with InitialGroupSize=5
// but only 2 active members (alice, bob), alice is in the
// minority partition and VoteOut is refused.
func TestMinorityPartitionRefusesVoteOut(t *testing.T) {
	cfg := membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
		InitialGroupSize:  5,
	}
	f := setup(t, []string{"alice", "bob"}, 1, cfg)
	alice := f.mgrs[0]

	if alice.InMajorityPartition() {
		t.Fatalf("alice should be in minority partition (2 of 5); got InMajorityPartition=true")
	}

	err := alice.VoteOut(context.Background(), r("bob"))
	if !errors.Is(err, membership.ErrPartitionMinority) {
		t.Fatalf("VoteOut err = %v, want ErrPartitionMinority", err)
	}
}

// TestMinorityPartitionRefusesVoteIn: VoteIn likewise refuses.
func TestMinorityPartitionRefusesVoteIn(t *testing.T) {
	cfg := membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
		InitialGroupSize:  5,
	}
	f := setup(t, []string{"alice", "bob"}, 1, cfg)
	alice := f.mgrs[0]

	err := alice.VoteIn(context.Background(), r("dave"), "")
	if !errors.Is(err, membership.ErrPartitionMinority) {
		t.Fatalf("VoteIn err = %v, want ErrPartitionMinority", err)
	}
}

// TestTiedSplitIsMinority: a strict majority is required, so a
// tied split (|ML| == N/2) is minority to prevent split-brain.
func TestTiedSplitIsMinority(t *testing.T) {
	cfg := membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
		InitialGroupSize:  4, // tied split = 2-of-4
	}
	f := setup(t, []string{"alice", "bob"}, 1, cfg)
	alice := f.mgrs[0]

	if alice.InMajorityPartition() {
		t.Fatalf("tied split (2 of 4) should be minority; got InMajorityPartition=true")
	}
}

// TestDefaultInitialGroupSizeMatchesMembers: when not configured,
// InitialGroupSize defaults to len(Members) — meaning a
// freshly-constructed conversation always considers itself in
// the majority (everyone is present).
func TestDefaultInitialGroupSizeMatchesMembers(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	if !f.mgrs[0].InMajorityPartition() {
		t.Fatal("default InitialGroupSize should make all replicas majority at startup")
	}
}
