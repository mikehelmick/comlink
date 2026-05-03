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
	"errors"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/membership"
)

// TestVoteInAcceptedAddsToML: alice initiates VoteIn(dave) in an
// alice+bob conversation. Bob default-Acks. The new ML at both
// replicas contains alice, bob, dave (sorted).
func TestVoteInAcceptedAddsToML(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 31, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	alice, bob := f.mgrs[0], f.mgrs[1]

	stop := runScheduler(t, f)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := alice.VoteIn(ctx, r("dave"), "127.0.0.1:9000"); err != nil {
		t.Fatalf("VoteIn failed: %v", err)
	}

	containsDave := func(members []*pb.ReplicaID) bool {
		for _, m := range members {
			if bytes.Equal(m.GetValue(), r("dave").GetValue()) {
				return true
			}
		}
		return false
	}

	if !containsDave(alice.Members()) {
		t.Fatalf("alice does not have dave in ML after VoteIn; got %v", alice.Members())
	}
	if !waitFor(2*time.Second, func() bool { return containsDave(bob.Members()) }) {
		t.Fatalf("bob did not add dave to ML; got %v", bob.Members())
	}
}

// TestVoteInTargetIsSelf rejects voting yourself in.
func TestVoteInTargetIsSelf(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	if err := f.mgrs[0].VoteIn(context.Background(), r("alice"), ""); !errors.Is(err, membership.ErrVoteInTargetIsSelf) {
		t.Fatalf("VoteIn(self) err = %v, want ErrVoteInTargetIsSelf", err)
	}
}

// TestVoteInTargetAlreadyMember rejects voting in an existing member.
func TestVoteInTargetAlreadyMember(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	err := f.mgrs[0].VoteIn(context.Background(), r("bob"), "")
	if !errors.Is(err, membership.ErrVoteInTargetAlreadyMember) {
		t.Fatalf("VoteIn(existing member) err = %v, want ErrVoteInTargetAlreadyMember", err)
	}
}

// TestVoteInTimeoutWithoutResponse: alice initiates VoteIn(dave)
// but bob is unreachable; ctx deadline trips.
func TestVoteInTimeoutWithoutResponse(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	alice := f.mgrs[0]
	_ = f.convs[1].Close()
	_ = f.mgrs[1].Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := alice.VoteIn(ctx, r("dave"), "addr")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("VoteIn err = %v, want context.DeadlineExceeded", err)
	}
}

