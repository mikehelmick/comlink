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

	"github.com/mikehelmick/comlink/membership"
)

// runScheduler kicks off a background goroutine that drains the
// scheduler so Sends and responses flow continuously while a
// blocking VoteOut call waits.
func runScheduler(t *testing.T, f *fixture) func() {
	t.Helper()
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				f.sched.RunAll()
			}
		}
	}()
	return func() { close(stop) }
}

// TestVoteOutAcceptedWhenAllAgree: alice initiates VoteOut(carol)
// after carol's been silent. bob also suspects carol. Both Ack;
// carol is removed from ML at all peers.
func TestVoteOutAcceptedWhenAllAgree(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 11, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 80 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
	})
	alice, bob, _ := f.mgrs[0], f.mgrs[1], f.mgrs[2]

	// Take carol offline so both alice and bob's FDs fire.
	_ = f.convs[2].Close()
	_ = f.mgrs[2].Close()

	stop := runScheduler(t, f)
	defer stop()

	// Wait for both alice and bob to suspect carol.
	if !waitFor(2*time.Second, func() bool {
		return alice.IsSuspected(r("carol")) && bob.IsSuspected(r("carol"))
	}) {
		t.Fatalf("alice and bob did not both suspect carol")
	}

	// Initiate VoteOut.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := alice.VoteOut(ctx, r("carol")); err != nil {
		t.Fatalf("VoteOut failed: %v", err)
	}

	// Carol is removed from ML at alice.
	for _, mem := range alice.Members() {
		if bytes.Equal(mem.GetValue(), r("carol").GetValue()) {
			t.Fatalf("alice still has carol in ML after VoteOut")
		}
	}
	// And at bob.
	if !waitFor(2*time.Second, func() bool {
		for _, mem := range bob.Members() {
			if bytes.Equal(mem.GetValue(), r("carol").GetValue()) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("bob did not remove carol from ML; current ML = %v", bob.Members())
	}
}

// TestVoteOutRejectedByNack: bob has just received a message from
// carol so does NOT suspect her; bob Nacks alice's VoteOut. The
// vote aborts and carol stays in ML.
func TestVoteOutRejectedByNack(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 17, membership.Config{
		QuietInterval:     5 * time.Second,
		// Long enough that bob doesn't time out carol while we
		// wait for the vote to resolve.
		SuspicionInterval: 30 * time.Second,
		TickInterval:      10 * time.Millisecond,
	})
	alice, _, carol := f.mgrs[0], f.mgrs[1], f.mgrs[2]

	stop := runScheduler(t, f)
	defer stop()

	// Have carol send a message so everyone has recent activity
	// from her — alice's VoteOut should get a Nack from bob.
	if _, err := carol.SendApp([]byte("alive")); err != nil {
		t.Fatal(err)
	}
	// Drain.
	if msg, ok := f.drainOne(0, time.Second); !ok || string(msg.Payload) != "alive" {
		t.Fatalf("alice did not receive carol's alive message")
	}
	if msg, ok := f.drainOne(1, time.Second); !ok || string(msg.Payload) != "alive" {
		t.Fatalf("bob did not receive carol's alive message")
	}

	// Crucial: alice now suspects carol locally (force this so the
	// initiator wants to evict). The simplest way: just trust that
	// alice's perspective is "I want to evict carol" — VoteOut
	// requires no local FD precondition, so we can call it directly.
	// Bob, however, has fresh activity from carol and is NOT
	// suspecting her; he'll Nack.

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := alice.VoteOut(ctx, r("carol"))
	if !errors.Is(err, membership.ErrVoteOutNacked) {
		t.Fatalf("VoteOut err = %v, want ErrVoteOutNacked", err)
	}

	// Carol still in ML at alice.
	found := false
	for _, mem := range alice.Members() {
		if bytes.Equal(mem.GetValue(), r("carol").GetValue()) {
			found = true
		}
	}
	if !found {
		t.Fatalf("carol removed from alice's ML despite Nack")
	}
}

// TestVoteOutTargetIsSelf rejects voting yourself out via this API.
func TestVoteOutTargetIsSelf(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	if err := f.mgrs[0].VoteOut(context.Background(), r("alice")); !errors.Is(err, membership.ErrVoteOutTargetIsSelf) {
		t.Fatalf("VoteOut(self) err = %v, want ErrVoteOutTargetIsSelf", err)
	}
}

// TestVoteOutTargetNotMember rejects voting out someone who isn't
// in the conversation.
func TestVoteOutTargetNotMember(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	err := f.mgrs[0].VoteOut(context.Background(), r("ghost"))
	if !errors.Is(err, membership.ErrVoteOutTargetNotMember) {
		t.Fatalf("VoteOut(ghost) err = %v, want ErrVoteOutTargetNotMember", err)
	}
}

// TestVoteOutTimeoutWithoutResponse: alice initiates VoteOut(carol)
// in a 2-replica conversation where carol is unreachable. The
// only would-be voter (bob) doesn't exist; alice waits for ctx.
func TestVoteOutTimeoutWithoutResponse(t *testing.T) {
	f := setup(t, []string{"alice", "bob", "carol"}, 23, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	alice := f.mgrs[0]

	// Take both bob and carol offline so alice has no peers to
	// respond.
	_ = f.convs[1].Close()
	_ = f.mgrs[1].Close()
	_ = f.convs[2].Close()
	_ = f.mgrs[2].Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := alice.VoteOut(ctx, r("carol"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("VoteOut err = %v, want context.DeadlineExceeded", err)
	}
}
