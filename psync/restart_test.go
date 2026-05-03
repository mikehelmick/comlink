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
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mikehelmick/comlink/clock"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// TestRestartFetchesLeavesFromPeer is the basic happy-path restart
// scenario: alice has been sending messages; bob comes back fresh
// (empty log, empty graph) and rebuilds via Restart.
func TestRestartFetchesLeavesFromPeer(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("restart-basic")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(11)
	t.Cleanup(func() { _ = sched.Close() })

	// Alice runs the whole test.
	aliceNet, _ := sched.Connect(r("alice"))
	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alice.Close() })

	// Alice sends three messages while bob is "down."
	for i := 0; i < 3; i++ {
		if _, err := alice.Send(fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
	// Drain alice's self-deliveries so the channel doesn't block her.
	for i := 0; i < 3; i++ {
		<-alice.Recv()
	}

	// Bob comes online — fresh log, fresh graph — and Restarts.
	bobNet, _ := sched.Connect(r("bob"))
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bob.Close() })

	// Drive the scheduler in the background while Restart waits.
	stopPump := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-ticker.C:
				sched.RunAll()
			}
		}
	}()
	defer close(stopPump)

	restartCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bob.Restart(restartCtx); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	// Now collect bob's deliveries — should be all three of alice's
	// messages, in causal order.
	got := drain(t, bob, 3)
	for i, d := range got {
		want := fmt.Appendf(nil, "m%d", i)
		if !bytes.Equal(d.Envelope.GetPayload(), want) {
			t.Fatalf("bob delivery %d = %q, want %q", i, d.Envelope.GetPayload(), want)
		}
	}
}

// TestRestartTimesOutWithoutAcks: if no peer responds (e.g. the
// only peer is unreachable), Restart returns ErrRestartTimedOut.
func TestRestartTimesOutWithoutAcks(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("restart-timeout")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(11)
	t.Cleanup(func() { _ = sched.Close() })

	// Only bob exists; alice is "permanently unreachable" (we don't
	// connect alice to the scheduler, so sends to alice resolve as
	// ErrUnknownPeer, which is silently swallowed by the broadcast).
	bobNet, _ := sched.Connect(r("bob"))
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bob.Close() })

	restartCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = bob.Restart(restartCtx)
	if !errors.Is(err, psync.ErrRestartTimedOut) {
		t.Fatalf("Restart err = %v, want ErrRestartTimedOut", err)
	}
}

// TestRestartHandshakeRetriesUntilDelivered is the Phase 1 exit-
// criterion test: a Restart invocation eventually completes even if
// the first N broadcasts of the restart message are dropped by the
// transport. We model the partition as alice<->bob blocked
// initially, with a manual clock used to drive retries; after N
// broadcasts we heal and the next retry succeeds.
func TestRestartHandshakeRetriesUntilDelivered(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("restart-retry")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(13)
	t.Cleanup(func() { _ = sched.Close() })

	manual := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	aliceNet, _ := sched.Connect(r("alice"))
	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		Clock: manual,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alice.Close() })
	if _, err := alice.Send([]byte("pre-restart")); err != nil {
		t.Fatal(err)
	}
	sched.RunAll()
	time.Sleep(5 * time.Millisecond)
	<-alice.Recv()

	// Block bob -> alice direction (RestartMessage path).
	var dropsLeft atomic.Int32
	dropsLeft.Store(3)
	sched.AddPartition(func(from, to *pb.ReplicaID) bool {
		if !bytes.Equal(from.GetValue(), r("bob").GetValue()) ||
			!bytes.Equal(to.GetValue(), r("alice").GetValue()) {
			return false
		}
		// While we still want to drop a broadcast, drop it.
		if dropsLeft.Load() > 0 {
			dropsLeft.Add(-1)
			return true
		}
		return false
	})

	bobNet, _ := sched.Connect(r("bob"))
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		Clock: manual,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bob.Close() })

	// Drive the scheduler in the background.
	stopPump := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPump:
				return
			case <-ticker.C:
				sched.RunAll()
			}
		}
	}()
	defer close(stopPump)

	// Drive Restart in a goroutine; meanwhile advance the manual
	// clock to fire retry timers.
	restartCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	restartDone := make(chan error, 1)
	go func() {
		restartDone <- bob.Restart(restartCtx)
	}()

	// Advance enough times to fire retries past the drop window.
	for i := 0; i < 8; i++ {
		// Brief real-time pause so the goroutine reaches its
		// timer-Reset before we Advance.
		time.Sleep(20 * time.Millisecond)
		manual.Advance(psync.DefaultRestartRetryInterval)
	}

	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("Restart did not complete after retries: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Restart did not return after %d retries; %d drops still pending",
			8, dropsLeft.Load())
	}

	// Bob should eventually deliver alice's "pre-restart" message
	// via leaf fetch + lost-message protocol.
	got := drain(t, bob, 1)
	if !bytes.Equal(got[0].Envelope.GetPayload(), []byte("pre-restart")) {
		t.Fatalf("bob delivery = %q, want %q", got[0].Envelope.GetPayload(), "pre-restart")
	}
}
