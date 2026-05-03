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
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// schedulerPump runs sched.RunAll on a tight loop; cancel via the
// returned func.
func schedulerPump(sched *memory.Scheduler) (stop func()) {
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				sched.RunAll()
			}
		}
	}()
	return func() { close(stopCh) }
}

// TestRestartReplaysFromLogWithoutPeers: bob's log has alice's prior
// messages; alice is unreachable; Restart still recovers state from
// the local log, even though it eventually times out waiting for a
// peer ack. We observe via bob's first post-restart Send: its
// vector clock must reflect alice's contributions, proving the
// graph was reconstructed from the log.
func TestRestartReplaysFromLogWithoutPeers(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("log-replay")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(11)
	t.Cleanup(func() { _ = sched.Close() })

	// Spin up alice + bob, have alice send 3 messages; bob receives
	// and logs them. The bobLog reference is held across the bob
	// "crash" (re-create bob with same log).
	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))
	bobLog := clog.NewMemory(convID)
	bobStorage := stable.NewMemory()

	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: bobLog, Storage: bobStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err := alice.Send(fmt.Appendf(nil, "alice-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
	// Drain.
	drain(t, alice, 3)
	drain(t, bob, 3)
	if got := bobLog.NextOffset(); got != 3 {
		t.Fatalf("bob log NextOffset = %d, want 3", got)
	}

	// Crash bob (close conversation + close network handle so we can
	// reconnect under the same ReplicaID); take alice offline by
	// closing her conversation and her network handle.
	_ = bob.Close()
	_ = bobNet.Close()
	_ = alice.Close()
	_ = aliceNet.Close()

	// Re-create bob with the SAME log (a real "restart"). New net
	// handle.
	newBobNet, err := sched.Connect(r("bob"))
	if err != nil {
		t.Fatalf("reconnect bob: %v", err)
	}
	newBob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: newBobNet, Log: bobLog, Storage: bobStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newBob.Close() })

	stop := schedulerPump(sched)
	defer stop()

	// Restart with a short timeout — alice is gone, so the leaf
	// exchange never completes. But the log replay portion runs
	// first, so bob's graph should be populated regardless.
	restartCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := newBob.Restart(restartCtx); !errors.Is(err, psync.ErrRestartTimedOut) {
		t.Fatalf("Restart err = %v, want ErrRestartTimedOut (alice is gone)", err)
	}

	// Now: bob.Send. The vector clock should reflect alice's seq 3
	// (recovered from log).
	id, err := newBob.Send([]byte("after-replay"))
	if err != nil {
		t.Fatal(err)
	}
	vc := id.GetVectorClock()
	// Alice slot is 0, bob slot is 1. Vector should be [3, 1]:
	// alice's seq 3 known via log replay, bob's first post-restart
	// send.
	if len(vc) != 2 || vc[0] != 3 || vc[1] != 1 {
		t.Fatalf("post-replay Send vector_clock = %v, want [3 1] — log replay didn't populate graph",
			vc)
	}

	// Drain bob's self-delivery.
	drain(t, newBob, 1)
}

// TestRestartRecoversPrunedRegionFromPeer is the Phase 1 exit-
// criterion pruned-region test: bob's log is missing messages that
// were delivered to it pre-failure (pruned away locally) but are
// still resident on alice. The union of local-log-replay + peer
// leaf-exchange + lost-message protocol must restore the missing
// messages.
func TestRestartRecoversPrunedRegionFromPeer(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("pruned-region")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(31)
	t.Cleanup(func() { _ = sched.Close() })

	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))
	bobLog := clog.NewMemory(convID)
	bobStorage := stable.NewMemory()

	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alice.Close() })
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: bobLog, Storage: bobStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Alice sends 5 messages.
	const N = 5
	for i := 0; i < N; i++ {
		if _, err := alice.Send(fmt.Appendf(nil, "m%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
	drain(t, alice, N)
	drain(t, bob, N)
	if got := bobLog.NextOffset(); got != N {
		t.Fatalf("bob log NextOffset = %d, want %d", got, N)
	}

	// Trim bob's log: drop offsets 0,1 (m0, m1).
	if err := bobLog.Truncate(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if got := bobLog.FirstOffset(); got != 2 {
		t.Fatalf("FirstOffset after trim = %d, want 2", got)
	}

	// Crash bob.
	_ = bob.Close()
	_ = bobNet.Close()

	// Re-create bob with the trimmed log.
	newBobNet, err := sched.Connect(r("bob"))
	if err != nil {
		t.Fatalf("reconnect bob: %v", err)
	}
	newBob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: newBobNet, Log: bobLog, Storage: bobStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newBob.Close() })

	stop := schedulerPump(sched)
	defer stop()

	restartCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := newBob.Restart(restartCtx); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	// Give the lost-message protocol time to transitively pull in
	// pruned ancestors (m0, m1).
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		// Check: has bob's log seen m0 and m1 again?
		_, err1 := bobLog.LookupBySender(ctx, r("alice").GetValue(), 1)
		_, err2 := bobLog.LookupBySender(ctx, r("alice").GetValue(), 2)
		if err1 == nil && err2 == nil {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("after Restart, bob's log still missing m0 (err1=%v) or m1 (err2=%v) — pruned region not recovered",
				err1, err2)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// And: bob should be able to send a message whose vector_clock
	// shows it knows about all 5 alice messages.
	id, err := newBob.Send([]byte("after-recovery"))
	if err != nil {
		t.Fatal(err)
	}
	vc := id.GetVectorClock()
	if len(vc) != 2 || vc[0] != N || vc[1] != 1 {
		t.Fatalf("post-recovery Send vector_clock = %v, want [%d 1]", vc, N)
	}
	// Drain.
	drainPayloads := func(c *psync.Conversation, count int) [][]byte {
		out := make([][]byte, 0, count)
		got := drain(t, c, count)
		for _, d := range got {
			out = append(out, bytes.Clone(d.Envelope.GetPayload()))
		}
		return out
	}
	_ = drainPayloads(newBob, 1)
}
