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
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/membership"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

func id16(tag string) []byte {
	b := make([]byte, 16)
	copy(b, tag)
	return b
}

func r(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

type fixture struct {
	t       *testing.T
	sched   *memory.Scheduler
	convs   []*psync.Conversation
	mgrs    []*membership.Manager
	members []*pb.ReplicaID
	convID  *pb.ConversationID
}

func setup(t *testing.T, replicas []string, seed uint64, mgrCfg membership.Config) *fixture {
	t.Helper()
	convID := &pb.ConversationID{Value: id16("mship-test")}
	members := make([]*pb.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = r(name)
	}
	sched := memory.NewScheduler(seed)
	t.Cleanup(func() { _ = sched.Close() })

	f := &fixture{
		t: t, sched: sched, members: members, convID: convID,
	}
	for _, m := range members {
		net, err := sched.Connect(m)
		if err != nil {
			t.Fatal(err)
		}
		c, err := psync.New(context.Background(), psync.Config{
			ConversationID: convID, Self: m, Members: members,
			Network: net, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
			DeliveryBufSize: 1024,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.convs = append(f.convs, c)

		// Per-replica Manager.
		cfg := mgrCfg
		cfg.Conversation = c
		cfg.Self = m
		cfg.Members = members
		mgr, err := membership.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		f.mgrs = append(f.mgrs, mgr)
	}
	t.Cleanup(func() {
		// Close convs first so pumps unblock from conv.Recv, then
		// close managers. Manager.Close is also order-independent
		// thanks to its stopped signal, but conv-first matches the
		// natural read direction.
		for _, c := range f.convs {
			_ = c.Close()
		}
		for _, mgr := range f.mgrs {
			_ = mgr.Close()
		}
	})
	return f
}

func (f *fixture) drive(rounds int) {
	for range rounds {
		f.sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
}

func (f *fixture) drainOne(i int, deadline time.Duration) (membership.AppMessage, bool) {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case msg, ok := <-f.mgrs[i].Recv():
		return msg, ok
	case <-timer.C:
		return membership.AppMessage{}, false
	}
}

func TestAppMessagesRoundTripThroughManager(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})

	if _, err := f.mgrs[0].SendApp([]byte("hello bob")); err != nil {
		t.Fatal(err)
	}
	f.drive(8)

	got, ok := f.drainOne(1, 2*time.Second)
	if !ok {
		t.Fatal("bob did not receive")
	}
	if !bytes.Equal(got.Payload, []byte("hello bob")) {
		t.Fatalf("got %q, want %q", got.Payload, "hello bob")
	}
	if !bytes.Equal(got.From.GetValue(), r("alice").GetValue()) {
		t.Fatalf("From = %x, want alice", got.From.GetValue())
	}
}

func TestHeartbeatsArentDeliveredToApp(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     50 * time.Millisecond,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      10 * time.Millisecond,
	})

	// Wait long enough for several heartbeats to fly.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		f.sched.RunAll()
		time.Sleep(10 * time.Millisecond)
	}

	// Bob should not have received any AppMessage — only
	// heartbeats, which are filtered out.
	if _, ok := f.drainOne(1, 50*time.Millisecond); ok {
		t.Fatal("bob received an AppMessage from heartbeats; should be filtered out")
	}

	// Send a real app message. Bob should receive that one.
	if _, err := f.mgrs[0].SendApp([]byte("real")); err != nil {
		t.Fatal(err)
	}
	f.drive(8)
	got, ok := f.drainOne(1, time.Second)
	if !ok {
		t.Fatal("bob did not receive real message")
	}
	if !bytes.Equal(got.Payload, []byte("real")) {
		t.Fatalf("got %q, want %q", got.Payload, "real")
	}
}

// TestSuspicionFiresAfterPeerSilence is a smoke test for the
// 3(c) skeleton: when bob "crashes," alice's FailureDetector
// should fire onSuspect, which the Manager translates into a
// SuspectDown envelope. We don't yet have a public hook to verify
// the SuspectDown was emitted — that observability is added with
// the full protocol in 3(d). For 3(c) the assertion is just
// "Manager survives the suspicion event without crashing."
func TestSuspicionFiresAfterPeerSilence(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 80 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
	})

	// Close bob first (conv then mgr) so alice gets no NoteReceived.
	_ = f.convs[1].Close()
	_ = f.mgrs[1].Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		f.sched.RunAll()
		time.Sleep(10 * time.Millisecond)
	}
	// Reaching here without panic is the entire 3(c) assertion.
}

func TestManagerCloseIsIdempotent(t *testing.T) {
	f := setup(t, []string{"alice", "bob"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	if err := f.mgrs[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.mgrs[0].Close(); err != nil {
		t.Fatal(err)
	}
}
