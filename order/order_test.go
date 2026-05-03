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

package order_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/order"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// ─── helpers (mirroring psync_test.go) ────────────────────────────

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

type orderFixture struct {
	t       *testing.T
	sched   *memory.Scheduler
	convs   []*psync.Conversation
	members []*pb.ReplicaID
	convID  *pb.ConversationID
}

func newOrderFixture(t *testing.T, replicas []string, seed uint64) *orderFixture {
	t.Helper()
	convID := &pb.ConversationID{Value: id16("order-test")}
	members := make([]*pb.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = r(name)
	}
	sched := memory.NewScheduler(seed)
	t.Cleanup(func() { _ = sched.Close() })

	f := &orderFixture{
		t:       t,
		sched:   sched,
		members: members,
		convID:  convID,
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
	}
	t.Cleanup(func() {
		for _, c := range f.convs {
			_ = c.Close()
		}
	})
	return f
}

func (f *orderFixture) drive(rounds int) {
	for range rounds {
		f.sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
}

func collectApplied(t *testing.T, o order.Order, want int, deadline time.Duration) []order.Applied {
	t.Helper()
	out := make([]order.Applied, 0, want)
	d := time.NewTimer(deadline)
	defer d.Stop()
	for len(out) < want {
		select {
		case a, ok := <-o.Apply():
			if !ok {
				t.Fatalf("Apply channel closed after %d/%d applied", len(out), want)
			}
			out = append(out, a)
		case <-d.C:
			t.Fatalf("timeout: only %d/%d applied", len(out), want)
		}
	}
	return out
}

// ─── PartialOrder tests ───────────────────────────────────────────

func TestPartialOrderForwardsAllDeliveries(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob"}, 1)
	alice, bob := f.convs[0], f.convs[1]
	pa := order.NewPartial(alice)
	defer pa.Close()
	pb := order.NewPartial(bob)
	defer pb.Close()

	if _, err := alice.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := bob.Send([]byte("world")); err != nil {
		t.Fatal(err)
	}
	f.drive(5)

	collectApplied(t, pa, 2, time.Second)
	collectApplied(t, pb, 2, time.Second)
}

// ─── Total tests ──────────────────────────────────────────────────

// TestTotalSameOrderEverywhere: every replica sees the same
// sequence of applied envelopes. The classic Total ordering
// invariant.
//
// Phase 1 has no heartbeats, so the last wave's messages won't
// become stable on their own — Phase 3 will fix that. For Phase 2
// tests we send an extra "settle" round whose messages act as
// acks for the previous round, then assert only on the rounds
// before settle.
func TestTotalSameOrderEverywhere(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob", "carol"}, 5)
	tots := make([]*order.Total, len(f.convs))
	for i, c := range f.convs {
		tots[i] = order.NewTotal(c)
		defer tots[i].Close()
	}

	const realRounds = 3
	const totalRounds = realRounds + 1 // +1 for settle round
	for round := range totalRounds {
		for i, c := range f.convs {
			if _, err := c.Send(fmt.Appendf(nil, "%d-%d", i, round)); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}
	f.drive(20)

	want := len(f.convs) * realRounds
	first := collectApplied(t, tots[0], want, 5*time.Second)
	firstSeq := payloadsOf(first)

	for i := 1; i < len(tots); i++ {
		got := collectApplied(t, tots[i], want, 5*time.Second)
		gotSeq := payloadsOf(got)
		if !slices.Equal(firstSeq, gotSeq) {
			t.Fatalf("Total replica %d sequence differs from replica 0:\n  rep0:  %v\n  rep%d: %v",
				i, firstSeq, i, gotSeq)
		}
	}
}

// TestTotalRespectsCausalOrder: even though Total imposes a total
// order, no message ever appears before one of its causal
// predecessors. b-2 at the end is a settle message so wave-2's
// a-2 becomes stable.
func TestTotalRespectsCausalOrder(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob"}, 7)
	a, b := f.convs[0], f.convs[1]
	ta := order.NewTotal(a)
	defer ta.Close()
	tb := order.NewTotal(b)
	defer tb.Close()

	// alice sends, bob replies, alice again, bob settles.
	_, _ = a.Send([]byte("a-1"))
	f.drive(5)
	_, _ = b.Send([]byte("b-1")) // b-1 depends on a-1
	f.drive(5)
	_, _ = a.Send([]byte("a-2")) // a-2 depends on b-1
	f.drive(5)
	_, _ = b.Send([]byte("b-2")) // settle: acks a-2 so wave 2 is complete
	f.drive(10)

	for _, ord := range []order.Order{ta, tb} {
		// Drain the first 3 — that's our assertion target. The 4th
		// (b-2) may or may not be applied yet, depending on whether
		// wave 2 has been wave-completed; either way we don't
		// inspect it.
		got := collectApplied(t, ord, 3, 3*time.Second)
		seq := payloadsOf(got)
		want := []string{"a-1", "b-1", "a-2"}
		if !slices.Equal(seq, want) {
			t.Fatalf("Total causal order violated: got %v, want %v", seq, want)
		}
	}
}

// TestTotalWaitsForWaveCompletion: if a replica is silent, Total
// blocks on its wave until the wave can be declared complete via
// stability.
func TestTotalWaitsForWaveCompletion(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob"}, 3)
	a, b := f.convs[0], f.convs[1]
	ta := order.NewTotal(a)
	defer ta.Close()

	// alice sends; bob does NOT reply yet.
	_, _ = a.Send([]byte("a-1"))
	f.drive(8)
	// alice's Total should NOT have applied yet — wave 1 has only
	// alice's message and is not complete (bob hasn't acked).
	select {
	case ap := <-ta.Apply():
		t.Fatalf("Total applied prematurely: %q (wave 1 not complete yet)", ap.Envelope.GetPayload())
	case <-time.After(100 * time.Millisecond):
	}

	// bob sends — that's an implicit ack of alice's wave-1 message.
	_, _ = b.Send([]byte("b-1"))
	f.drive(8)

	// Now Total should apply both.
	got := collectApplied(t, ta, 2, time.Second)
	seq := payloadsOf(got)
	want := []string{"a-1", "b-1"}
	if !slices.Equal(seq, want) {
		t.Fatalf("Total sequence after wave completion = %v, want %v", seq, want)
	}
}

func payloadsOf(applied []order.Applied) []string {
	out := make([]string, 0, len(applied))
	for _, a := range applied {
		out = append(out, string(a.Envelope.GetPayload()))
	}
	return out
}
