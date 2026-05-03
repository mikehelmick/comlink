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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// fixture wires a set of Conversations together over a shared
// in-memory scheduler. Tests drive delivery via fixture.RunAll
// (synchronous, deterministic) or by manual Step calls when they
// want to observe in-flight states.
type fixture struct {
	t       *testing.T
	sched   *memory.Scheduler
	convs   []*psync.Conversation
	members []*pb.ReplicaID
	convID  *pb.ConversationID
}

func newFixture(t *testing.T, replicas []string, seed uint64) *fixture {
	t.Helper()
	convID := &pb.ConversationID{Value: id16("psync-test-conv")}
	members := make([]*pb.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = r(name)
	}
	sched := memory.NewScheduler(seed)
	t.Cleanup(func() { _ = sched.Close() })

	f := &fixture{
		t:       t,
		sched:   sched,
		members: members,
		convID:  convID,
	}
	for _, m := range members {
		f.convs = append(f.convs, f.spinUp(m))
	}
	t.Cleanup(func() {
		for _, c := range f.convs {
			_ = c.Close()
		}
	})
	return f
}

func (f *fixture) spinUp(self *pb.ReplicaID) *psync.Conversation {
	f.t.Helper()
	net, err := f.sched.Connect(self)
	if err != nil {
		f.t.Fatal(err)
	}
	storage := stable.NewMemory()
	logImpl := clog.NewMemory(f.convID)
	c, err := psync.New(context.Background(), psync.Config{
		ConversationID: f.convID,
		Self:           self,
		Members:        f.members,
		Network:        net,
		Log:            logImpl,
		Storage:        storage,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

// id16 / r are reused from membership_test.go via package-level test
// helpers; that file lives in the same _test package so they're
// already in scope here.

func id16(tag string) []byte {
	b := make([]byte, 16)
	copy(b, tag)
	return b
}

// drain pulls expected deliveries from c.Recv with a deadline.
func drain(t *testing.T, c *psync.Conversation, want int) []psync.Delivery {
	t.Helper()
	out := make([]psync.Delivery, 0, want)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for len(out) < want {
		select {
		case d, ok := <-c.Recv():
			if !ok {
				t.Fatalf("Recv closed after %d deliveries; wanted %d", len(out), want)
			}
			out = append(out, d)
		case <-deadline.C:
			t.Fatalf("timeout: only %d/%d deliveries arrived", len(out), want)
		}
	}
	return out
}

// runUntilQuiescent calls sched.RunAll repeatedly until both the
// scheduler queue is empty and no replica is mid-mailbox-process.
// In our deterministic in-memory transport, RunAll until nothing
// remains pending is sufficient because each delivery's
// downstream effects synchronously enqueue more messages.
func (f *fixture) runUntilQuiescent() {
	for {
		f.sched.RunAll()
		// Brief sleep to let any handler-spawned sends land in the
		// scheduler queue. (Conversation.handleSend runs inside the
		// processing goroutine and synchronously calls
		// network.Send, which enqueues onto the scheduler — but the
		// network.Send happens after the receiver's mailbox dispatch
		// finishes, so the queue may be empty when we check while a
		// handler is still mid-execution.) The runtime ordering
		// between RunAll, the receiver loop, and handler-side sends
		// requires us to pause briefly to let goroutines settle.
		time.Sleep(5 * time.Millisecond)
		if f.sched.Pending() == 0 {
			return
		}
	}
}

// ─── tests ────────────────────────────────────────────────────────

func TestSendDeliversToSelfAndPeer(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice, bob := f.convs[0], f.convs[1]

	id, err := alice.Send([]byte("hello bob"))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == nil {
		t.Fatal("Send returned nil id")
	}
	f.runUntilQuiescent()

	got := drain(t, alice, 1)
	if string(got[0].Envelope.GetPayload()) != "hello bob" {
		t.Fatalf("alice self-delivery payload = %q", got[0].Envelope.GetPayload())
	}
	got = drain(t, bob, 1)
	if string(got[0].Envelope.GetPayload()) != "hello bob" {
		t.Fatalf("bob delivery payload = %q", got[0].Envelope.GetPayload())
	}
}

func TestVectorClockOnSend(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice := f.convs[0]
	id, err := alice.Send([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	vc := id.GetVectorClock()
	want := []uint64{1, 0}
	if len(vc) != 2 || vc[0] != want[0] || vc[1] != want[1] {
		t.Fatalf("first send vector_clock = %v, want %v", vc, want)
	}
}

func TestSecondSendIncrementsSenderSlot(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice := f.convs[0]
	_, _ = alice.Send([]byte("a"))
	id2, err := alice.Send([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	vc := id2.GetVectorClock()
	if vc[0] != 2 {
		t.Fatalf("second alice send vector_clock[0] = %d, want 2", vc[0])
	}
}

func TestRoundTripIncludesPeerInVector(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice, bob := f.convs[0], f.convs[1]

	_, _ = alice.Send([]byte("a"))
	f.runUntilQuiescent()
	drain(t, alice, 1)
	drain(t, bob, 1)

	id, _ := bob.Send([]byte("b reply"))
	vc := id.GetVectorClock()
	// Bob's slot is 1; bob's first send must include alice's seq 1
	// at slot 0.
	if vc[0] != 1 || vc[1] != 1 {
		t.Fatalf("bob's reply vector_clock = %v, want [1 1]", vc)
	}
}

// TestLostMessageProtocol: deliver a parent OUT OF ORDER. We do
// this by using the in-memory transport's reorder mode, sending two
// messages from the same sender, and confirming the receiver
// eventually delivers them in causal order.
//
// Setup: alice sends two messages back-to-back. With reorder
// enabled and a seed that flips them, bob receives M2 first. M2's
// vector_clock[alice] = 2 — so bob defers it and requests alice's
// seq 1. alice retransmits. Bob then delivers M1 then M2.
func TestLostMessageProtocolReorderedDelivery(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 7)
	f.sched.SetReorder(true)
	alice, bob := f.convs[0], f.convs[1]

	if _, err := alice.Send([]byte("m1")); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.Send([]byte("m2")); err != nil {
		t.Fatal(err)
	}
	f.runUntilQuiescent()

	// Alice self-delivers both; verify.
	aliceDeliveries := drain(t, alice, 2)
	if string(aliceDeliveries[0].Envelope.GetPayload()) != "m1" ||
		string(aliceDeliveries[1].Envelope.GetPayload()) != "m2" {
		t.Fatalf("alice self-deliveries out of order: %q, %q",
			aliceDeliveries[0].Envelope.GetPayload(),
			aliceDeliveries[1].Envelope.GetPayload())
	}

	// Bob receives both, in causal order regardless of network order.
	bobDeliveries := drain(t, bob, 2)
	if string(bobDeliveries[0].Envelope.GetPayload()) != "m1" ||
		string(bobDeliveries[1].Envelope.GetPayload()) != "m2" {
		t.Fatalf("bob delivery violated causal order: %q, %q",
			bobDeliveries[0].Envelope.GetPayload(),
			bobDeliveries[1].Envelope.GetPayload())
	}
}

func TestMaskoutDropsMessagesFromMaskedReplica(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice, bob := f.convs[0], f.convs[1]

	if err := bob.Maskout(ctx, r("alice")); err != nil {
		t.Fatal(err)
	}

	// Alice sends but bob shouldn't receive.
	_, _ = alice.Send([]byte("hidden"))
	f.runUntilQuiescent()

	// Alice still self-delivers.
	drain(t, alice, 1)

	// Bob's recv should be empty.
	select {
	case d := <-bob.Recv():
		t.Fatalf("bob got delivery despite mask: %q", d.Envelope.GetPayload())
	case <-time.After(100 * time.Millisecond):
	}

	// Maskin and a fresh send should be received.
	if err := bob.Maskin(ctx, r("alice")); err != nil {
		t.Fatal(err)
	}
	_, _ = alice.Send([]byte("visible"))
	f.runUntilQuiescent()
	// Alice self-deliveries: the second one.
	drain(t, alice, 1)
	// Bob should now receive "visible". But wait — bob's graph
	// doesn't have alice's seq 1 (the masked send), so when the
	// "visible" message arrives at bob with vector [2, 0], bob will
	// defer it and request alice's seq 1. Alice will retransmit.
	// Eventually bob receives both "hidden" and "visible".
	bobDeliveries := drain(t, bob, 2)
	if string(bobDeliveries[0].Envelope.GetPayload()) != "hidden" ||
		string(bobDeliveries[1].Envelope.GetPayload()) != "visible" {
		t.Fatalf("after Maskin, bob got %q, %q; want hidden, visible",
			bobDeliveries[0].Envelope.GetPayload(),
			bobDeliveries[1].Envelope.GetPayload())
	}
}

func TestThreeReplicaConvergence(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob", "carol"}, 1)
	const N = 5
	// Each replica sends a few messages.
	for i := 0; i < N; i++ {
		for _, c := range f.convs {
			if _, err := c.Send(fmt.Appendf(nil, "msg-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		f.runUntilQuiescent()
	}
	// Every replica should have received N*3 deliveries (own +
	// each peer's N).
	for i, c := range f.convs {
		got := drain(t, c, N*3)
		if len(got) != N*3 {
			t.Fatalf("replica %d got %d deliveries, want %d", i, len(got), N*3)
		}
	}
}

func TestCausalOrderIsRespectedAtAllReplicas(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 13)
	f.sched.SetReorder(true)
	alice, bob := f.convs[0], f.convs[1]

	// Alice sends 3 messages; bob sends 3 messages interleaved.
	const N = 3
	for i := 0; i < N; i++ {
		_, _ = alice.Send(fmt.Appendf(nil, "a-%d", i))
		f.runUntilQuiescent()
		_, _ = bob.Send(fmt.Appendf(nil, "b-%d", i))
		f.runUntilQuiescent()
	}

	verifyCausalOrder := func(name string, c *psync.Conversation) {
		got := drain(t, c, 2*N)
		// For each delivered envelope, every causal predecessor
		// (per its vector clock) must have been delivered earlier
		// in the slice.
		seen := make(map[indexKey]int) // sender bytes + seq -> position in slice
		for pos, d := range got {
			vc := d.Envelope.GetId().GetVectorClock()
			senderBytes := d.Envelope.GetId().GetSender().GetValue()
			senderSeq := d.Node.SenderSeq
			senderSlot := d.Node.SenderSlot
			for slot, depSeq := range vc {
				if slot == senderSlot {
					if senderSeq <= 1 {
						continue
					}
					depSeq = senderSeq - 1
				}
				if depSeq == 0 {
					continue
				}
				parentBytes := f.members[slot].GetValue()
				k := indexKey{sender: string(parentBytes), seq: depSeq}
				parentPos, ok := seen[k]
				if !ok {
					t.Fatalf("[%s] delivery at pos %d (sender=%x seq=%d) referenced parent (slot=%d, seq=%d) that was never delivered",
						name, pos, senderBytes, senderSeq, slot, depSeq)
				}
				if parentPos >= pos {
					t.Fatalf("[%s] delivery at pos %d (sender=%x seq=%d) referenced parent (slot=%d, seq=%d) delivered at pos %d (>= self)",
						name, pos, senderBytes, senderSeq, slot, depSeq, parentPos)
				}
			}
			seen[indexKey{sender: string(senderBytes), seq: senderSeq}] = pos
		}
	}
	verifyCausalOrder("alice", alice)
	verifyCausalOrder("bob", bob)
}

// indexKey duplicated locally for the test (the package-internal
// type isn't exported).
type indexKey struct {
	sender string
	seq    uint64
}

// TestEachAcceptedMessageIsLogged confirms PLAN §1 durability:
// every Delivery this conversation emits also exists in its log.
func TestEachAcceptedMessageIsLogged(t *testing.T) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("durability")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(2)
	t.Cleanup(func() { _ = sched.Close() })
	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))

	aliceLog := clog.NewMemory(convID)
	bobLog := clog.NewMemory(convID)

	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: aliceLog, Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: bobLog, Storage: stable.NewMemory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alice.Close(); _ = bob.Close() })

	if _, err := alice.Send([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		sched.RunAll()
		time.Sleep(5 * time.Millisecond)
	}

	// Drain deliveries on alice and bob.
	count := atomic.Int32{}
	for _, c := range []*psync.Conversation{alice, bob} {
		select {
		case <-c.Recv():
			count.Add(1)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for delivery on a replica")
		}
	}

	// Both logs must contain alice's seq 1.
	for name, lg := range map[string]clog.MessageLog{"alice": aliceLog, "bob": bobLog} {
		entry, err := lg.LookupBySender(ctx, r("alice").GetValue(), 1)
		if err != nil {
			t.Errorf("[%s] log Lookup: %v", name, err)
			continue
		}
		if !bytes.Equal(entry.Envelope.GetPayload(), []byte("durable")) {
			t.Errorf("[%s] logged payload = %q, want %q", name, entry.Envelope.GetPayload(), "durable")
		}
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	f := newFixture(t, []string{"alice", "bob"}, 1)
	alice := f.convs[0]
	if err := alice.Close(); err != nil {
		t.Fatal(err)
	}
	if err := alice.Close(); err != nil {
		t.Fatal("second Close returned err: " + err.Error())
	}
}
