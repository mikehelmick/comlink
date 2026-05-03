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
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// TestCausalOrderUnderRandomReorder is the Phase 1 exit-criterion
// property test. For a sweep of seeds, each running an N-replica
// conversation with the scheduler in reorder mode, verify that
// every replica's delivered sequence respects causal order:
// for each delivery, every causal predecessor (per its vector
// clock) was already delivered earlier at this replica.
//
// We don't inject loss here — Psync's lost-message protocol
// requires retransmits, which the in-memory scheduler doesn't
// provide once a message is dropped. Reorder + delay (via random
// scheduling order) is the relevant property for the in-memory
// transport.
func TestCausalOrderUnderRandomReorder(t *testing.T) {
	const seeds = 25 // number of randomized scenarios
	const replicas = 3
	const sendsPerReplica = 4

	for seed := uint64(1); seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runCausalOrderScenario(t, seed, replicas, sendsPerReplica)
		})
	}
}

func runCausalOrderScenario(t *testing.T, seed uint64, nReplicas, sendsEach int) {
	t.Helper()
	ctx := context.Background()

	convID := &pb.ConversationID{Value: id16(fmt.Sprintf("prop-%d", seed))}
	members := make([]*pb.ReplicaID, nReplicas)
	for i := range nReplicas {
		members[i] = r(fmt.Sprintf("rep-%d", i))
	}
	sched := memory.NewScheduler(seed)
	sched.SetReorder(true)
	defer sched.Close()

	convs := make([]*psync.Conversation, nReplicas)
	for i := range nReplicas {
		net, err := sched.Connect(members[i])
		if err != nil {
			t.Fatal(err)
		}
		c, err := psync.New(ctx, psync.Config{
			ConversationID: convID, Self: members[i], Members: members,
			Network: net, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
			DeliveryBufSize: 1024,
		})
		if err != nil {
			t.Fatal(err)
		}
		convs[i] = c
	}
	defer func() {
		for _, c := range convs {
			_ = c.Close()
		}
	}()

	// Each replica emits sendsEach messages; we interleave the rounds.
	for round := range sendsEach {
		for i, c := range convs {
			if _, err := c.Send(fmt.Appendf(nil, "%d-%d", i, round)); err != nil {
				t.Fatal(err)
			}
		}
		// Drain after each round so the scheduler queue doesn't grow
		// without bound; this also gives the system time to deliver.
		for j := 0; j < 8; j++ {
			sched.RunAll()
			time.Sleep(2 * time.Millisecond)
		}
	}
	// Final settle.
	for j := 0; j < 25; j++ {
		sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}

	want := nReplicas * sendsEach
	for i, c := range convs {
		got := drain(t, c, want)
		// Verify causal-order invariant per replica.
		seen := make(map[indexKey]int)
		for pos, d := range got {
			senderBytes := d.Envelope.GetId().GetSender().GetValue()
			senderSeq := d.Node.SenderSeq
			senderSlot := d.Node.SenderSlot
			vc := d.Envelope.GetId().GetVectorClock()
			for slot, depSeq := range vc {
				var requiredSeq uint64
				if slot == senderSlot {
					if senderSeq <= 1 {
						continue
					}
					requiredSeq = senderSeq - 1
				} else {
					if depSeq == 0 {
						continue
					}
					requiredSeq = depSeq
				}
				parentBytes := members[slot].GetValue()
				k := indexKey{sender: string(parentBytes), seq: requiredSeq}
				parentPos, ok := seen[k]
				if !ok {
					t.Fatalf("seed=%d replica=%d pos=%d (sender=%x seq=%d): parent (slot=%d, seq=%d) NEVER delivered before",
						seed, i, pos, senderBytes, senderSeq, slot, requiredSeq)
				}
				if parentPos >= pos {
					t.Fatalf("seed=%d replica=%d pos=%d: parent (slot=%d, seq=%d) delivered at pos %d (>= self)",
						seed, i, pos, slot, requiredSeq, parentPos)
				}
			}
			seen[indexKey{sender: string(senderBytes), seq: senderSeq}] = pos
		}
	}
}

// BenchmarkPsyncRoundTrip measures the latency of one Send +
// peer-delivery using the in-memory transport. Comparable to the
// paper's Table-1 ~2.9 ms one-byte round trip on Sun-3/75 over 10
// Mbit Ethernet — modern numbers will be very different, the point
// is to track ours over time and catch regressions at the phase
// that introduces them.
func BenchmarkPsyncRoundTrip(b *testing.B) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("psync-bench")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(1)
	defer sched.Close()
	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))
	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		DeliveryBufSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer alice.Close()
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		DeliveryBufSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer bob.Close()

	// Background pump.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				sched.RunAll()
			}
		}
	}()
	defer close(stop)

	payload := []byte{42}
	b.ResetTimer()
	for b.Loop() {
		if _, err := alice.Send(payload); err != nil {
			b.Fatal(err)
		}
		<-alice.Recv() // self-delivery
		<-bob.Recv()   // peer delivery
	}
}
