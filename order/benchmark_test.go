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
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/order"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// BenchmarkOrderResponseTime measures the per-operation response
// time for the replicated-directory workload under Total vs
// SemOrder across mixes of class-1 (commutative) operations,
// tracking the paper's Tables 1 and 2.
//
// "Response time" matches the paper's definition (§6.2): elapsed
// time between Send and the corresponding Apply on the same
// replica.
func BenchmarkOrderResponseTime(b *testing.B) {
	for _, ordCfg := range []struct {
		name string
		make func(c *psync.Conversation) order.Order
	}{
		{"Total", func(c *psync.Conversation) order.Order { return order.NewTotal(c) }},
		{"SemOrder",
			func(c *psync.Conversation) order.Order {
				return order.NewSemOrder(c, dirClassifier)
			},
		},
	} {
		for _, pctCommutative := range []int{0, 50, 75, 90, 99, 100} {
			name := fmt.Sprintf("%s/comm=%d%%", ordCfg.name, pctCommutative)
			b.Run(name, func(b *testing.B) {
				runResponseTimeBenchmark(b, ordCfg.make, pctCommutative)
			})
		}
	}
}

func runResponseTimeBenchmark(b *testing.B, makeOrder func(*psync.Conversation) order.Order, pctCommutative int) {
	ctx := context.Background()
	convID := &pb.ConversationID{Value: id16("ord-bench")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(101)
	defer sched.Close()

	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))
	alice, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		DeliveryBufSize: 8192,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer alice.Close()
	bob, err := psync.New(ctx, psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: clog.NewMemory(convID), Storage: stable.NewMemory(),
		DeliveryBufSize: 8192,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer bob.Close()

	oa := makeOrder(alice)
	defer oa.Close()
	ob := makeOrder(bob)
	defer ob.Close()

	// Background scheduler pump.
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

	// Drain bob's apply channel in the background so it doesn't
	// backpressure (we measure alice's response time only).
	bobDrain := make(chan struct{})
	go func() {
		defer close(bobDrain)
		for {
			select {
			case _, ok := <-ob.Apply():
				if !ok {
					return
				}
			case <-stop:
				return
			}
		}
	}()
	defer func() { <-bobDrain }()

	// payloadFor returns a directory op with class chosen by the
	// pctCommutative split. iter is used as a per-iteration nonce
	// to keep MessageIDs distinct (the seq counter takes care of
	// that already, but unique payloads make perf-time analysis
	// readable).
	payloadFor := func(iter int, commutative bool) []byte {
		if commutative {
			// delete (class 1) — ensure unique key so this delete
			// isn't trivially a no-op.
			return fmt.Appendf(nil, "delete:k%d", iter)
		}
		// insert (class 2)
		return fmt.Appendf(nil, "insert:k%d:v%d", iter, iter)
	}

	b.ResetTimer()
	iter := 0
	for b.Loop() {
		commutative := (iter % 100) < pctCommutative
		payload := payloadFor(iter, commutative)
		start := time.Now()
		if _, err := alice.Send(payload); err != nil {
			b.Fatal(err)
		}
		// Wait for alice's Apply on this op (or any subsequent op
		// that catches up). Loop because Total/SemOrder may apply
		// in batches.
		seen := false
		for !seen {
			select {
			case a, ok := <-oa.Apply():
				if !ok {
					b.Fatal("alice Apply channel closed unexpectedly")
				}
				if string(a.Envelope.GetPayload()) == string(payload) {
					b.ReportMetric(float64(time.Since(start).Microseconds()), "us/op-resp")
					seen = true
				}
			case <-time.After(10 * time.Second):
				b.Fatalf("response timeout: iter=%d payload=%q", iter, payload)
			}
		}
		iter++
	}
}

