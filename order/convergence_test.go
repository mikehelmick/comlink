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
	"testing"
	"time"

	"github.com/mikehelmick/comlink/order"
	"github.com/mikehelmick/comlink/psync"
)

// TestReplicatedCounterConvergesUnderAllOrderings is the Phase 2
// exit-criterion convergence test: a replicated counter (each
// replica issues a fixed number of increments) yields the same
// final value at every replica under PartialOrder, Total, and
// SemOrder.
//
// Increments are fully commutative ("add 1" doesn't depend on what
// other increments are interleaved), so every ordering should
// converge to the same total.
func TestReplicatedCounterConvergesUnderAllOrderings(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(c *psync.Conversation) order.Order
	}{
		{"PartialOrder", func(c *psync.Conversation) order.Order { return order.NewPartial(c) }},
		{"Total", func(c *psync.Conversation) order.Order { return order.NewTotal(c) }},
		{"SemOrder",
			func(c *psync.Conversation) order.Order {
				// All ops are class 1 (commutative).
				return order.NewSemOrder(c, order.ClassifierFunc(func([]byte) int { return 1 }))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runCounterTest(t, tc.make)
		})
	}
}

func runCounterTest(t *testing.T, makeOrder func(*psync.Conversation) order.Order) {
	t.Helper()
	f := newOrderFixture(t, []string{"alice", "bob", "carol"}, 53)
	orderers := make([]order.Order, len(f.convs))
	for i, c := range f.convs {
		orderers[i] = makeOrder(c)
		defer orderers[i].Close()
	}

	const incsPerReplica = 5
	const settle = 1
	want := incsPerReplica * len(f.convs)

	for round := 0; round < incsPerReplica+settle; round++ {
		for _, c := range f.convs {
			if _, err := c.Send([]byte("inc")); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}
	f.drive(20)

	// Every replica should observe enough applied "inc" ops to
	// reach the total. (Total + SemOrder may need wave-completion;
	// Partial gets every delivery immediately. We always have at
	// least `want` ops because of the +settle round; only count up
	// to `want`.)
	for i, o := range orderers {
		count := 0
		deadline := time.NewTimer(5 * time.Second)
	drain:
		for count < want {
			select {
			case a, ok := <-o.Apply():
				if !ok {
					break drain
				}
				if string(a.Envelope.GetPayload()) == "inc" {
					count++
				}
			case <-deadline.C:
				t.Fatalf("[replica %d] only saw %d/%d inc ops", i, count, want)
			}
		}
		deadline.Stop()
		if count != want {
			t.Fatalf("[replica %d] counter = %d, want %d", i, count, want)
		}
	}
}

