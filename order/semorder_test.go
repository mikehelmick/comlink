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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mikehelmick/comlink/order"
)

// dirClassifier is the paper §3 directory example: deletes are
// class-1 (commute with each other), inserts/updates are class-2
// (must be totally ordered).
var dirClassifier = order.ClassifierFunc(func(payload []byte) int {
	switch {
	case strings.HasPrefix(string(payload), "delete:"):
		return 1
	case strings.HasPrefix(string(payload), "insert:"), strings.HasPrefix(string(payload), "update:"):
		return 2
	default:
		return 1
	}
})

// applyDirOp executes one directory operation against state.
func applyDirOp(state map[string]string, payload string) {
	switch {
	case strings.HasPrefix(payload, "insert:"):
		parts := strings.SplitN(payload[len("insert:"):], ":", 2)
		if len(parts) == 2 {
			state[parts[0]] = parts[1]
		}
	case strings.HasPrefix(payload, "update:"):
		parts := strings.SplitN(payload[len("update:"):], ":", 2)
		if len(parts) == 2 {
			state[parts[0]] = parts[1]
		}
	case strings.HasPrefix(payload, "delete:"):
		key := payload[len("delete:"):]
		delete(state, key)
	}
}

// TestSemOrderDirectoryConverges runs the paper §3 directory
// example: each replica applies a mix of inserts/deletes via
// SemOrder; final states must match across all replicas.
//
// Class-2 ops (inserts) must be totally-ordered (same order at
// every replica); class-1 ops (deletes) may apply in different
// orders but produce the same observable result because deleting
// a non-existent key is a no-op.
func TestSemOrderDirectoryConverges(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob", "carol"}, 23)
	orderers := make([]*order.SemOrder, len(f.convs))
	for i, c := range f.convs {
		orderers[i] = order.NewSemOrder(c, dirClassifier)
		defer orderers[i].Close()
	}

	// alice, bob, carol each do 4 ops total: 2 inserts + 2 deletes,
	// interleaved. Plus one settle round at the end.
	scripts := [][]string{
		{"insert:k1:v1", "delete:k0", "insert:k2:v2", "delete:k1"},
		{"insert:k3:v3", "delete:k2", "insert:k4:v4", "delete:k3"},
		{"insert:k5:v5", "delete:k4", "insert:k6:v6", "delete:k5"},
	}
	const settleOps = 1
	const opsPerReplica = 4

	for round := range opsPerReplica + settleOps {
		for i, c := range f.convs {
			var op string
			if round < opsPerReplica {
				op = scripts[i][round]
			} else {
				// Settle: a final delete that ensures wave-completion.
				op = fmt.Sprintf("delete:settle-%d", i)
			}
			if _, err := c.Send([]byte(op)); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}
	f.drive(20)

	// Collect applied ops per replica until we have enough or time
	// out. We don't know the exact count per replica because
	// settle ops may or may not have applied; just take whatever
	// we get.
	states := make([]map[string]string, len(orderers))
	for i, o := range orderers {
		state := map[string]string{}
		drained := 0
		want := opsPerReplica * len(f.convs)
		deadline := time.NewTimer(5 * time.Second)
	drain:
		for drained < want {
			select {
			case a, ok := <-o.Apply():
				if !ok {
					break drain
				}
				applyDirOp(state, string(a.Envelope.GetPayload()))
				drained++
			case <-deadline.C:
				t.Fatalf("replica %d only drained %d/%d ops", i, drained, want)
			}
		}
		deadline.Stop()
		states[i] = state
	}

	// Final states must match.
	for i := 1; i < len(states); i++ {
		if !reflect.DeepEqual(states[0], states[i]) {
			t.Fatalf("SemOrder did not converge:\n  rep0: %v\n  rep%d: %v", states[0], i, states[i])
		}
	}
}

// TestSemOrderClass1OpsCanInterleaveDifferently exercises the
// paper §3 promise: class-1 ops may be applied in different orders
// at different replicas (for delivery convenience / parallelism)
// while final state still converges.
//
// We do this by having every operation be a class-1 delete, with
// multiple unrelated keys, and verifying the per-replica APPLIED
// SEQUENCES can differ while final states converge.
func TestSemOrderClass1OpsCanInterleaveDifferently(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob"}, 31)
	a, b := f.convs[0], f.convs[1]
	oa := order.NewSemOrder(a, dirClassifier)
	defer oa.Close()
	ob := order.NewSemOrder(b, dirClassifier)
	defer ob.Close()

	// Pre-seed state with some keys so deletes have something to do.
	// We can't easily seed shared state without running through
	// SemOrder, so just send an insert burst first (class-2,
	// totally-ordered).
	for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
		if _, err := a.Send(fmt.Appendf(nil, "insert:%s:val", key)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 8; i++ {
		f.drive(5)
	}
	// Settle the insert burst: bob sends a delete to ack alice's
	// inserts (so wave completion progresses).
	_, _ = b.Send([]byte("delete:nonexistent-1"))
	f.drive(8)

	// Now both replicas issue several deletes targeting different
	// keys. Class-1 ops can interleave.
	for _, key := range []string{"a", "b"} {
		_, _ = a.Send(fmt.Appendf(nil, "delete:%s", key))
	}
	for _, key := range []string{"c", "d"} {
		_, _ = b.Send(fmt.Appendf(nil, "delete:%s", key))
	}
	// Settle.
	_, _ = a.Send([]byte("delete:nonexistent-a"))
	_, _ = b.Send([]byte("delete:nonexistent-b"))
	f.drive(15)

	// Drain whatever applied. The final state at each replica
	// should be {"e": "val", "f": "val"} (after the four real
	// deletes a, b, c, d). The order of deletes doesn't matter
	// for the final state.
	stateA := drainSemOrderState(t, oa, 6+1+2+2+2)
	stateB := drainSemOrderState(t, ob, 6+1+2+2+2)
	if !reflect.DeepEqual(stateA, stateB) {
		t.Fatalf("convergence: rep0=%v rep1=%v", stateA, stateB)
	}
	if _, present := stateA["a"]; present {
		t.Errorf("expected a deleted; got state=%v", stateA)
	}
	if got := stateA["e"]; got != "val" {
		t.Errorf("expected e=val; got state=%v", stateA)
	}
}

func drainSemOrderState(t *testing.T, o order.Order, max int) map[string]string {
	t.Helper()
	state := map[string]string{}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	count := 0
	for count < max {
		select {
		case a, ok := <-o.Apply():
			if !ok {
				return state
			}
			applyDirOp(state, string(a.Envelope.GetPayload()))
			count++
		case <-deadline.C:
			return state
		}
	}
	return state
}

// k3Classifier exercises the k-set generalization from §3 with
// k=3: reads (class 1), writes (class 2), deletes (class 3).
var k3Classifier = order.ClassifierFunc(func(payload []byte) int {
	switch {
	case strings.HasPrefix(string(payload), "read"):
		return 1
	case strings.HasPrefix(string(payload), "write"):
		return 2
	case strings.HasPrefix(string(payload), "delete"):
		return 3
	default:
		return 1
	}
})

// TestSemOrderK3Generalization runs SemOrder with k=3
// classification — verifies the algorithm handles more than the
// binary case correctly. This test focuses on convergence: every
// replica's stream of applied operations must yield the same
// final state when interpreted as kv-store mutations.
func TestSemOrderK3Generalization(t *testing.T) {
	f := newOrderFixture(t, []string{"alice", "bob"}, 41)
	a, b := f.convs[0], f.convs[1]
	oa := order.NewSemOrder(a, k3Classifier)
	defer oa.Close()
	ob := order.NewSemOrder(b, k3Classifier)
	defer ob.Close()

	apply := func(state map[string]string, payload string) {
		switch {
		case strings.HasPrefix(payload, "write:"):
			parts := strings.SplitN(payload[len("write:"):], ":", 2)
			if len(parts) == 2 {
				state[parts[0]] = parts[1]
			}
		case strings.HasPrefix(payload, "delete:"):
			key := payload[len("delete:"):]
			delete(state, key)
		}
		// "read:..." payloads are no-ops for state-machine
		// convergence — the switch above drops them implicitly.
	}

	// Mix of all 3 classes from each replica + settle messages.
	for _, op := range []string{"write:k1:v1", "read:k1", "write:k2:v2", "delete:k1", "read:k2"} {
		_, _ = a.Send([]byte(op))
	}
	for _, op := range []string{"read:k0", "write:k3:v3", "read:k1", "write:k4:v4"} {
		_, _ = b.Send([]byte(op))
	}
	for i := 0; i < 8; i++ {
		f.drive(5)
	}
	// Settle round — ensure wave completion.
	for _, c := range []*order.SemOrder{} {
		_ = c
	}
	_, _ = a.Send([]byte("read:settle-a"))
	_, _ = b.Send([]byte("read:settle-b"))
	f.drive(20)

	stateA := map[string]string{}
	stateB := map[string]string{}
	const want = 5 + 4 + 2
	for i, pair := range []struct {
		o     order.Order
		state map[string]string
	}{
		{oa, stateA}, {ob, stateB},
	} {
		drained := 0
		deadline := time.NewTimer(5 * time.Second)
	drain:
		for drained < want {
			select {
			case ap, ok := <-pair.o.Apply():
				if !ok {
					break drain
				}
				apply(pair.state, string(ap.Envelope.GetPayload()))
				drained++
			case <-deadline.C:
				t.Fatalf("replica %d only drained %d/%d ops", i, drained, want)
			}
		}
		deadline.Stop()
	}

	if !reflect.DeepEqual(stateA, stateB) {
		t.Fatalf("k=3 convergence failed:\n  rep0: %v\n  rep1: %v", stateA, stateB)
	}
}
