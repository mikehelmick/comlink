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

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport/memory"
)

// ─── replicated KV — the "user application" half ─────────────────

// kvOp is the serialized command form. The substrate's wire
// format is bytes; the app picks its own encoding (JSON here for
// readability — production apps would use protobuf or msgpack).
type kvOp struct {
	Op string `json:"op"` // "set" | "del"
	K  string `json:"k"`
	V  string `json:"v,omitempty"`
}

// kvStore is the replicated state machine. Apply is the only
// mutation path; reads go through Get without going through the
// substrate (eventually-consistent local reads).
type kvStore struct {
	mu   sync.Mutex
	data map[string]string
}

func newKVStore() *kvStore {
	return &kvStore{data: make(map[string]string)}
}

func (s *kvStore) Apply(ctx context.Context, msg *comlink.Message) {
	var o kvOp
	if err := json.Unmarshal(msg.Payload, &o); err != nil {
		return // malformed op; ignore (deterministic).
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch o.Op {
	case "set":
		s.data[o.K] = o.V
	case "del":
		delete(s.data, o.K)
	}
}

func (s *kvStore) Get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *kvStore) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// kvSet is a convenience wrapper. In ~80 lines of app code we
// have a working replicated key-value store on top of comlink.
func kvSet(ctx context.Context, sub *comlink.Substrate, k, v string) error {
	bs, _ := json.Marshal(kvOp{Op: "set", K: k, V: v})
	_, err := sub.Submit(ctx, bs)
	return err
}

// ─── the test ────────────────────────────────────────────────────

// TestReplicatedKVConverges exercises the comlink public API
// end-to-end:
//
//   - 3 Cluster nodes on a shared in-memory transport
//   - One application Substrate per node (OrderingTotal — every
//     replica applies in the same order)
//   - kvStore as the user's StateMachine
//   - Each replica concurrently kvSets a unique key
//   - After settle, every replica must have the same {a,b,c}
//
// This is the PLAN §5 exit-criterion: a useful replicated state
// machine in under ~80 lines of application code (the kvOp +
// kvStore + kvSet block above).
func TestReplicatedKVConverges(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	replicas := []string{"alice", "bob", "carol"}
	members := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = comlink.ReplicaID(id16(name))
	}

	type node struct {
		cluster *comlink.Cluster
		sub     *comlink.Substrate
		store   *kvStore
	}
	nodes := make([]*node, len(replicas))
	convID, _ := comlink.NewConversationID()
	for i, name := range replicas {
		net, err := sched.Connect(&pb.ReplicaID{Value: id16(name)})
		if err != nil {
			t.Fatal(err)
		}
		c, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
			Self:      comlink.ReplicaID(id16(name)),
			Members:   []comlink.ReplicaID{comlink.ReplicaID(id16(name))},
			DataDir:   t.TempDir(),
			Bootstrap: &comlink.BootstrapConfig{Force: true},
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		store := newKVStore()
		sub, err := c.NewSubstrate(ctx, comlink.SubstrateConfig{
			ConversationID: convID,
			Members:        members,
			Ordering:       comlink.OrderingTotal,
			StateMachine:   store,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		nodes[i] = &node{cluster: c, sub: sub, store: store}
	}

	// Drive the in-memory scheduler in the background.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				sched.RunAll()
			}
		}
	}()
	defer close(stop)

	// Each replica concurrently sets its own key (a single op per
	// node — keeps the test fast while still exercising the full
	// pipeline of Submit → Order → SM.Apply across replicas).
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *node) {
			defer wg.Done()
			submitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			k := fmt.Sprintf("k-%d", idx)
			v := fmt.Sprintf("v-%d", idx)
			if err := kvSet(submitCtx, n.sub, k, v); err != nil {
				t.Errorf("replica %d kvSet(%s): %v", idx, k, err)
			}
		}(i, n)
	}
	wg.Wait()

	// Poll until every replica has applied all 3 ops.
	want := map[string]string{
		"k-0": "v-0",
		"k-1": "v-1",
		"k-2": "v-2",
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		converged := true
		for _, n := range nodes {
			got := n.store.Snapshot()
			if !sameMap(got, want) {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		if time.Now().After(deadline) {
			for i, n := range nodes {
				t.Errorf("replica %d snapshot = %v, want %v", i, n.store.Snapshot(), want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Sanity: cross-replica reads agree.
	for i, n := range nodes {
		if v, ok := n.store.Get("k-1"); !ok || v != "v-1" {
			t.Errorf("replica %d Get(k-1) = %q,%v; want v-1,true", i, v, ok)
		}
	}
}

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
