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

package comlink_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/transport/memory"
)

// counterSM is a minimal replicated counter — increments a
// counter for every Apply'd message.
type counterSM struct {
	mu    sync.Mutex
	count int
}

func (c *counterSM) Apply(ctx context.Context, msg *comlink.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
}

func (c *counterSM) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// TestSubstrateSingleReplicaSubmitApplies: a one-node cluster
// can Submit a command and the SM Apply runs locally.
func TestSubstrateSingleReplicaSubmitApplies(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	cluster, err := comlink.NewCluster(ctx, founderClusterConfig(t, sched, "alice", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	sm := &counterSM{}
	convID, _ := comlink.NewConversationID()
	sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   sm,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	subCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := sub.Submit(subCtx, []byte("inc")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if got := sm.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}
}

// TestSubstrateMultiReplicaConverges spins up 3 Cluster + 3
// Substrate (all on the same in-memory scheduler) and verifies
// that all replicas converge on the same count after every
// Submit.
func TestSubstrateMultiReplicaConverges(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	replicas := []string{"alice", "bob", "carol"}
	members := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = id16(name)
	}

	type node struct {
		cluster *comlink.Cluster
		sub     *comlink.Substrate
		sm      *counterSM
	}
	nodes := make([]*node, len(replicas))
	convID, _ := comlink.NewConversationID()
	for i, name := range replicas {
		net, err := sched.Connect(pbID(id16(name)))
		if err != nil {
			t.Fatal(err)
		}
		c, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
			Self:      id16(name),
			Members:   []comlink.ReplicaID{id16(name)}, // single-node system convs (we don't exercise system here)
			DataDir:   t.TempDir(),
			Bootstrap: &comlink.BootstrapConfig{Force: true},
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		sm := &counterSM{}
		sub, err := c.NewSubstrate(ctx, comlink.SubstrateConfig{
			ConversationID: convID,
			Members:        members,
			Ordering:       comlink.OrderingTotal,
			StateMachine:   sm,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		nodes[i] = &node{cluster: c, sub: sub, sm: sm}
	}

	// Run scheduler in background.
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

	// Each replica submits concurrently (NOT round-robin
	// synchronously — with OrderingTotal that would deadlock,
	// since each Submit blocks on local Apply, which blocks on
	// wave-completion, which needs other replicas' messages).
	const realRounds = 3
	const settleRounds = 1
	const totalRounds = realRounds + settleRounds
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *node) {
			defer wg.Done()
			for round := 0; round < totalRounds; round++ {
				subCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				if _, err := n.sub.Submit(subCtx, []byte{byte(idx), byte(round)}); err != nil {
					t.Errorf("replica %d round %d Submit: %v", idx, round, err)
					cancel()
					return
				}
				cancel()
			}
		}(i, n)
	}
	wg.Wait()

	// Each replica should EVENTUALLY have applied at least
	// realRounds*N (Submit only waits for self-apply; peer
	// applies trail by the network roundtrip + ordering
	// pipeline). Poll for convergence.
	want := realRounds * len(nodes)
	deadline := time.Now().Add(5 * time.Second)
	for {
		allConverged := true
		for _, n := range nodes {
			if n.sm.Count() < want {
				allConverged = false
				break
			}
		}
		if allConverged {
			break
		}
		if time.Now().After(deadline) {
			for i, n := range nodes {
				if got := n.sm.Count(); got < want {
					t.Errorf("replica %d count = %d, want at least %d", i, got, want)
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
