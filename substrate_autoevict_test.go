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

// TestSubstrateAutoEvictUnblocksWavesAfterPeerDeath (Phase 10(a)):
// 3-replica Substrate with AutoEvict configured. After all three
// are up and converged, "kill" one (Close its substrate) and
// verify the survivors can still Submit and converge — the
// substrate's failure detector auto-freezes the dead peer's
// slot so the OrderingTotal wave gate doesn't stall on it.
//
// Without AutoEvict (the historical default), Submit on the
// survivors would block forever waiting for the dead peer to
// advance its wave.
func TestSubstrateAutoEvictUnblocksWavesAfterPeerDeath(t *testing.T) {
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
		evicted []comlink.ReplicaID
		mu      sync.Mutex
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
			Members:   []comlink.ReplicaID{id16(name)},
			DataDir:   t.TempDir(),
			Bootstrap: &comlink.BootstrapConfig{Force: true},
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		n := &node{cluster: c, sm: &counterSM{}}
		nodes[i] = n
		// Short SuspicionInterval so the test doesn't take forever.
		sub, err := c.NewSubstrate(ctx, comlink.SubstrateConfig{
			ConversationID: convID,
			Members:        members,
			Ordering:       comlink.OrderingTotal,
			StateMachine:   n.sm,
			AutoEvict: &comlink.AutoEvictConfig{
				QuietInterval:     100 * time.Millisecond,
				SuspicionInterval: 1500 * time.Millisecond,
				OnEvict: func(peer comlink.ReplicaID, _ []comlink.ReplicaID) {
					n.mu.Lock()
					n.evicted = append(n.evicted, peer)
					n.mu.Unlock()
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		n.sub = sub
	}

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

	// Give heartbeats time to establish liveness before any
	// of the SuspicionInterval timers could fire from initial
	// startup latency in the in-memory scheduler.
	time.Sleep(500 * time.Millisecond)

	// Phase 1: all 3 up — submit one round so we know wave
	// completion works end-to-end.
	for i, n := range nodes {
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		if _, err := n.sub.Submit(ctx2, []byte{byte(i), 'a'}); err != nil {
			cancel()
			t.Fatalf("phase-1 Submit on replica %d: %v", i, err)
		}
		cancel()
	}

	// Phase 2: kill carol (last replica). Survivors are alice (0)
	// and bob (1). Without AutoEvict, their next Submit would
	// hang forever — wave needs all 3 to advance.
	if err := nodes[2].sub.Close(); err != nil {
		t.Errorf("close carol substrate: %v", err)
	}

	// Wait for both survivors' FDs to fire OnEvict for carol.
	// SuspicionInterval=1.5s + a generous slack.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for _, n := range nodes[:2] {
			n.mu.Lock()
			carolEvicted := false
			for _, e := range n.evicted {
				if e.Equal(id16("carol")) {
					carolEvicted = true
					break
				}
			}
			n.mu.Unlock()
			if !carolEvicted {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			for i, n := range nodes[:2] {
				n.mu.Lock()
				t.Errorf("replica %d never auto-evicted (evicted set: %v)", i, n.evicted)
				n.mu.Unlock()
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Phase 3: survivors Submit. Should NOT block now that
	// carol's slot is frozen.
	for i := 0; i < 2; i++ {
		ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
		if _, err := nodes[i].sub.Submit(ctx2, []byte{byte(i), 'b'}); err != nil {
			cancel()
			t.Fatalf("phase-3 Submit on survivor %d after auto-evict: %v", i, err)
		}
		cancel()
	}

	// Sanity: alice's SM has applied phase-1 (alice + bob + carol)
	// and phase-3 (alice + bob) — total 5 applies eventually.
	end := time.Now().Add(3 * time.Second)
	for {
		if nodes[0].sm.Count() >= 5 && nodes[1].sm.Count() >= 5 {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("survivors didn't converge: alice=%d bob=%d", nodes[0].sm.Count(), nodes[1].sm.Count())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
