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

package kvstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/kvstore"
)

// TestKVStoreFiveReplicaSurvivesNodeLoss exercises Phase 6's
// failure-injection exit criterion:
//
//  1. 5 replicas come up via sponsor handshake.
//  2. The kvstore Substrate is created on every replica with
//     Members = [r0..r4].
//  3. r0 writes "before" — every survivor sees it.
//  4. r4 is Closed mid-traffic (simulates a process crash).
//  5. The operator (r0) VoteOuts r4 from the SYSTEM cluster.
//     This is what unblocks the application substrate too: psync
//     would otherwise wait for r4's vector-clock slot indefinitely.
//  6. r0 writes "after" — every SURVIVING replica converges.
//
// The system has 5 members initially. Quorum for VoteOut is
// ⌈5/2⌉+1 = 3 Acks (initiator counts implicitly), so we need
// 2 of the 3 other survivors to Ack the eviction — and the
// majority gate ⌊N/2⌋ < |ML| (4 > 2) holds.
//
// Note: app-substrate-level VoteOut is a Phase 7 feature; for
// now the system-conv VoteOut is what lets the app substrate
// make progress.
func TestKVStoreFiveReplicaSurvivesNodeLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("5-replica gRPC test is heavy; skip in -short")
	}

	replicas := []string{"r0", "r1", "r2", "r3", "r4"}
	clusters := startSponsorJoinedCluster(t, replicas)

	expect := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		expect[i] = id16(name)
	}
	allReplicasReady(t, clusters, expect, 10*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Phase 1 — full-cluster write.
	if err := nodes[0].store.Set(ctx, "before", "value-before"); err != nil {
		t.Fatalf("Set 'before' on r0: %v", err)
	}
	waitConvergeStores(t, nodes, map[string]string{
		"before": "value-before",
	}, 10*time.Second)

	// Phase 2 — kill r4. Close the store and cluster directly;
	// the t.Cleanup hook will idempotently no-op for them later.
	t.Log("simulating r4 crash via Close")
	if err := nodes[4].store.Close(); err != nil {
		t.Errorf("close r4 store: %v", err)
	}
	if err := clusters[4].Close(); err != nil {
		t.Errorf("close r4 cluster: %v", err)
	}

	// Wait for the surviving replicas' failure detectors to
	// suspect r4. VoteOut Ack policy: peer Acks iff peer also
	// suspects target — otherwise peer Nacks (split-suspicion
	// gate, PLAN §2.13). FD default SuspicionInterval=2s, so we
	// wait a bit longer to be safe.
	time.Sleep(3 * time.Second)

	// Phase 3 — r0 VoteOuts r4 from the system. Until this lands,
	// the app substrate also can't make total-order progress
	// (r4's slot would block wave completion).
	voteCtx, voteCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := clusters[0].VoteOut(voteCtx, id16("r4")); err != nil {
		voteCancel()
		t.Fatalf("VoteOut r4 from system: %v", err)
	}
	voteCancel()

	// Survivors are r0..r3.
	survivors := nodes[:4]

	// Phase 3b — mirror the eviction on the application substrate.
	// Cluster.VoteOut only affects the system conv; each app
	// substrate's psync still expects r4's slot. FreezeMember on
	// every survivor unblocks total-order wave completion.
	for i, n := range survivors {
		if err := n.store.FreezeMember(id16("r4")); err != nil {
			t.Errorf("survivor %d FreezeMember r4: %v", i, err)
		}
	}

	// Phase 4 — write after the eviction; survivors converge on
	// the full {before, after} map.
	if err := survivors[1].store.Set(ctx, "after", "value-after"); err != nil {
		t.Fatalf("Set 'after' on r1: %v", err)
	}
	waitConvergeStores(t, survivors, map[string]string{
		"before": "value-before",
		"after":  "value-after",
	}, 15*time.Second)
}

// TestKVStoreFiveReplicaConcurrentWrites: 5 replicas, every
// replica writes a distinct key concurrently, all converge.
// Smoke test for cross-cluster gRPC traffic under load (larger
// fan-out than the 3-replica test).
func TestKVStoreFiveReplicaConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("5-replica gRPC test is heavy; skip in -short")
	}

	replicas := []string{"r0", "r1", "r2", "r3", "r4"}
	clusters := startSponsorJoinedCluster(t, replicas)

	expect := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		expect[i] = id16(name)
	}
	allReplicasReady(t, clusters, expect, 10*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		idx int
		err error
	}
	results := make(chan result, len(nodes))
	for i, n := range nodes {
		go func(idx int, n *kvstore.Store) {
			err := n.Set(ctx, fmt.Sprintf("k-%d", idx), fmt.Sprintf("v-%d", idx))
			results <- result{idx: idx, err: err}
		}(i, n.store)
	}
	for i := 0; i < len(nodes); i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("replica %d Set: %v", r.idx, r.err)
		}
	}

	want := make(map[string]string, len(nodes))
	for i := range nodes {
		want[fmt.Sprintf("k-%d", i)] = fmt.Sprintf("v-%d", i)
	}
	waitConvergeStores(t, nodes, want, 15*time.Second)
}
