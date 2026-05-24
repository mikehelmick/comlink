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
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/kvstore"
)

// TestKVStoreReplicaRestartRecoversSMState (Phase 7(c)): a
// restarted replica's StateMachine recovers its pre-crash state
// from the local log via Substrate.NewSubstrate's auto-replay
// (Phase 7(b)).
//
// Flow:
//   1. 3 replicas via sponsor handshake.
//   2. Each writes one key. All converge.
//   3. r2 closed (clean crash).
//   4. r2 reopened on same DataDir, kvstore re-created.
//   5. WITHOUT any new writes from peers, r2's local Get for
//      each pre-crash key returns the recovered value.
func TestKVStoreReplicaRestartRecoversSMState(t *testing.T) {
	if testing.Short() {
		t.Skip("gRPC restart test is heavy; skip in -short")
	}
	ctx := context.Background()

	replicas := []string{"r0", "r1", "r2"}
	clusters := startSponsorJoinedCluster(t, replicas)
	expect := []comlink.ReplicaID{id16("r0"), id16("r1"), id16("r2")}
	allReplicasReady(t, clusters, expect, 5*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	// Phase 1 — three writes, one per replica.
	want := map[string]string{
		"from-r0": "alpha",
		"from-r1": "beta",
		"from-r2": "gamma",
	}
	for i, n := range nodes {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		key := []string{"from-r0", "from-r1", "from-r2"}[i]
		val := want[key]
		if err := n.store.Set(wctx, key, val); err != nil {
			cancel()
			t.Fatalf("pre-crash Set on r%d: %v", i, err)
		}
		cancel()
	}
	waitConvergeStores(t, nodes, want, 10*time.Second)

	r2DataDir := clusters[2].DataDir()
	r2Self := clusters[2].Self()

	// Phase 2 — crash r2.
	t.Log("crashing r2 after full converge")
	if err := nodes[2].store.Close(); err != nil {
		t.Errorf("r2 store Close: %v", err)
	}
	if err := clusters[2].Close(); err != nil {
		t.Errorf("r2 cluster Close: %v", err)
	}

	// Phase 3 — restart r2 from disk.
	t.Log("restarting r2 from persisted state")
	restartedR2, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    r2Self,
		DataDir: r2DataDir,
		Transport: comlink.TransportConfig{
			Listen: "127.0.0.1:0",
		},
	})
	if err != nil {
		t.Fatalf("restart r2: %v", err)
	}
	defer restartedR2.Close()

	restartedStore, err := kvstore.New(ctx, kvstore.Config{
		Cluster:        restartedR2,
		ConversationID: convID,
		Members:        expect,
	})
	if err != nil {
		t.Fatalf("create restarted r2 store: %v", err)
	}
	defer restartedStore.Close()

	// Phase 4 — local Get on the restarted r2 returns the
	// pre-crash values WITHOUT any peer interaction. This is the
	// 7(b) auto-replay-from-log guarantee.
	//
	// The replay happens inline in NewSubstrate, but the resulting
	// SM.Apply calls run async through Order's wave gates. Poll
	// briefly to allow the recovery to settle.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for k, v := range want {
			got, present := restartedStore.Get(k)
			if !present || got != v {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			snap := restartedStore.Snapshot()
			t.Fatalf("r2 SM did not recover from log: snapshot=%v want=%v", snap, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestKVStoreReplicaRestartRejoinsCluster: demonstrates that a
// replica's cluster-level identity survives a clean process
// restart (same DataDir). Specifically:
//
//   - 3 replicas via sponsor handshake.
//   - Each writes one key.
//   - r2 is Closed.
//   - r2 is reopened on the SAME DataDir (no Force — relies on
//     persisted ClusterID + ML).
//   - r2's ClusterID matches the original; r2 is still in the
//     persisted ML alongside r0 and r1.
//   - r0 issues a NEW write; r2 (with a fresh in-memory SM)
//     observes it via the substrate.
//
// What this test does NOT yet verify: that r2 recovers its
// PRE-CRASH SM state (the "before" writes from r0/r1). The
// kvstore SM is in-memory only; rebuilding from the local log
// requires a Phase 7 checkpoint/replay mechanism. The test
// asserts only the "cluster tolerates restart and continues
// processing new writes" subset of the PLAN exit criterion.
func TestKVStoreReplicaRestartRejoinsCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("gRPC restart test is heavy; skip in -short")
	}
	ctx := context.Background()

	// Bring up r0, r1, r2 via sponsor handshake.
	replicas := []string{"r0", "r1", "r2"}
	clusters := startSponsorJoinedCluster(t, replicas)
	expect := []comlink.ReplicaID{id16("r0"), id16("r1"), id16("r2")}
	allReplicasReady(t, clusters, expect, 5*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	// Capture metadata we'll need after restart.
	originalClusterID := clusters[2].ClusterID()
	r2DataDir := clusters[2].DataDir()
	r2Self := clusters[2].Self()

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := nodes[0].store.Set(wctx, "from-r0", "pre"); err != nil {
		t.Fatalf("pre-crash Set: %v", err)
	}
	cancel()
	waitConvergeStores(t, nodes, map[string]string{
		"from-r0": "pre",
	}, 5*time.Second)

	// Crash r2.
	t.Log("crashing r2")
	if err := nodes[2].store.Close(); err != nil {
		t.Errorf("r2 store Close: %v", err)
	}
	if err := clusters[2].Close(); err != nil {
		t.Errorf("r2 cluster Close: %v", err)
	}

	// Restart r2: same DataDir, no Force. Loads persisted
	// ClusterID + ML from disk and rejoins. Skip the sponsor
	// handshake path since we already have persisted state.
	t.Log("restarting r2 from persisted state")
	restartedR2, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    r2Self,
		DataDir: r2DataDir,
		Transport: comlink.TransportConfig{
			Listen: "127.0.0.1:0",
		},
	})
	if err != nil {
		t.Fatalf("restart r2: %v", err)
	}
	defer restartedR2.Close()

	if !restartedR2.ClusterID().Equal(originalClusterID) {
		t.Fatalf("restarted r2 ClusterID = %s, want original %s",
			restartedR2.ClusterID(), originalClusterID)
	}
	// r2's persisted ML should still include r0, r1, r2.
	got := restartedR2.Members()
	for _, want := range expect {
		found := false
		for _, m := range got {
			if m.Equal(want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("restarted r2 Members = %v, missing %s", got, want)
		}
	}

	// r2's surviving peers (r0, r1) still hold the OLD routing
	// for r2 (pointing at the dead listener). Update them with
	// the new addr so the cluster can reach the restarted r2.
	newR2Addr := restartedR2.ListenAddr()
	for i := 0; i < 2; i++ {
		if err := clusters[i].UpdatePeerAddr(r2Self, newR2Addr); err != nil {
			t.Fatalf("update peer addr for r2 on cluster %d: %v", i, err)
		}
	}

	// Rebuild r2's Store on the restarted Cluster. New in-memory
	// SM, so "from-r0":"pre" is absent locally — but a NEW write
	// from r0 must propagate to the restarted r2.
	restartedStore, err := kvstore.New(ctx, kvstore.Config{
		Cluster:        restartedR2,
		ConversationID: convID,
		Members:        expect,
	})
	if err != nil {
		t.Fatalf("create restarted r2 store: %v", err)
	}
	defer restartedStore.Close()

	// r0 writes a NEW key. The restarted r2 must observe it.
	postCtx, postCancel := context.WithTimeout(ctx, 15*time.Second)
	defer postCancel()
	if err := nodes[0].store.Set(postCtx, "from-r0", "post"); err != nil {
		t.Fatalf("post-restart Set: %v", err)
	}

	// Poll r2 until it sees the new value.
	deadline := time.Now().Add(10 * time.Second)
	for {
		v, ok := restartedStore.Get("from-r0")
		if ok && v == "post" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted r2 did not observe r0's post-restart Set (got %q,%v)", v, ok)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
