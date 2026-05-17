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
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/kvstore"
)

// ─── gRPC harness ───────────────────────────────────────────────

// grpcNode wraps a Cluster + Store talking over a real gRPC
// listener (not the in-memory transport).
type grpcNode struct {
	cluster *comlink.Cluster
	store   *kvstore.Store
}

// startSponsorJoinedCluster brings up an n-replica cluster over
// gRPC using the sponsor handshake (PLAN §5(h)):
//
//   - replica[0] founds (Bootstrap.Force=true), no sponsors.
//   - replica[i>0] joins via sponsor = replica[0]'s gRPC addr.
//     The Join RPC runs VoteIn on the founder side; the joiner
//     installs the resulting ClusterID + ML.
//
// Returns the founder and joiner Clusters (not Stores — the
// caller decides when to create app substrates).
//
// Each Cluster gets its own DataDir; the cluster ID is shared
// via the handshake, not via shared state.
func startSponsorJoinedCluster(t *testing.T, replicaNames []string) []*comlink.Cluster {
	t.Helper()
	if len(replicaNames) < 1 {
		t.Fatal("need at least 1 replica")
	}
	ctx := context.Background()
	clusters := make([]*comlink.Cluster, len(replicaNames))

	// Founder.
	founder := id16(replicaNames[0])
	c0, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      founder,
		Members:   []comlink.ReplicaID{founder},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatalf("founder %s: %v", replicaNames[0], err)
	}
	clusters[0] = c0
	founderAddr := c0.ListenAddr()

	// Joiners.
	for i := 1; i < len(replicaNames); i++ {
		name := replicaNames[i]
		joinCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		ci, err := comlink.NewCluster(joinCtx, comlink.ClusterConfig{
			Self:    id16(name),
			DataDir: t.TempDir(),
			Transport: comlink.TransportConfig{
				Listen: "127.0.0.1:0",
				Sponsors: []comlink.Sponsor{
					{ID: founder, Addr: founderAddr},
				},
			},
		})
		cancel()
		if err != nil {
			// Clean up already-started clusters before bailing.
			for j := 0; j < i; j++ {
				_ = clusters[j].Close()
			}
			t.Fatalf("joiner %s: %v", name, err)
		}
		clusters[i] = ci
	}

	t.Cleanup(func() {
		for _, c := range clusters {
			if c != nil {
				_ = c.Close()
			}
		}
	})
	return clusters
}

// allReplicasReady polls until every cluster's Members() view
// contains every expected replica. Sponsor handshake + MemberAdd
// propagation is async; this just gives them time to settle.
func allReplicasReady(t *testing.T, clusters []*comlink.Cluster, expect []comlink.ReplicaID, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		ok := true
		for _, c := range clusters {
			got := c.Members()
			if len(got) < len(expect) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(end) {
			for i, c := range clusters {
				t.Errorf("cluster %d Members = %v, want at least %d", i, c.Members(), len(expect))
			}
			t.Fatal("clusters did not converge on shared membership")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startStoresOnClusters creates a Store on each Cluster against
// the supplied ConversationID + Members. All Stores share the
// same conv ID, so they form one replicated kvstore instance.
func startStoresOnClusters(t *testing.T, clusters []*comlink.Cluster, convID comlink.ConversationID, members []comlink.ReplicaID) []*grpcNode {
	t.Helper()
	ctx := context.Background()
	nodes := make([]*grpcNode, len(clusters))
	for i, c := range clusters {
		store, err := kvstore.New(ctx, kvstore.Config{
			Cluster:        c,
			ConversationID: convID,
			Members:        members,
		})
		if err != nil {
			t.Fatalf("Store on cluster %d: %v", i, err)
		}
		nodes[i] = &grpcNode{cluster: c, store: store}
		t.Cleanup(func() { _ = store.Close() })
	}
	return nodes
}

// waitConvergeStores is the gRPC equivalent of the in-memory
// waitConverge helper.
func waitConvergeStores(t *testing.T, nodes []*grpcNode, want map[string]string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		ok := true
		for _, n := range nodes {
			if !sameMap(n.store.Snapshot(), want) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(end) {
			for i, n := range nodes {
				t.Errorf("replica %d snapshot = %v, want %v", i, n.store.Snapshot(), want)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ─── tests ──────────────────────────────────────────────────────

// TestKVStoreThreeReplicaGRPC: founder + 2 sponsor-joined
// replicas over real gRPC. Each replica writes one key; all
// converge.
func TestKVStoreThreeReplicaGRPC(t *testing.T) {
	replicas := []string{"alice", "bob", "carol"}
	clusters := startSponsorJoinedCluster(t, replicas)

	expect := []comlink.ReplicaID{id16("alice"), id16("bob"), id16("carol")}
	allReplicasReady(t, clusters, expect, 5*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *grpcNode) {
			defer wg.Done()
			k := fmt.Sprintf("k-%d", idx)
			v := fmt.Sprintf("v-%d", idx)
			if err := n.store.Set(ctx, k, v); err != nil {
				t.Errorf("replica %d Set: %v", idx, err)
			}
		}(i, n)
	}
	wg.Wait()

	waitConvergeStores(t, nodes, map[string]string{
		"k-0": "v-0",
		"k-1": "v-1",
		"k-2": "v-2",
	}, 10*time.Second)
}

// TestKVStoreGRPCWatchAcrossReplicas: a Watch on replica 2 must
// receive an event when replica 0 does a Set — proves end-to-end
// across the gRPC wire.
func TestKVStoreGRPCWatchAcrossReplicas(t *testing.T) {
	replicas := []string{"alice", "bob", "carol"}
	clusters := startSponsorJoinedCluster(t, replicas)
	expect := []comlink.ReplicaID{id16("alice"), id16("bob"), id16("carol")}
	allReplicasReady(t, clusters, expect, 5*time.Second)

	convID, _ := comlink.NewConversationID()
	nodes := startStoresOnClusters(t, clusters, convID, expect)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ch, cancelWatch := nodes[2].store.Watch("watched")
	defer cancelWatch()

	if err := nodes[0].store.Set(ctx, "watched", "by-alice"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Type != kvstore.EventSet || ev.Value != "by-alice" {
			t.Errorf("event = %+v, want EventSet by-alice", ev)
		}
	case <-ctx.Done():
		t.Fatal("Watch on replica 2 did not receive Set from replica 0 within timeout")
	}
}
