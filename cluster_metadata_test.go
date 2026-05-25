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
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/transport/memory"
)

// mustNewSingleNodeCluster spins up a fresh single-replica
// cluster on a private in-memory scheduler. Used by tests that
// only need self-deliver semantics.
func mustNewSingleNodeCluster(t *testing.T, name string) *comlink.Cluster {
	t.Helper()
	sched := memory.NewScheduler(1)
	t.Cleanup(func() { _ = sched.Close() })
	self := id16(name)
	net, err := sched.Connect(pbID(self))
	if err != nil {
		t.Fatal(err)
	}
	c, err := comlink.NewCluster(context.Background(), comlink.ClusterConfig{
		Self:      self,
		Members:   []comlink.ReplicaID{self},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// startTwoNodePeers spins up two clusters with the SAME
// ClusterID sharing one in-memory scheduler, so they form a
// real 2-replica cluster (system conv has both as members).
// Returns (a, b) Clusters. Both are torn down on test cleanup.
func startTwoNodePeers(t *testing.T, clusterID comlink.ClusterID, a, b comlink.ReplicaID) (*comlink.Cluster, *comlink.Cluster) {
	t.Helper()
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	t.Cleanup(func() { _ = sched.Close() })

	netA, err := sched.Connect(pbID(a))
	if err != nil {
		t.Fatal(err)
	}
	netB, err := sched.Connect(pbID(b))
	if err != nil {
		t.Fatal(err)
	}

	mems := []comlink.ReplicaID{a, b}
	ca, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    a,
		Members: mems,
		DataDir: t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{
			Force:     true,
			ClusterID: clusterID,
		},
		Transport: comlink.TransportConfig{Network: netA},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ca.Close() })

	cb, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    b,
		Members: mems,
		DataDir: t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{
			Force:     true,
			ClusterID: clusterID,
		},
		Transport: comlink.TransportConfig{Network: netB},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cb.Close() })

	// Drive the in-memory scheduler in the background.
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
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

	return ca, cb
}

// TestSubmitMetadataSelfDeliver (Phase 11(a)): a single-replica
// cluster submits a metadata message; the same message comes
// back on MetadataMessages.
func TestSubmitMetadataSelfDeliver(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()

	got := cluster.MetadataMessages()

	if err := cluster.SubmitMetadata(ctx, []byte("hello")); err != nil {
		t.Fatalf("SubmitMetadata: %v", err)
	}

	select {
	case m := <-got:
		if string(m.Payload) != "hello" {
			t.Fatalf("Payload = %q, want %q", m.Payload, "hello")
		}
		if !m.From.Equal(id16("alice")) {
			t.Fatalf("From = %s, want alice", m.From)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("metadata message never arrived on self-deliver")
	}
}

// TestSubmitMetadataCrossReplica: a 2-replica cluster, both
// SubmitMetadata; each sees the other's payload on its
// MetadataMessages channel.
func TestSubmitMetadataCrossReplica(t *testing.T) {
	ctx := context.Background()
	alice := id16("alice")
	bob := id16("bob")
	clusterID, _ := comlink.NewClusterID()

	a, b := startTwoNodePeers(t, clusterID, alice, bob)

	aCh := a.MetadataMessages()
	bCh := b.MetadataMessages()

	if err := a.SubmitMetadata(ctx, []byte("from-alice")); err != nil {
		t.Fatalf("alice SubmitMetadata: %v", err)
	}
	if err := b.SubmitMetadata(ctx, []byte("from-bob")); err != nil {
		t.Fatalf("bob SubmitMetadata: %v", err)
	}

	// Each replica must see BOTH messages (self-deliver +
	// peer-deliver). Drain up to 4 messages with a deadline.
	collect := func(ch <-chan comlink.MetadataMessage) map[string]bool {
		seen := map[string]bool{}
		deadline := time.After(5 * time.Second)
		for len(seen) < 2 {
			select {
			case m := <-ch:
				seen[string(m.Payload)] = true
			case <-deadline:
				return seen
			}
		}
		return seen
	}
	wantBoth := func(seen map[string]bool, who string) {
		if !seen["from-alice"] || !seen["from-bob"] {
			t.Errorf("%s saw %v, want both from-alice and from-bob", who, seen)
		}
	}
	wantBoth(collect(aCh), "alice")
	wantBoth(collect(bCh), "bob")
}

// TestSubmitMetadataEmptyPayloadRejected: empty payload returns
// an error rather than silently sending a 0-byte message.
func TestSubmitMetadataEmptyPayloadRejected(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()
	if err := cluster.SubmitMetadata(ctx, nil); err == nil {
		t.Fatal("SubmitMetadata with empty payload: want error, got nil")
	}
}
