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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/kvstore"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport/memory"
)

// TestKVStoreDiskSnapshotRecovery (Phase 10(f)): a Store
// configured with SnapshotDir periodically writes its state to
// disk. On restart against the same DataDir + SnapshotDir, the
// Store loads the on-disk snapshot — its in-memory map is the
// pre-crash state BEFORE any peer interaction, BEFORE any
// log replay completes.
//
// This is the catastrophic-crash recovery story: pod loses
// memory but the PVC survives → next pod start sees the same
// state via disk snapshot + comlink log replay.
func TestKVStoreDiskSnapshotRecovery(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	dataDir := t.TempDir()
	snapDir := filepath.Join(dataDir, "kvstore")
	net, err := sched.Connect(&pb.ReplicaID{Value: id16("alice")})
	if err != nil {
		t.Fatal(err)
	}

	mkCluster := func() *comlink.Cluster {
		c, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
			Self:      id16("alice"),
			Members:   []comlink.ReplicaID{id16("alice")},
			DataDir:   dataDir,
			Bootstrap: &comlink.BootstrapConfig{Force: true},
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	// ─── Phase 1: bring up, populate, force a snapshot ───
	cluster := mkCluster()
	convID, _ := comlink.NewConversationID()
	store, err := kvstore.New(ctx, kvstore.Config{
		Cluster:          cluster,
		ConversationID:   convID,
		Members:          []comlink.ReplicaID{id16("alice")},
		SnapshotDir:      snapDir,
		SnapshotInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drive scheduler in the background.
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

	want := map[string]string{}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		want[k] = v
		wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := store.Set(wctx, k, v); err != nil {
			cancel()
			t.Fatalf("Set %d: %v", i, err)
		}
		cancel()
	}

	// Wait until the snapshot loop has written at least one
	// snapshot covering all 20 writes. The loop fires every
	// 50ms and only writes when maxOff has advanced.
	snapPath := filepath.Join(snapDir, "state.snap")
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(snapPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never written to %s", snapPath)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Give the periodic snapshot one more cycle so it captures
	// the LATEST state (not just the first write).
	time.Sleep(150 * time.Millisecond)

	// ─── Phase 2: clean shutdown ───
	if err := store.Close(); err != nil {
		t.Errorf("store Close: %v", err)
	}
	if err := cluster.Close(); err != nil {
		t.Errorf("cluster Close: %v", err)
	}
	close(stop)

	// ─── Phase 3: restart from disk; verify state restored
	// BEFORE any new traffic. Fresh scheduler so the new
	// Connect doesn't collide with alice's still-registered
	// entry on `sched`.
	sched2 := memory.NewScheduler(1)
	defer sched2.Close()
	net2, err := sched2.Connect(&pb.ReplicaID{Value: id16("alice")})
	if err != nil {
		t.Fatal(err)
	}
	cluster2, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   dataDir,
		Transport: comlink.TransportConfig{Network: net2},
	})
	if err != nil {
		t.Fatalf("restart cluster: %v", err)
	}
	defer cluster2.Close()

	store2, err := kvstore.New(ctx, kvstore.Config{
		Cluster:        cluster2,
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		SnapshotDir:    snapDir,
	})
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	defer store2.Close()

	// IMMEDIATELY after Store construction, the Restore from
	// disk has already happened (it's synchronous in
	// NewSubstrate). State should match the pre-crash snapshot.
	got := store2.SnapshotMap()
	if len(got) != len(want) {
		t.Fatalf("post-restart state size = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if g := got[k]; g != v {
			t.Errorf("post-restart [%q] = %q, want %q", k, g, v)
		}
	}
}

// TestKVStoreDiskSnapshotEmptyOnFirstRun: a Store with
// SnapshotDir but no prior snapshot file starts with an
// empty map (no error, no Restore).
func TestKVStoreDiskSnapshotEmptyOnFirstRun(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	net, _ := sched.Connect(&pb.ReplicaID{Value: id16("alice")})
	cluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	convID, _ := comlink.NewConversationID()
	store, err := kvstore.New(ctx, kvstore.Config{
		Cluster:        cluster,
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		SnapshotDir:    t.TempDir(), // exists but empty
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if got := store.SnapshotMap(); len(got) != 0 {
		t.Errorf("expected empty state on first run, got %v", got)
	}
}
