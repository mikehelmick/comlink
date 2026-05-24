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

package directory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/directory"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport/memory"
)

// ─── harness ────────────────────────────────────────────────────

func id16(tag string) comlink.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return comlink.ReplicaID(b)
}

type node struct {
	cluster *comlink.Cluster
	dir     *directory.Directory
}

// buildClusterWithDirectory: 3 replicas on the in-memory transport
// running a shared Directory.
func buildClusterWithDirectory(t *testing.T) (nodes []*node, stop func()) {
	t.Helper()
	ctx := context.Background()
	sched := memory.NewScheduler(1)

	replicas := []string{"alice", "bob", "carol"}
	members := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = id16(name)
	}
	convID, _ := comlink.NewConversationID()

	nodes = make([]*node, len(replicas))
	for i, name := range replicas {
		net, err := sched.Connect(&pb.ReplicaID{Value: id16(name)})
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
		dir, err := directory.New(ctx, directory.Config{
			Cluster:        c,
			ConversationID: convID,
			Members:        members,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{cluster: c, dir: dir}
	}

	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				sched.RunAll()
			}
		}
	}()
	stop = func() {
		close(stopCh)
		for _, n := range nodes {
			_ = n.dir.Close()
			_ = n.cluster.Close()
		}
		_ = sched.Close()
	}
	return nodes, stop
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

func waitConverge(t *testing.T, nodes []*node, want map[string]string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		ok := true
		for _, n := range nodes {
			if !sameMap(n.dir.Snapshot(), want) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(end) {
			for i, n := range nodes {
				t.Errorf("replica %d snapshot = %v, want %v", i, n.dir.Snapshot(), want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ─── tests ──────────────────────────────────────────────────────

// TestDirectoryInsertReplicates: basic sanity — Insert on one
// replica propagates to peers.
func TestDirectoryInsertReplicates(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := nodes[0].dir.Insert(ctx, "alpha", "1"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"alpha": "1"}, 5*time.Second)
}

// TestDirectoryInsertSemantics: Insert is "create-if-absent".
// Re-Inserting an existing name does NOT overwrite.
func TestDirectoryInsertSemantics(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := nodes[0].dir.Insert(ctx, "k", "first"); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].dir.Insert(ctx, "k", "second"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "first"}, 5*time.Second)
}

// TestDirectoryUpdateSemantics: Update is "overwrite-if-present".
// Update on an absent name is a no-op.
//
// KNOWN BUG (skipped): the originating replica of an Insert
// frequently fails to observe a subsequent Update from a peer
// on the same name. Suspected SemOrder wave-1 completion gap
// where the inserter never sees a wave-≥1 message from the
// third quiet replica because its substrate heartbeat path
// classifies into a class that doesn't advance the wave gate
// as expected. Reproducible at -count=5+. Tracked for Phase 7
// SemOrder hardening; until then, applications that need
// Insert-then-Update semantics across replicas should use
// OrderingTotal instead.
func TestDirectoryUpdateSemantics(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Update on absent — no-op.
	if err := nodes[0].dir.Update(ctx, "ghost", "value"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{}, 5*time.Second)

	// Insert then Update — second value wins. We must wait for
	// the Insert to converge on r1 BEFORE issuing Update from r1
	// — otherwise r1's Update finds "k" absent and no-ops (the
	// directory's "update-if-present" semantics).
	if err := nodes[0].dir.Insert(ctx, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "v1"}, 5*time.Second)
	if err := nodes[1].dir.Update(ctx, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "v2"}, 5*time.Second)
}

// TestDirectoryDelete: Delete clears the entry on every replica.
func TestDirectoryDelete(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := nodes[0].dir.Insert(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "v"}, 5*time.Second)
	if err := nodes[1].dir.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{}, 5*time.Second)
}

// TestDirectoryConcurrentInsertsToDisjointNames is the §3
// commutativity demonstration: each replica concurrently
// Inserts a DIFFERENT name. SemOrder permits these to be
// applied in any order on each replica without needing a
// wave barrier, since the classifier puts disjoint names in
// disjoint commutativity classes.
//
// What we observe externally: all 3 replicas converge on the
// union of all inserts, and the test finishes much faster than
// it would under strict total ordering across the same number
// of writes.
func TestDirectoryConcurrentInsertsToDisjointNames(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *node) {
			defer wg.Done()
			name := fmt.Sprintf("name-%d", idx)
			val := fmt.Sprintf("val-%d", idx)
			if err := n.dir.Insert(ctx, name, val); err != nil {
				t.Errorf("replica %d Insert: %v", idx, err)
			}
		}(i, n)
	}
	wg.Wait()

	want := map[string]string{
		"name-0": "val-0",
		"name-1": "val-1",
		"name-2": "val-2",
	}
	waitConverge(t, nodes, want, 5*time.Second)
}

// TestDirectorySameNameConflictResolvesDeterministically:
// Two replicas concurrently Update("k", X) and Update("k", Y).
// Per the directory's Insert semantics we need to seed "k"
// first. SemOrder bins both Updates into the same class →
// total-orders them → all replicas agree on the same final
// value (whichever update was ordered last in that class).
func TestDirectorySameNameConflictResolvesDeterministically(t *testing.T) {
	nodes, stop := buildClusterWithDirectory(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Seed.
	if err := nodes[0].dir.Insert(ctx, "k", "init"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "init"}, 5*time.Second)

	// Concurrent Updates from two replicas — both target "k".
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = nodes[0].dir.Update(ctx, "k", "from-alice")
	}()
	go func() {
		defer wg.Done()
		_ = nodes[1].dir.Update(ctx, "k", "from-bob")
	}()
	wg.Wait()

	// Whichever Update was ordered last wins. Every replica
	// must agree on the same winner.
	end := time.Now().Add(5 * time.Second)
	for {
		v0, _ := nodes[0].dir.Lookup("k")
		allMatch := true
		for _, n := range nodes {
			v, ok := n.dir.Lookup("k")
			if !ok || v != v0 {
				allMatch = false
				break
			}
		}
		if allMatch && (v0 == "from-alice" || v0 == "from-bob") {
			return
		}
		if time.Now().After(end) {
			for i, n := range nodes {
				v, _ := n.dir.Lookup("k")
				t.Errorf("replica %d Lookup(k) = %q", i, v)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
