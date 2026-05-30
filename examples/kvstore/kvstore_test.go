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
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport/memory"
)

// ─── test harness ───────────────────────────────────────────────

func id16(tag string) comlink.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return comlink.ReplicaID(b)
}

type node struct {
	cluster *comlink.Cluster
	store   *kvstore.Store
}

// build3 spins up a 3-replica in-memory cluster running a Store
// on a shared conversation. Returns the nodes and a deferred
// stop func that tears the scheduler loop down.
func build3(t *testing.T) (nodes []*node, stop func()) {
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
		store, err := kvstore.New(ctx, kvstore.Config{
			Cluster:        c,
			ConversationID: convID,
			Members:        members,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &node{cluster: c, store: store}
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
			_ = n.store.Close()
			_ = n.cluster.Close()
		}
		_ = sched.Close()
	}
	return nodes, stop
}

// waitConverge polls until every node's Snapshot equals want or
// the deadline expires.
func waitConverge(t *testing.T, nodes []*node, want map[string]string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		ok := true
		for _, n := range nodes {
			if !sameMap(n.store.SnapshotMap(), want) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(end) {
			for i, n := range nodes {
				t.Errorf("replica %d snapshot = %v, want %v", i, n.store.SnapshotMap(), want)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
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

// ─── tests ──────────────────────────────────────────────────────

// TestSetReplicates: a Set on one replica propagates to peers.
func TestSetReplicates(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := nodes[0].store.Set(ctx, "hello", "world"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitConverge(t, nodes, map[string]string{"hello": "world"}, 3*time.Second)
}

// TestDeleteReplicates: Delete propagates and clears state.
func TestDeleteReplicates(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := nodes[0].store.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{"k": "v"}, 3*time.Second)

	if _, err := nodes[1].store.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	waitConverge(t, nodes, map[string]string{}, 3*time.Second)
}

// TestConcurrentSetsConverge: each replica concurrently Sets
// its own key. Every replica should see the same final map.
func TestConcurrentSetsConverge(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *node) {
			defer wg.Done()
			k := fmt.Sprintf("k-%d", idx)
			v := fmt.Sprintf("v-%d", idx)
			if _, err := n.store.Set(ctx, k, v); err != nil {
				t.Errorf("replica %d Set: %v", idx, err)
			}
		}(i, n)
	}
	wg.Wait()

	waitConverge(t, nodes, map[string]string{
		"k-0": "v-0",
		"k-1": "v-1",
		"k-2": "v-2",
	}, 5*time.Second)
}

// TestSameKeyConflictResolvesDeterministically: two replicas
// concurrently Set the same key. Whichever wins the global
// order is the final value on every replica — i.e. the system
// is consistent even when not the "right" value is chosen.
func TestSameKeyConflictResolvesDeterministically(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = nodes[0].store.Set(ctx, "k", "from-alice")
	}()
	go func() {
		defer wg.Done()
		_, _ = nodes[1].store.Set(ctx, "k", "from-bob")
	}()
	wg.Wait()

	// Wait for every replica to have applied both Sets (count == 2).
	// The final value is whichever one was ordered last.
	end := time.Now().Add(5 * time.Second)
	for {
		v0, _ := nodes[0].store.Get("k")
		allMatch := true
		for _, n := range nodes {
			v, ok := n.store.Get("k")
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
				v, _ := n.store.Get("k")
				t.Errorf("replica %d Get(k) = %q", i, v)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWatchFiresOnSetAndDelete: subscribe to a key BEFORE the
// Set, expect EventSet on the channel; then Delete, expect
// EventDelete.
func TestWatchFiresOnSetAndDelete(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, cancelWatch := nodes[2].store.Watch("hello")
	defer cancelWatch()

	if _, err := nodes[0].store.Set(ctx, "hello", "world"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Type != kvstore.EventSet || ev.Key != "hello" || ev.Value != "world" {
			t.Errorf("Set event = %+v, want {EventSet hello world false}", ev)
		}
		if ev.PriorExists {
			t.Errorf("PriorExists = true on first Set; want false")
		}
	case <-ctx.Done():
		t.Fatal("did not receive Set event")
	}

	if _, err := nodes[1].store.Delete(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Type != kvstore.EventDelete || ev.Key != "hello" {
			t.Errorf("Delete event = %+v, want {EventDelete hello}", ev)
		}
	case <-ctx.Done():
		t.Fatal("did not receive Delete event")
	}
}

// TestWatchCancelClosesChannel: cancel returns; subsequent
// mutations on the key do NOT push to the (now-closed)
// channel.
func TestWatchCancelClosesChannel(t *testing.T) {
	nodes, stop := build3(t)
	defer stop()

	ch, cancelWatch := nodes[0].store.Watch("k")
	cancelWatch()

	// Channel must be closed.
	_, open := <-ch
	if open {
		t.Fatal("channel not closed after cancel")
	}

	// Another Set should not panic — i.e. notify is a no-op.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := nodes[1].store.Set(ctx, "k", "v"); err != nil {
		t.Fatalf("Set after Watch cancel: %v", err)
	}
}
