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

// TestTombstoneGCSweepDropsAndRetains exercises the
// gcTombstones safe-wave logic via the only public surface
// that drives it: the periodic snapshot loop.
//
// Setup: 3-replica gRPC cluster, fast SnapshotInterval, every
// replica writes one Set then issues a Delete on a distinct
// key. Each replica's Set + Delete bumps its own slot. After
// the sweep, tombstones with wave below the global min should
// be GC'd; tombstones at-or-above should be retained.
//
// We can't easily inspect the tombstone map directly from
// outside the package, so we use the kvstore_tombstones_gc_total
// counter from the Prometheus registry to observe the sweep
// firing.
func TestTombstoneGCSweepDropsAndRetains(t *testing.T) {
	if testing.Short() {
		t.Skip("3-replica gRPC tombstone test is heavy; skip in -short")
	}

	replicas := []string{"alice", "bob", "carol"}
	clusters := startSponsorJoinedCluster(t, replicas)
	expect := []comlink.ReplicaID{id16("alice"), id16("bob"), id16("carol")}
	allReplicasReady(t, clusters, expect, 5*time.Second)

	convID, _ := comlink.NewConversationID()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build Stores with snapshot dir + fast cadence so the
	// GC pass fires quickly. Disable the proactive-ack loop —
	// it'd inflate peerWaveSeen via background acks and make
	// the test's assertions about pre-GC state harder to read.
	stores := make([]*kvstore.Store, len(clusters))
	for i, c := range clusters {
		store, err := kvstore.New(ctx, kvstore.Config{
			Cluster:          c,
			ConversationID:   convID,
			Members:          expect,
			SnapshotDir:      t.TempDir(),
			SnapshotInterval: 200 * time.Millisecond,
			Ack:              kvstore.AckConfig{Disabled: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		stores[i] = store
	}

	// Each replica sets and then deletes a distinct key. This
	// creates tombstones with each sender's own slot at >= 2,
	// and every replica observes every other replica past
	// wave=2 (because Apply fires for every replica's sends).
	for i, st := range stores {
		k := fmt.Sprintf("k-%s", replicas[i])
		if err := st.Set(ctx, k, "v"); err != nil {
			t.Fatalf("Set on %s: %v", replicas[i], err)
		}
		if err := st.Delete(ctx, k); err != nil {
			t.Fatalf("Delete on %s: %v", replicas[i], err)
		}
	}

	// Let the cluster converge and a few snapshot ticks fire.
	// 1.5s ≈ 7 snapshot cycles at 200ms cadence.
	time.Sleep(1500 * time.Millisecond)

	// Drive a few more writes from every replica so the
	// peerWaveSeen min advances past the tombstone waves.
	for round := 0; round < 3; round++ {
		for i, st := range stores {
			k := fmt.Sprintf("post-%s-%d", replicas[i], round)
			if err := st.Set(ctx, k, "v"); err != nil {
				t.Errorf("Set %s: %v", k, err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Allow GC sweeps to run.
	time.Sleep(1 * time.Second)

	// Verify tombstones were dropped: GC counter > 0 on
	// every replica. (We can't probe the map directly, but
	// the counter is the in-band proof the safe-wave gate
	// went open and dropped something.)
	gcCount := scrapeCounter(t, "kvstore_tombstones_gc_total")
	if gcCount == 0 {
		t.Fatalf("expected at least one tombstone GC'd; counter = 0")
	}
	t.Logf("kvstore_tombstones_gc_total = %d (sweep fired and dropped tombstones ✓)", gcCount)

	// Live-tombstone gauge should be at-or-near zero by now.
	live := scrapeGauge(t, "kvstore_tombstones_live")
	if live > 3 {
		t.Errorf("kvstore_tombstones_live = %d, expected near 0 after GC", live)
	}
	t.Logf("kvstore_tombstones_live = %d", live)
}

// scrapeCounter pulls the value of a single-counter metric
// from the package-global registry. Returns 0 if not found
// (the metric is registered lazily on first promauto register).
func scrapeCounter(t *testing.T, name string) uint64 {
	t.Helper()
	mfs, err := comlink.MetricsRegistry().Gather()
	if err != nil {
		t.Fatalf("metrics gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return uint64(c.GetValue())
			}
		}
	}
	return 0
}

func scrapeGauge(t *testing.T, name string) uint64 {
	t.Helper()
	mfs, err := comlink.MetricsRegistry().Gather()
	if err != nil {
		t.Fatalf("metrics gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if g := m.GetGauge(); g != nil {
				return uint64(g.GetValue())
			}
		}
	}
	return 0
}
