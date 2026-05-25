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

package trim_test

import (
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/trim"
)

func r(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

func mark(applied, snap uint64) trim.Mark {
	return trim.Mark{Applied: clog.Offset(applied), Snapshot: clog.Offset(snap)}
}

func TestUpdateAndGet(t *testing.T) {
	tr := trim.New()
	if _, ok := tr.Get(r("alice")); ok {
		t.Fatal("Get on empty tracker returned ok=true")
	}
	if !tr.UpdateApplied(r("alice"), 10) {
		t.Fatal("first Update returned false")
	}
	got, ok := tr.Get(r("alice"))
	if !ok || got.Applied != 10 || got.Snapshot != 10 {
		t.Fatalf("Get(alice) = (%+v, %v), want ({10,10}, true)", got, ok)
	}
}

func TestUpdateOnlyAdvances(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 10)
	if tr.UpdateApplied(r("alice"), 5) {
		t.Fatal("retreating Update returned true")
	}
	if got, _ := tr.Get(r("alice")); got.Applied != 10 {
		t.Fatalf("Get(alice).Applied = %d, want 10 (retreat ignored)", got.Applied)
	}
	if !tr.UpdateApplied(r("alice"), 15) {
		t.Fatal("advancing Update returned false")
	}
	if got, _ := tr.Get(r("alice")); got.Applied != 15 {
		t.Fatalf("Get(alice).Applied = %d, want 15", got.Applied)
	}
}

func TestSafeFrontierIsMinOverActive(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 100)
	tr.UpdateApplied(r("bob"), 50)
	tr.UpdateApplied(r("carol"), 75)

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if !ok {
		t.Fatal("SafeFrontier ok=false with all members watermarked")
	}
	if got != 50 {
		t.Fatalf("SafeFrontier = %d, want 50 (min)", got)
	}
}

func TestSafeFrontierUnsetReplicaPins(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 100)
	tr.UpdateApplied(r("bob"), 50)

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if ok {
		t.Fatalf("SafeFrontier returned ok=true with carol unwatermarked; got=%d", got)
	}
	if got != 0 {
		t.Fatalf("SafeFrontier = %d, want 0 when any member is unwatermarked", got)
	}
}

func TestForgetDropsFromMin(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 100)
	tr.UpdateApplied(r("bob"), 50)
	tr.UpdateApplied(r("carol"), 75)

	got, _ := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if got != 50 {
		t.Fatalf("pre-Forget SafeFrontier = %d, want 50", got)
	}

	tr.Forget(r("bob"))

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("carol")})
	if !ok {
		t.Fatal("post-Forget SafeFrontier ok=false")
	}
	if got != 75 {
		t.Fatalf("post-Forget SafeFrontier = %d, want 75 (carol)", got)
	}
}

func TestSafeFrontierEmptyActive(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 100)
	if _, ok := tr.SafeFrontier(nil); ok {
		t.Fatal("SafeFrontier on empty active returned ok=true")
	}
}

func TestSnapshot(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 10)
	tr.UpdateApplied(r("bob"), 20)
	snap := tr.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}
	// Mutating the snapshot should not affect the tracker.
	snap[string(r("alice").GetValue())] = mark(999, 999)
	if got, _ := tr.Get(r("alice")); got.Applied != 10 {
		t.Fatalf("Get(alice).Applied after mutating snapshot = %d, want 10", got.Applied)
	}
}

func TestConcurrentUpdateAndRead(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 1)
	tr.UpdateApplied(r("bob"), 1)
	done := make(chan struct{})
	go func() {
		for i := uint64(2); i < 1000; i++ {
			tr.UpdateApplied(r("alice"), clog.Offset(i))
		}
		close(done)
	}()
	for range 1000 {
		_, _ = tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob")})
	}
	<-done
}

// TestSafeFrontierWithSnapshotZero (Phase 10(e)): a replica with
// snapshot=0 pins the cluster frontier to 0, regardless of how
// far it's applied. This is correct — without a snapshot, a
// joiner couldn't recover any trimmed history.
func TestSafeFrontierWithSnapshotZero(t *testing.T) {
	tr := trim.New()
	tr.UpdateApplied(r("alice"), 100)             // applied=snap=100
	tr.Update(r("bob"), mark(100, 0))             // applied=100, no snapshot
	tr.UpdateApplied(r("carol"), 100)

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if !ok {
		t.Fatal("SafeFrontier ok=false")
	}
	if got != 0 {
		t.Fatalf("SafeFrontier = %d, want 0 (bob has no snapshot)", got)
	}
}

// TestSafeFrontierMinOverPerReplicaMins (Phase 10(e)): each
// replica's contribution is min(applied, snapshot); the cluster
// frontier is min over replicas.
func TestSafeFrontierMinOverPerReplicaMins(t *testing.T) {
	tr := trim.New()
	tr.Update(r("alice"), mark(100, 80))   // contributes 80
	tr.Update(r("bob"), mark(90, 95))      // contributes 90
	tr.Update(r("carol"), mark(120, 110))  // contributes 110

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if !ok {
		t.Fatal("SafeFrontier ok=false")
	}
	if got != 80 {
		t.Fatalf("SafeFrontier = %d, want 80 (min over per-replica mins)", got)
	}
}
