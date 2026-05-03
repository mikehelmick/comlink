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

func TestUpdateAndGet(t *testing.T) {
	tr := trim.New()
	if _, ok := tr.Get(r("alice")); ok {
		t.Fatal("Get on empty tracker returned ok=true")
	}
	if !tr.Update(r("alice"), 10) {
		t.Fatal("first Update returned false")
	}
	got, ok := tr.Get(r("alice"))
	if !ok || got != 10 {
		t.Fatalf("Get(alice) = (%d, %v), want (10, true)", got, ok)
	}
}

func TestUpdateOnlyAdvances(t *testing.T) {
	tr := trim.New()
	tr.Update(r("alice"), 10)
	if tr.Update(r("alice"), 5) {
		t.Fatal("retreating Update returned true")
	}
	if got, _ := tr.Get(r("alice")); got != 10 {
		t.Fatalf("Get(alice) = %d, want 10 (retreat ignored)", got)
	}
	if !tr.Update(r("alice"), 15) {
		t.Fatal("advancing Update returned false")
	}
	if got, _ := tr.Get(r("alice")); got != 15 {
		t.Fatalf("Get(alice) = %d, want 15", got)
	}
}

func TestSafeFrontierIsMinOverActive(t *testing.T) {
	tr := trim.New()
	tr.Update(r("alice"), 100)
	tr.Update(r("bob"), 50)
	tr.Update(r("carol"), 75)

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if !ok {
		t.Fatal("SafeFrontier ok=false with all members watermarked")
	}
	if got != 50 {
		t.Fatalf("SafeFrontier = %d, want 50 (min)", got)
	}
}

func TestSafeFrontierUnsetReplicaPins(t *testing.T) {
	// PLAN §5.3 safety: if any active member has no watermark
	// yet, the frontier must be 0 (don't trim anything).
	tr := trim.New()
	tr.Update(r("alice"), 100)
	tr.Update(r("bob"), 50)

	got, ok := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if ok {
		t.Fatalf("SafeFrontier returned ok=true with carol unwatermarked; got=%d", got)
	}
	if got != 0 {
		t.Fatalf("SafeFrontier = %d, want 0 when any member is unwatermarked", got)
	}
}

func TestForgetDropsFromMin(t *testing.T) {
	// Voted-out replica's stale watermark should not pin trim.
	tr := trim.New()
	tr.Update(r("alice"), 100)
	tr.Update(r("bob"), 50)   // bob about to be voted out
	tr.Update(r("carol"), 75)

	// Pre-Forget: min is 50 (bob).
	got, _ := tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob"), r("carol")})
	if got != 50 {
		t.Fatalf("pre-Forget SafeFrontier = %d, want 50", got)
	}

	// Vote bob out.
	tr.Forget(r("bob"))

	// Post-Forget with bob excluded from active list: min is 75
	// (carol).
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
	tr.Update(r("alice"), 100)
	if _, ok := tr.SafeFrontier(nil); ok {
		t.Fatal("SafeFrontier on empty active returned ok=true")
	}
}

func TestSnapshot(t *testing.T) {
	tr := trim.New()
	tr.Update(r("alice"), 10)
	tr.Update(r("bob"), 20)
	snap := tr.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot len = %d, want 2", len(snap))
	}
	// Mutating the snapshot should not affect the tracker.
	snap[string(r("alice").GetValue())] = 999
	if got, _ := tr.Get(r("alice")); got != 10 {
		t.Fatalf("Get(alice) after mutating snapshot = %d, want 10", got)
	}
}

// Concurrent Update + SafeFrontier must not race.
func TestConcurrentUpdateAndRead(t *testing.T) {
	tr := trim.New()
	tr.Update(r("alice"), 1)
	tr.Update(r("bob"), 1)
	done := make(chan struct{})
	go func() {
		for i := uint64(2); i < 1000; i++ {
			tr.Update(r("alice"), clog.Offset(i))
		}
		close(done)
	}()
	for range 1000 {
		_, _ = tr.SafeFrontier([]*pb.ReplicaID{r("alice"), r("bob")})
	}
	<-done
}
