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

// Package trim implements the high-water-mark trim coordination
// from PLAN §2.8 (Phase 4).
//
// Each replica periodically advertises a "watermark" — the lowest
// log offset it still needs for its own recovery — by multicasting
// a Watermark frame into the conversation. Receivers update their
// local view of every peer's latest watermark. The group-wide
// safe-trim frontier is min(watermark[r]) over r in the active
// membership list. Trimming the local log up to this frontier is
// safe because every member has signalled they no longer need
// anything below it.
//
// PLAN §2.8 safety guarantee: never trim past the minimum of
// watermarks from every ACTIVE member — including soft-suspected
// replicas that may recover. Voted-out replicas are explicitly
// excluded (their watermark is dropped from the min computation).
//
// This package is pure data: it has no I/O, no goroutines, no
// timers. Wiring (broadcast cadence, Truncate triggering) lives
// in membership.Manager (Phase 4(c)).
package trim

import (
	"bytes"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
)

// Mark is one replica's watermark contribution: the applied
// frontier (highest offset whose message has been applied locally)
// and the snapshot frontier (highest offset durably covered by a
// local snapshot, or 0 if the replica has no snapshot).
//
// For trim safety the per-replica usable frontier is min(applied,
// snapshot). A replica with snapshot=0 always pins the cluster
// frontier to 0 — appropriate when no snapshot exists, since a
// joiner couldn't recover trimmed history any other way.
//
// For the system conv (which doesn't snapshot today), callers
// use UpdateApplied with both fields equal — the snapshot field
// gets the applied value too, so the system conv's trim
// continues to work the way it did before Phase 10(e).
type Mark struct {
	Applied  clog.Offset
	Snapshot clog.Offset
}

// usableFrontier is the per-replica trim contribution: the lower
// of the applied and snapshot watermarks. A snapshot of 0 means
// "no snapshot", which we treat as zero contribution (pinning
// the cluster frontier to 0).
func (m Mark) usableFrontier() clog.Offset {
	if m.Snapshot == 0 {
		return 0
	}
	if m.Applied < m.Snapshot {
		return m.Applied
	}
	return m.Snapshot
}

// Tracker holds the latest watermark observed from each member.
// Concurrent access is safe.
type Tracker struct {
	mu    sync.Mutex
	marks map[string]Mark
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{marks: make(map[string]Mark)}
}

// Update records a peer's latest watermark contribution. Both
// fields are monotonic — a retreat in either is silently
// ignored.
//
// Returns true if EITHER field advanced.
func (t *Tracker) Update(replica *pb.ReplicaID, mark Mark) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(replica.GetValue())
	cur := t.marks[key] // zero value if absent
	advanced := false
	if mark.Applied > cur.Applied {
		cur.Applied = mark.Applied
		advanced = true
	}
	if mark.Snapshot > cur.Snapshot {
		cur.Snapshot = mark.Snapshot
		advanced = true
	}
	if !advanced {
		return false
	}
	t.marks[key] = cur
	return true
}

// UpdateApplied is a backwards-compat helper for callers that
// don't snapshot (or that conflate applied + snapshot, like the
// system conv's Manager). Sets both Applied and Snapshot to
// `offset`, so trim treats this replica's watermark as fully
// usable for trim safety.
func (t *Tracker) UpdateApplied(replica *pb.ReplicaID, offset clog.Offset) bool {
	return t.Update(replica, Mark{Applied: offset, Snapshot: offset})
}

// Get returns replica's latest watermark and whether it has been
// observed.
func (t *Tracker) Get(replica *pb.ReplicaID) (Mark, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.marks[string(replica.GetValue())]
	return m, ok
}

// Forget drops replica from the tracker — used when a replica is
// voted out. Subsequent SafeFrontier computations no longer wait
// on this replica.
func (t *Tracker) Forget(replica *pb.ReplicaID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.marks, string(replica.GetValue()))
}

// SafeFrontier returns the safe-trim offset given the current
// active membership list: min over r in active of
// usableFrontier(mark_r). If any active replica has not yet
// advertised a watermark, the frontier is 0 (paper §5.3 safety
// condition).
//
// Returns (frontier, ok). ok=false if no active members or any
// active member is missing a watermark.
func (t *Tracker) SafeFrontier(active []*pb.ReplicaID) (clog.Offset, bool) {
	if len(active) == 0 {
		return 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var min clog.Offset
	first := true
	for _, r := range active {
		m, ok := t.marks[string(r.GetValue())]
		if !ok {
			return 0, false
		}
		f := m.usableFrontier()
		if first || f < min {
			min = f
			first = false
		}
	}
	return min, true
}

// Snapshot returns a copy of every (replica, mark) pair in the
// tracker. Useful for tests and debugging.
func (t *Tracker) Snapshot() map[string]Mark {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]Mark, len(t.marks))
	for k, v := range t.marks {
		out[k] = v
	}
	return out
}

// _ keeps bytes referenced; future commits that compare ReplicaID
// byte-wise here will use it.
var _ = bytes.Compare
