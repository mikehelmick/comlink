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

// Tracker holds the latest watermark observed from each member.
// Concurrent access is safe.
type Tracker struct {
	mu    sync.Mutex
	marks map[string]clog.Offset // string(replicaID.value) -> offset
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{marks: make(map[string]clog.Offset)}
}

// Update records replica's latest watermark. If a previous value
// exists, the new value is taken iff offset > existing (watermarks
// only ever advance — a replica that retreats its watermark is a
// protocol violation we silently ignore).
//
// Returns true if the stored value advanced as a result.
func (t *Tracker) Update(replica *pb.ReplicaID, offset clog.Offset) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(replica.GetValue())
	if existing, ok := t.marks[key]; ok && offset <= existing {
		return false
	}
	t.marks[key] = offset
	return true
}

// Get returns replica's latest known watermark and whether it has
// been observed.
func (t *Tracker) Get(replica *pb.ReplicaID) (clog.Offset, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	off, ok := t.marks[string(replica.GetValue())]
	return off, ok
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
// active membership list: min(watermark[r]) over r in active.
// If any active replica has not yet advertised a watermark, the
// frontier is 0 (nothing safe to trim — paper §5.3 safety
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
		off, ok := t.marks[string(r.GetValue())]
		if !ok {
			return 0, false
		}
		if first || off < min {
			min = off
			first = false
		}
	}
	return min, true
}

// Snapshot returns a copy of every (replica, offset) pair in the
// tracker. Useful for tests and debugging.
func (t *Tracker) Snapshot() map[string]clog.Offset {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]clog.Offset, len(t.marks))
	for k, v := range t.marks {
		out[k] = v
	}
	return out
}

// _ keeps bytes referenced; future commits that compare ReplicaID
// byte-wise here will use it.
var _ = bytes.Compare
