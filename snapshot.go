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

package comlink

import "sync/atomic"

// Snapshotter is the OPTIONAL capability a StateMachine can
// implement to participate in comlink's snapshot protocol
// (PLAN §10).
//
// Apps that implement Snapshotter opt into:
//
//   - Joiner bootstrap via snapshot: a new replica added via
//     VoteIn after the cluster has trimmed messages can still
//     catch up — its sponsor captures a snapshot, the joiner
//     installs it via Restore, then resumes the lost-message
//     protocol only from the snapshot's throughOffset forward.
//
//   - Trim safety: the log's safe-trim frontier advances past
//     offsets covered by every member's snapshot watermark,
//     so durable history can be bounded even on long-running
//     clusters.
//
// Apps that DON'T implement Snapshotter still work, but:
//   - Joiners must replay from wherever the cluster's logs
//     start (limited by current trim watermarks).
//   - Trim waits for every member to APPLY past the watermark;
//     no further compaction is possible.
//
// Design boundary: the app owns serialization format, durable
// storage, cadence, size, compression, versioning. Comlink only
// ever sees the opaque bytes + a throughOffset boundary.
type Snapshotter interface {
	// Snapshot returns a durably-serializable representation of
	// the SM's state as of its most recent Apply. The bytes are
	// opaque to comlink — the app picks the format. The returned
	// throughOffset MUST be the substrate-supplied Message.Offset
	// of the latest Apply included in the snapshot, OR zero if
	// no Apply has happened yet.
	//
	// Implementations must be safe to call from any goroutine.
	// The substrate guarantees no Apply is in-flight on the
	// caller goroutine — but Snapshot CAN race with peer Submits
	// applying on a different goroutine, so apps that need a
	// consistent point-in-time view should snapshot under their
	// own internal lock (typically the same lock Apply uses).
	Snapshot() (bytes []byte, throughOffset uint64, err error)

	// Restore re-installs SM state from snapshot bytes. Called
	// exactly once at Substrate construction time if a snapshot
	// is supplied via SubstrateConfig.InitialSnapshot.
	//
	// After Restore returns, the substrate plays Apply for
	// messages whose Offset is STRICTLY GREATER than the
	// snapshot's throughOffset. Apps must finish installing the
	// state synchronously before returning.
	Restore(bytes []byte) error
}

// Snapshot is the wire-form pair (opaque bytes, throughOffset)
// that travels between snapshotter calls and substrate config.
type Snapshot struct {
	// Bytes is the SM's serialized state. Opaque to comlink.
	Bytes []byte
	// ThroughOffset is the log offset of the last Apply included
	// in this snapshot. Apply messages with Offset > ThroughOffset
	// are still pending and will be replayed by the substrate
	// after Restore.
	ThroughOffset uint64
}

// snapshotWatermark is the per-substrate tracker for "this
// replica has durably snapshotted through offset N". Updated
// via Substrate.AdvanceSnapshotWatermark. Published to peers
// via the trim watermark protocol (Phase 10(c)) and consulted
// during sponsor handshake (Phase 10(d)).
//
// Uses an atomic so reads from the trim goroutine don't need
// to coordinate with the app's AdvanceSnapshotWatermark calls.
type snapshotWatermark struct {
	throughOffset atomic.Uint64
}

func (w *snapshotWatermark) advance(offset uint64) {
	for {
		cur := w.throughOffset.Load()
		if offset <= cur {
			return // monotonic — never goes backwards
		}
		if w.throughOffset.CompareAndSwap(cur, offset) {
			return
		}
	}
}

func (w *snapshotWatermark) get() uint64 {
	return w.throughOffset.Load()
}
