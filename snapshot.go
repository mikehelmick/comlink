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

import (
	"io"
	"sync/atomic"
)

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
	// Apps with huge state should consider a future
	// StreamingSnapshotter (Phase 11+) that writes to an
	// io.Writer instead of returning bytes; for Phase 10 the
	// byte-slice form is the only producer-side API and apps
	// with multi-GB state should handle the memory budget
	// themselves (e.g., gzip + chunked encoding inside the
	// returned []byte, or use the streaming Persist API once
	// added).
	//
	// Implementations must be safe to call from any goroutine.
	// Snapshot CAN race with peer Submits applying on a
	// different goroutine, so apps that need a consistent
	// point-in-time view should snapshot under their own
	// internal lock (typically the same lock Apply uses).
	Snapshot() (bytes []byte, throughOffset uint64, err error)

	// Restore re-installs SM state from a snapshot reader.
	// Called exactly once at Substrate construction time if a
	// snapshot is supplied via SubstrateConfig.InitialSnapshot.
	//
	// io.Reader (not []byte) because the wire transfer is
	// chunked (Phase 10(c) StreamSnapshot RPC) and joiners
	// stage chunks to disk: passing the reassembled file as a
	// Reader avoids holding multi-GB snapshots in memory.
	// Implementations may io.ReadAll if their state is small.
	//
	// After Restore returns, the substrate plays Apply for
	// messages whose Offset is STRICTLY GREATER than the
	// snapshot's throughOffset. Apps must finish installing the
	// state synchronously before returning. The caller closes
	// any underlying file after Restore returns.
	Restore(r io.Reader) error
}

// Snapshot is the wire-form pair (opaque source, throughOffset)
// passed to SubstrateConfig.InitialSnapshot. Bytes OR Reader
// must be set (Reader takes precedence) — the substrate calls
// SM.Restore(reader) where reader is either Reader directly or
// a bytes.NewReader(Bytes) wrapper.
//
// The Reader form is what the streaming join handshake uses
// (Phase 10(d)) — joiners stream chunks to a temp file in the
// DataDir, then open it as the reader. Apps loading their own
// persisted snapshots can use either form.
type Snapshot struct {
	// Bytes is the SM's serialized state, in memory. For small
	// snapshots. Either Bytes or Reader must be non-nil.
	Bytes []byte
	// Reader is a stream over the snapshot's serialized state.
	// For large snapshots that shouldn't be held in memory.
	// Either Bytes or Reader must be non-nil; if both, Reader
	// wins. The substrate does NOT close it — caller owns
	// lifecycle.
	Reader io.Reader
	// ThroughOffset is the log offset of the last Apply included
	// in this snapshot. Apply messages with Offset > ThroughOffset
	// are still pending and will be replayed by the substrate
	// after Restore.
	ThroughOffset uint64
}

// reader returns an io.Reader over the snapshot's bytes, using
// the Reader field if set and otherwise wrapping Bytes.
// Returns nil if neither is set.
func (s *Snapshot) reader() io.Reader {
	if s == nil {
		return nil
	}
	if s.Reader != nil {
		return s.Reader
	}
	if s.Bytes != nil {
		return bytesReader(s.Bytes)
	}
	return nil
}

// bytesReader is the trivial bytes-to-Reader adapter, kept as
// a helper so the import stays inside snapshot.go.
func bytesReader(b []byte) io.Reader {
	return &byteSliceReader{b: b}
}

type byteSliceReader struct {
	b []byte
	i int
}

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
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
	w.advanceReturning(offset)
}

// advanceReturning is the same as advance but returns whether
// the watermark actually moved. Used by Substrate to decide
// whether a broadcast is needed.
func (w *snapshotWatermark) advanceReturning(offset uint64) bool {
	for {
		cur := w.throughOffset.Load()
		if offset <= cur {
			return false
		}
		if w.throughOffset.CompareAndSwap(cur, offset) {
			return true
		}
	}
}

func (w *snapshotWatermark) get() uint64 {
	return w.throughOffset.Load()
}
