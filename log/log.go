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

// Package log defines and implements MessageLog, the durable
// append-only stream of accepted Psync messages described in
// PLAN §2.8.
//
// The log holds Envelopes (the wire/storage form from
// proto/comlink/v1). Psync writes every accepted message through it
// before delivering upward; on restart, Psync rebuilds its in-memory
// context graph by replaying the log and then catching the tail from
// peers.
//
// Architecture: the file-backed implementation keeps an in-memory
// cache (entries slice + index) as the source of truth for all
// reads. The on-disk file is the durable write-ahead backing — Append
// writes through to the file (fsync) and then updates the cache;
// Lookup and Range are pure memory accesses with no file I/O. The
// file is scanned exactly once at Open time to populate the cache,
// then is only touched by subsequent Appends. This keeps Psync's
// hot-path lookups fast (the lost-message protocol hammers Lookup);
// the file exists for crash-recovery on restart.
//
// Phase 0 ships an in-memory implementation (Memory) and a single-
// file implementation (File) with fsync-on-append. Phase 4 will
// replace File with a segmented implementation so Truncate can
// reclaim disk; the interface is shaped to make that swap
// transparent.
//
// Index. The log maintains an in-memory index from
// (sender_replica_bytes, sender_sequence) -> Offset because that is
// the natural lookup key for Psync's lost-message protocol. Append
// callers must pass senderSeq explicitly because the log itself does
// not know the conversation's membership-slot ordering needed to
// extract that value from envelope.id.vector_clock.
package log

import (
	"context"
	"errors"
	"iter"
	"math"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
)

// Offset is a sequential, monotonically-increasing index into the log.
// Offsets persist across restarts: entry K is always at offset K, even
// after a Truncate has dropped earlier entries.
type Offset uint64

// EndOfLog is the upper bound for Range that means "to the current
// tail at the time iteration starts."
const EndOfLog Offset = math.MaxUint64

// Common errors.
var (
	// ErrNotFound means the log has no entry for the given lookup.
	ErrNotFound = errors.New("log: entry not found")
	// ErrConversationMismatch means the on-disk log was opened with a
	// different ConversationID than the one passed to Open.
	ErrConversationMismatch = errors.New("log: log belongs to a different conversation")
	// ErrClosed means the log was closed.
	ErrClosed = errors.New("log: log closed")
	// ErrCorrupt means the on-disk log failed integrity checks during
	// open or read.
	ErrCorrupt = errors.New("log: log is corrupt")
)

// Entry is a single record in the log.
type Entry struct {
	Offset   Offset
	Envelope *pb.Envelope
}

// MessageLog is the durable, ordered, append-only message stream.
//
// Implementations must be safe for concurrent use.
type MessageLog interface {
	// ConversationID returns the conversation this log is bound to.
	ConversationID() *pb.ConversationID

	// Append durably writes envelope and returns its assigned offset.
	// senderSeq must equal envelope.id.vector_clock at the sender's
	// slot — the log indexes (envelope.id.sender, senderSeq) for
	// LookupBySender.
	//
	// On return, the entry is durable: it survives a process kill.
	Append(ctx context.Context, envelope *pb.Envelope, senderSeq uint64) (Offset, error)

	// LookupBySender returns the entry whose envelope was sent by
	// senderReplica with sequence number senderSeq, or ErrNotFound.
	// senderReplica is the raw value bytes of pb.ReplicaID.
	LookupBySender(ctx context.Context, senderReplica []byte, senderSeq uint64) (Entry, error)

	// Range yields entries in offset order, starting at from
	// (inclusive) and ending at to (exclusive). Use EndOfLog for to
	// to read everything from `from` to the current tail.
	//
	// Iteration stops on first error; the error is yielded as the
	// second value of the final pair.
	Range(ctx context.Context, from, to Offset) iter.Seq2[Entry, error]

	// FirstOffset returns the lowest offset still readable. It is
	// initially 0; advances after Truncate.
	FirstOffset() Offset

	// NextOffset returns the offset that the next Append will be
	// assigned (i.e. one past the highest stored offset).
	NextOffset() Offset

	// Truncate marks every entry with offset < belowOffset as
	// removable. Reads and lookups for those offsets will return
	// ErrNotFound after Truncate returns. Phase 0 implementations may
	// or may not actually reclaim disk; Phase 4's segmented impl
	// will. Idempotent.
	Truncate(ctx context.Context, belowOffset Offset) error

	// Close releases resources.
	Close() error
}
