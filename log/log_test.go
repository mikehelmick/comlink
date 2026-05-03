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

package log_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"google.golang.org/protobuf/proto"
)

// fixedConvID returns a deterministic ConversationID built from a tag
// so different tests don't accidentally collide on their on-disk
// state.
func fixedConvID(tag string) *pb.ConversationID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ConversationID{Value: b}
}

func replicaID(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// makeEnvelope builds a deterministic envelope for a (sender,
// senderSeq) pair. The vector clock is filled with senderSeq at index
// 0; tests that exercise vector-clock semantics can override.
func makeEnvelope(convID *pb.ConversationID, sender *pb.ReplicaID, senderSeq uint64, payload string) *pb.Envelope {
	return &pb.Envelope{
		Id: &pb.MessageID{
			ConversationId: convID,
			Sender:         sender,
			VectorClock:    []uint64{senderSeq},
		},
		Payload: []byte(payload),
	}
}

type factory func(t *testing.T, convID *pb.ConversationID) clog.MessageLog

func memFactory(t *testing.T, convID *pb.ConversationID) clog.MessageLog {
	t.Helper()
	m := clog.NewMemory(convID)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// runConformance exercises the contract on any MessageLog impl.
func runConformance(t *testing.T, makeLog factory) {
	t.Helper()
	ctx := context.Background()
	convID := fixedConvID("conformance")
	sender := replicaID("alice")

	t.Run("conversation_id_roundtrips", func(t *testing.T) {
		l := makeLog(t, convID)
		got := l.ConversationID()
		if !proto.Equal(got, convID) {
			t.Fatalf("ConversationID = %v, want %v", got, convID)
		}
	})

	t.Run("append_assigns_sequential_offsets", func(t *testing.T) {
		l := makeLog(t, convID)
		for i := uint64(0); i < 5; i++ {
			off, err := l.Append(ctx, makeEnvelope(convID, sender, i+1, fmt.Sprintf("msg-%d", i)), i+1)
			if err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
			if want := clog.Offset(i); off != want {
				t.Fatalf("Append %d returned offset %d, want %d", i, off, want)
			}
		}
		if got := l.NextOffset(); got != 5 {
			t.Fatalf("NextOffset = %d, want 5", got)
		}
		if got := l.FirstOffset(); got != 0 {
			t.Fatalf("FirstOffset = %d, want 0", got)
		}
	})

	t.Run("lookup_by_sender", func(t *testing.T) {
		l := makeLog(t, convID)
		for i := uint64(1); i <= 3; i++ {
			if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, fmt.Sprintf("p%d", i)), i); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		got, err := l.LookupBySender(ctx, sender.GetValue(), 2)
		if err != nil {
			t.Fatalf("LookupBySender: %v", err)
		}
		if string(got.Envelope.GetPayload()) != "p2" {
			t.Fatalf("Lookup payload = %q, want %q", got.Envelope.GetPayload(), "p2")
		}
		if got.Offset != 1 {
			t.Fatalf("Lookup offset = %d, want 1", got.Offset)
		}
	})

	t.Run("lookup_missing_returns_not_found", func(t *testing.T) {
		l := makeLog(t, convID)
		_, err := l.LookupBySender(ctx, sender.GetValue(), 99)
		if !errors.Is(err, clog.ErrNotFound) {
			t.Fatalf("LookupBySender err = %v, want ErrNotFound", err)
		}
	})

	t.Run("range_yields_inclusive_from_exclusive_to", func(t *testing.T) {
		l := makeLog(t, convID)
		for i := uint64(1); i <= 5; i++ {
			if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, fmt.Sprintf("e%d", i)), i); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		for entry, err := range l.Range(ctx, 1, 4) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(entry.Envelope.GetPayload()))
		}
		want := []string{"e2", "e3", "e4"}
		if !slices.Equal(got, want) {
			t.Fatalf("Range(1,4) = %v, want %v", got, want)
		}
	})

	t.Run("range_to_endoflog_reads_to_tail", func(t *testing.T) {
		l := makeLog(t, convID)
		for i := uint64(1); i <= 3; i++ {
			if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, fmt.Sprintf("t%d", i)), i); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		for entry, err := range l.Range(ctx, 0, clog.EndOfLog) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(entry.Envelope.GetPayload()))
		}
		want := []string{"t1", "t2", "t3"}
		if !slices.Equal(got, want) {
			t.Fatalf("Range(0, EndOfLog) = %v, want %v", got, want)
		}
	})

	t.Run("truncate_drops_below_threshold", func(t *testing.T) {
		l := makeLog(t, convID)
		// Append 5 entries: seq i lands at offset i-1.
		for i := uint64(1); i <= 5; i++ {
			if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, fmt.Sprintf("d%d", i)), i); err != nil {
				t.Fatal(err)
			}
		}
		// Truncate(3) drops offsets 0, 1, 2 — i.e. seqs 1, 2, 3.
		if err := l.Truncate(ctx, 3); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
		if got := l.FirstOffset(); got != 3 {
			t.Fatalf("FirstOffset after truncate = %d, want 3", got)
		}
		for _, gone := range []uint64{1, 2, 3} {
			if _, err := l.LookupBySender(ctx, sender.GetValue(), gone); !errors.Is(err, clog.ErrNotFound) {
				t.Errorf("Lookup truncated seq %d: err = %v, want ErrNotFound", gone, err)
			}
		}
		// Survivors (seqs 4, 5) are still readable.
		got, err := l.LookupBySender(ctx, sender.GetValue(), 4)
		if err != nil {
			t.Fatalf("Lookup survivor seq 4: %v", err)
		}
		if string(got.Envelope.GetPayload()) != "d4" {
			t.Fatalf("payload after truncate = %q, want d4", got.Envelope.GetPayload())
		}
	})

	t.Run("truncate_idempotent", func(t *testing.T) {
		l := makeLog(t, convID)
		for i := uint64(1); i <= 3; i++ {
			if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, ""), i); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Truncate(ctx, 2); err != nil {
			t.Fatal(err)
		}
		if err := l.Truncate(ctx, 2); err != nil {
			t.Fatalf("second Truncate(2): %v", err)
		}
		if err := l.Truncate(ctx, 1); err != nil {
			t.Fatalf("Truncate to lower bound: %v", err)
		}
		if got := l.FirstOffset(); got != 2 {
			t.Fatalf("FirstOffset after no-op truncate = %d, want 2", got)
		}
	})

	t.Run("returned_envelopes_isolated", func(t *testing.T) {
		l := makeLog(t, convID)
		env := makeEnvelope(convID, sender, 1, "original")
		if _, err := l.Append(ctx, env, 1); err != nil {
			t.Fatal(err)
		}
		// Mutate the original; stored copy must not change.
		env.Payload = []byte("MUTATED")
		got, err := l.LookupBySender(ctx, sender.GetValue(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Envelope.GetPayload()) != "original" {
			t.Fatalf("stored payload was mutated through input slice: %q", got.Envelope.GetPayload())
		}
		// Mutate the returned copy; another lookup must see the
		// original.
		got.Envelope.Payload = []byte("MUTATED2")
		got2, err := l.LookupBySender(ctx, sender.GetValue(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if string(got2.Envelope.GetPayload()) != "original" {
			t.Fatalf("stored payload was mutated through output slice: %q", got2.Envelope.GetPayload())
		}
	})

	t.Run("concurrent_append_safe", func(t *testing.T) {
		l := makeLog(t, convID)
		const N = 50
		// Different senders so each (sender, senderSeq) is unique.
		var wg sync.WaitGroup
		for i := range N {
			wg.Go(func() {
				s := replicaID(fmt.Sprintf("conc-%d", i))
				if _, err := l.Append(ctx, makeEnvelope(convID, s, 1, "x"), 1); err != nil {
					t.Errorf("Append %d: %v", i, err)
				}
			})
		}
		wg.Wait()
		if got := l.NextOffset(); got != N {
			t.Fatalf("NextOffset after concurrent appends = %d, want %d", got, N)
		}
	})
}

func TestMemoryConformance(t *testing.T) {
	runConformance(t, memFactory)
}

func TestFileConformance(t *testing.T) {
	runConformance(t, func(t *testing.T, convID *pb.ConversationID) clog.MessageLog {
		t.Helper()
		dir := t.TempDir()
		l, err := clog.OpenFile(dir, convID)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		t.Cleanup(func() { _ = l.Close() })
		return l
	})
}

// TestFilePersistsAcrossReopen is the Phase 0 exit-criterion crash-
// safety test: every successfully-returned Append survives a process
// kill (modeled by Close + reopen) and is recoverable via Range.
func TestFilePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	convID := fixedConvID("persist")
	sender := replicaID("alice")
	dir := t.TempDir()

	l1, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if _, err := l1.Append(ctx, makeEnvelope(convID, sender, i, fmt.Sprintf("m%d", i)), i); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 5 {
		t.Fatalf("NextOffset after reopen = %d, want 5", got)
	}
	var got []string
	for entry, err := range l2.Range(ctx, 0, clog.EndOfLog) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(entry.Envelope.GetPayload()))
	}
	want := []string{"m1", "m2", "m3", "m4", "m5"}
	if !slices.Equal(got, want) {
		t.Fatalf("after reopen Range = %v, want %v", got, want)
	}
}

// TestFileRejectsDifferentConversationID is the Phase 0 exit-
// criterion sanity check from PLAN §2.10.
func TestFileRejectsDifferentConversationID(t *testing.T) {
	dir := t.TempDir()
	l1, err := clog.OpenFile(dir, fixedConvID("conv-A"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = clog.OpenFile(dir, fixedConvID("conv-B"))
	if !errors.Is(err, clog.ErrConversationMismatch) {
		t.Fatalf("OpenFile with different conv ID: err = %v, want ErrConversationMismatch", err)
	}
}

// TestFileTrailingPartialFrameRecovered models a crash that left a
// half-written frame on disk — a real concern with single-file
// append-only logs. The recovered log should drop the partial and
// expose only fully-fsynced entries.
func TestFileTrailingPartialFrameRecovered(t *testing.T) {
	ctx := context.Background()
	convID := fixedConvID("partial")
	sender := replicaID("alice")
	dir := t.TempDir()

	l1, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l1.Append(ctx, makeEnvelope(convID, sender, 1, "good"), 1); err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}

	// Append garbage tail bytes simulating an interrupted write.
	dataPath := filepath.Join(dir, "log.data")
	df, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	junk := make([]byte, 16)
	binary.LittleEndian.PutUint32(junk[0:4], 999_999_999) // claims a huge payload that doesn't exist
	if _, err := df.Write(junk); err != nil {
		t.Fatal(err)
	}
	if err := df.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatalf("reopen with partial tail: %v", err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 1 {
		t.Fatalf("NextOffset = %d, want 1 (partial frame should have been truncated)", got)
	}

	// New Appends should succeed cleanly post-recovery.
	if _, err := l2.Append(ctx, makeEnvelope(convID, sender, 2, "after"), 2); err != nil {
		t.Fatalf("Append after partial-recovery: %v", err)
	}
	got, err := l2.LookupBySender(ctx, sender.GetValue(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Envelope.GetPayload()) != "after" {
		t.Fatalf("post-recovery Lookup = %q, want %q", got.Envelope.GetPayload(), "after")
	}
}

// TestFileTruncationPersistsAcrossReopen ensures the meta file is
// honored after restart.
func TestFileTruncationPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	convID := fixedConvID("truncate-persist")
	sender := replicaID("alice")
	dir := t.TempDir()

	l1, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 4; i++ {
		if _, err := l1.Append(ctx, makeEnvelope(convID, sender, i, ""), i); err != nil {
			t.Fatal(err)
		}
	}
	if err := l1.Truncate(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := l1.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := clog.OpenFile(dir, convID)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if got := l2.FirstOffset(); got != 2 {
		t.Fatalf("FirstOffset after reopen = %d, want 2", got)
	}
	if _, err := l2.LookupBySender(ctx, sender.GetValue(), 1); !errors.Is(err, clog.ErrNotFound) {
		t.Errorf("Lookup truncated seq 1 after reopen: err = %v, want ErrNotFound", err)
	}
}

// BenchmarkFileAppend gives an early signal that fsync-per-append is
// not catastrophically slow. PLAN §2.8 calls this out as a known
// performance trade-off.
func BenchmarkFileAppend(b *testing.B) {
	ctx := context.Background()
	convID := fixedConvID("bench")
	sender := replicaID("alice")
	dir := b.TempDir()
	l, err := clog.OpenFile(dir, convID)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	env := makeEnvelope(convID, sender, 0, "benchmark-payload")
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		env.Id.VectorClock[0] = uint64(i + 1)
		if _, err := l.Append(ctx, env, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileLookup confirms LookupBySender on the file impl runs
// at memory speed (no file I/O). Psync's lost-message protocol
// hammers this path; if it ever became disk-bound we'd notice here
// before it shows up in higher-level latency.
func BenchmarkFileLookup(b *testing.B) {
	ctx := context.Background()
	convID := fixedConvID("bench-lookup")
	sender := replicaID("alice")
	dir := b.TempDir()
	l, err := clog.OpenFile(dir, convID)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	const N = 10_000
	for i := uint64(1); i <= N; i++ {
		if _, err := l.Append(ctx, makeEnvelope(convID, sender, i, "x"), i); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		seq := uint64(i%N) + 1
		if _, err := l.LookupBySender(ctx, sender.GetValue(), seq); err != nil {
			b.Fatal(err)
		}
	}
}
