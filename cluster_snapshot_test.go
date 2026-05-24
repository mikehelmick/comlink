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
	"bytes"
	"testing"
)

// TestSnapshotChunksRoundTrip: serializing a snapshot with the
// chunked encoder, then reassembling chunk-by-chunk, recovers
// the original bytes and preserves through_offset. Exercises
// the chunked wire format (Phase 10(c)) without needing a real
// gRPC server in the unit-test layer.
func TestSnapshotChunksRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		input      []byte
		chunkBytes int
		throughOff uint64
		wantChunks int
	}{
		{"empty", []byte{}, 1024, 99, 1},
		{"under one chunk", []byte("hello"), 1024, 1, 1},
		{"exactly one chunk", bytes.Repeat([]byte{0xAA}, 1024), 1024, 2, 1},
		{"two chunks even", bytes.Repeat([]byte{0xAA}, 2048), 1024, 3, 2},
		{"three chunks ragged", bytes.Repeat([]byte{0xAA}, 1024+512), 1024, 4, 2},
		{"big-ish", bytes.Repeat([]byte{0xCC}, 5*1024*1024+17), 1 << 20, 5000, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := snapshotChunksForTest(tc.input, tc.throughOff, tc.chunkBytes)
			if err != nil {
				t.Fatalf("snapshotChunksForTest: %v", err)
			}
			if got := len(chunks); got != tc.wantChunks {
				t.Errorf("len(chunks) = %d, want %d", got, tc.wantChunks)
			}
			// Reassemble.
			var got bytes.Buffer
			for i, c := range chunks {
				if uint64(i) != c.GetChunkIndex() {
					t.Errorf("chunk %d: index = %d", i, c.GetChunkIndex())
				}
				// Header only on first.
				if i == 0 {
					h := c.GetHeader()
					if h == nil {
						t.Fatal("chunk 0 missing header")
					}
					if h.GetThroughOffset() != tc.throughOff {
						t.Errorf("through_offset = %d, want %d", h.GetThroughOffset(), tc.throughOff)
					}
					if h.GetTotalSizeBytes() != uint64(len(tc.input)) {
						t.Errorf("total_size_bytes = %d, want %d", h.GetTotalSizeBytes(), len(tc.input))
					}
				} else if c.GetHeader() != nil {
					t.Errorf("chunk %d has header set (non-first)", i)
				}
				// last only on the last chunk.
				if c.GetLast() != (i == len(chunks)-1) {
					t.Errorf("chunk %d last = %v, want %v", i, c.GetLast(), i == len(chunks)-1)
				}
				got.Write(c.GetData())
			}
			if !bytes.Equal(got.Bytes(), tc.input) {
				t.Errorf("reassembled mismatch: len got %d, want %d (first diff might be byte-wise)", got.Len(), len(tc.input))
			}
		})
	}
}
