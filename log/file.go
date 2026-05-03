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

package log

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// File is a single-file MessageLog used in Phase 0.
//
// Architecture: an in-memory cache (entries + index, identical in
// shape to the Memory impl) is the source of truth for all reads.
// The on-disk file is the durable write-ahead backing — Append writes
// through to the file (fsync) and then updates the cache; reads
// (Lookup, Range) are pure memory accesses with no I/O. The file is
// scanned exactly once at Open time to populate the cache.
//
// On-disk format:
//
//	header := magicBytes (8) | conv_id_len (uint32 LE) | conv_id_bytes
//	frame  := payload_len (uint32 LE) | sender_seq (uint64 LE) | crc32 (uint32 LE) | payload_bytes
//	file   := header | frame*
//
// Each frame stores one Envelope. payload_bytes is the proto wire-form
// of the envelope. crc32 covers payload_bytes (Castagnoli table). The
// frame's logical Offset equals its zero-based position in the file
// (after the header), regardless of any prior Truncate — i.e. logical
// offsets remain stable across Truncate. A sidecar meta file records
// firstOffset; entries below firstOffset are physically present on
// disk (Phase 4's segmented impl will reclaim them) but are excluded
// from the in-memory cache so Lookup/Range correctly report them
// gone.
type File struct {
	mu       sync.Mutex
	dir      string
	dataFile *os.File
	dataPath string
	metaPath string
	closed   bool

	// In-memory cache — source of truth for all reads.
	convID      *pb.ConversationID
	entries     []Entry
	firstOffset Offset
	index       map[indexKey]Offset
}

const (
	dataFileName = "log.data"
	metaFileName = "log.meta"
)

// magicBytes prefixes the data file so we can sanity-check that the
// file we're reopening is actually one of ours.
var magicBytes = [8]byte{'c', 'o', 'm', 'l', 'i', 'n', 'k', 1}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFile opens (creating if necessary) a File-backed log in dir
// bound to convID. Returns ErrConversationMismatch if the on-disk log
// was previously bound to a different conversation.
func OpenFile(dir string, convID *pb.ConversationID) (*File, error) {
	if convID == nil {
		return nil, errors.New("log: nil conversation id")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f := &File{
		dir:      dir,
		dataPath: filepath.Join(dir, dataFileName),
		metaPath: filepath.Join(dir, metaFileName),
		convID:   proto.Clone(convID).(*pb.ConversationID),
		index:    make(map[indexKey]Offset),
	}
	if err := f.openOrCreate(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) openOrCreate() error {
	// Read meta if present (firstOffset).
	if data, err := os.ReadFile(f.metaPath); err == nil {
		if len(data) != 8 {
			return fmt.Errorf("%w: meta file has wrong length %d", ErrCorrupt, len(data))
		}
		f.firstOffset = Offset(binary.LittleEndian.Uint64(data))
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	df, err := os.OpenFile(f.dataPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	f.dataFile = df

	st, err := df.Stat()
	if err != nil {
		_ = df.Close()
		return err
	}
	if st.Size() == 0 {
		if err := f.writeHeader(); err != nil {
			_ = df.Close()
			return err
		}
		return nil
	}

	if err := f.readAndValidateHeader(); err != nil {
		_ = df.Close()
		return err
	}
	if err := f.scanIntoCache(); err != nil {
		_ = df.Close()
		return err
	}
	return nil
}

func (f *File) writeHeader() error {
	convBytes, err := proto.Marshal(f.convID)
	if err != nil {
		return err
	}
	buf := make([]byte, 8+4+len(convBytes))
	copy(buf[:8], magicBytes[:])
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(convBytes)))
	copy(buf[12:], convBytes)
	if _, err := f.dataFile.Write(buf); err != nil {
		return err
	}
	return f.dataFile.Sync()
}

func (f *File) readAndValidateHeader() error {
	if _, err := f.dataFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var magic [8]byte
	if _, err := io.ReadFull(f.dataFile, magic[:]); err != nil {
		return fmt.Errorf("%w: read magic: %v", ErrCorrupt, err)
	}
	if magic != magicBytes {
		return fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(f.dataFile, lenBuf[:]); err != nil {
		return fmt.Errorf("%w: read conv-id length: %v", ErrCorrupt, err)
	}
	convLen := binary.LittleEndian.Uint32(lenBuf[:])
	convBytes := make([]byte, convLen)
	if _, err := io.ReadFull(f.dataFile, convBytes); err != nil {
		return fmt.Errorf("%w: read conv-id: %v", ErrCorrupt, err)
	}
	var stored pb.ConversationID
	if err := proto.Unmarshal(convBytes, &stored); err != nil {
		return fmt.Errorf("%w: parse conv-id: %v", ErrCorrupt, err)
	}
	if !proto.Equal(&stored, f.convID) {
		return ErrConversationMismatch
	}
	return nil
}

// scanIntoCache reads every frame from after the header to EOF and
// populates entries + index. Frames with logical offset < firstOffset
// are read (so we advance file position correctly) but skipped from
// the cache.
//
// A trailing partial frame (e.g. crash mid-Append before fsync
// completed) is silently truncated from the file so we never expose
// half-written entries.
func (f *File) scanIntoCache() error {
	pos, err := f.dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	off := Offset(0)
	for {
		framePos := pos
		var hdr [16]byte // payload_len(4) | sender_seq(8) | crc32(4)
		_, err := io.ReadFull(f.dataFile, hdr[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || err != nil {
			if truncErr := f.dataFile.Truncate(framePos); truncErr != nil {
				return truncErr
			}
			if _, err := f.dataFile.Seek(framePos, io.SeekStart); err != nil {
				return err
			}
			pos = framePos
			break
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[0:4])
		senderSeq := binary.LittleEndian.Uint64(hdr[4:12])
		wantCRC := binary.LittleEndian.Uint32(hdr[12:16])
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f.dataFile, payload); err != nil {
			if truncErr := f.dataFile.Truncate(framePos); truncErr != nil {
				return truncErr
			}
			if _, err := f.dataFile.Seek(framePos, io.SeekStart); err != nil {
				return err
			}
			pos = framePos
			break
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return fmt.Errorf("%w: crc mismatch at offset %d", ErrCorrupt, off)
		}
		if off >= f.firstOffset {
			env := &pb.Envelope{}
			if err := proto.Unmarshal(payload, env); err != nil {
				return fmt.Errorf("%w: parse envelope at offset %d: %v", ErrCorrupt, off, err)
			}
			f.entries = append(f.entries, Entry{Offset: off, Envelope: env})
			f.index[indexKey{sender: string(env.GetId().GetSender().GetValue()), seq: senderSeq}] = off
		}
		pos = framePos + int64(len(hdr)) + int64(len(payload))
		off++
	}
	if _, err := f.dataFile.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	return nil
}

// ConversationID returns a clone of the bound conversation ID.
func (f *File) ConversationID() *pb.ConversationID {
	return proto.Clone(f.convID).(*pb.ConversationID)
}

// Append durably writes envelope and updates the in-memory cache.
func (f *File) Append(_ context.Context, envelope *pb.Envelope, senderSeq uint64) (Offset, error) {
	if envelope == nil || envelope.GetId() == nil || envelope.GetId().GetSender() == nil {
		return 0, ErrCorrupt
	}
	payload, err := proto.Marshal(envelope)
	if err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, ErrClosed
	}

	pos, err := f.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	hdr := make([]byte, 16)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(hdr[4:12], senderSeq)
	binary.LittleEndian.PutUint32(hdr[12:16], crc32.Checksum(payload, crcTable))
	if _, err := f.dataFile.Write(hdr); err != nil {
		_ = f.dataFile.Truncate(pos)
		return 0, err
	}
	if _, err := f.dataFile.Write(payload); err != nil {
		_ = f.dataFile.Truncate(pos)
		return 0, err
	}
	if err := f.dataFile.Sync(); err != nil {
		_ = f.dataFile.Truncate(pos)
		return 0, err
	}

	off := f.firstOffset + Offset(len(f.entries))
	cloned := proto.Clone(envelope).(*pb.Envelope)
	f.entries = append(f.entries, Entry{Offset: off, Envelope: cloned})
	f.index[indexKey{sender: string(envelope.GetId().GetSender().GetValue()), seq: senderSeq}] = off
	return off, nil
}

// LookupBySender returns the entry indexed for (senderReplica, senderSeq).
// Pure memory access — no file I/O.
func (f *File) LookupBySender(_ context.Context, senderReplica []byte, senderSeq uint64) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return Entry{}, ErrClosed
	}
	off, ok := f.index[indexKey{sender: string(senderReplica), seq: senderSeq}]
	if !ok || off < f.firstOffset {
		return Entry{}, ErrNotFound
	}
	e := f.entries[off-f.firstOffset]
	return Entry{Offset: e.Offset, Envelope: proto.Clone(e.Envelope).(*pb.Envelope)}, nil
}

// Range iterates entries in [from, to). Pure memory access.
func (f *File) Range(_ context.Context, from, to Offset) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			yield(Entry{}, ErrClosed)
			return
		}
		first := f.firstOffset
		next := first + Offset(len(f.entries))
		if from < first {
			from = first
		}
		if to > next {
			to = next
		}
		var snapshot []Entry
		if from < to {
			snapshot = make([]Entry, 0, to-from)
			for i := from; i < to; i++ {
				e := f.entries[i-first]
				snapshot = append(snapshot, Entry{Offset: e.Offset, Envelope: proto.Clone(e.Envelope).(*pb.Envelope)})
			}
		}
		f.mu.Unlock()
		for _, e := range snapshot {
			if !yield(e, nil) {
				return
			}
		}
	}
}

// FirstOffset returns the lowest readable offset.
func (f *File) FirstOffset() Offset {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.firstOffset
}

// NextOffset returns the offset the next Append will use.
func (f *File) NextOffset() Offset {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.firstOffset + Offset(len(f.entries))
}

// Truncate drops in-memory state below belowOffset and persists the
// new firstOffset via the meta file. The on-disk data file is left in
// place; Phase 4's segmented impl will reclaim it.
func (f *File) Truncate(_ context.Context, belowOffset Offset) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	if belowOffset <= f.firstOffset {
		return nil
	}
	maxFirst := f.firstOffset + Offset(len(f.entries))
	if belowOffset > maxFirst {
		belowOffset = maxFirst
	}
	if err := f.writeMetaLocked(belowOffset); err != nil {
		return err
	}
	drop := int(belowOffset - f.firstOffset)
	for k, off := range f.index {
		if off < belowOffset {
			delete(f.index, k)
		}
	}
	f.entries = f.entries[drop:]
	f.firstOffset = belowOffset
	return nil
}

func (f *File) writeMetaLocked(firstOffset Offset) error {
	tmp, err := os.CreateTemp(f.dir, "log.meta.tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(firstOffset))
	if _, err := tmp.Write(buf[:]); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, f.metaPath); err != nil {
		cleanup()
		return err
	}
	dirF, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	syncErr := dirF.Sync()
	closeErr := dirF.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Close releases the data file. The in-memory cache is dropped.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	f.entries = nil
	f.index = nil
	if f.dataFile != nil {
		err := f.dataFile.Close()
		f.dataFile = nil
		return err
	}
	return nil
}
