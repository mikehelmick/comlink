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
	"iter"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Memory is an in-process MessageLog backed by a slice. Useful for
// tests. Not durable.
type Memory struct {
	mu          sync.RWMutex
	convID      *pb.ConversationID
	entries     []Entry
	firstOffset Offset
	index       map[indexKey]Offset
	closed      bool
}

type indexKey struct {
	sender string
	seq    uint64
}

// NewMemory returns an empty in-memory log bound to convID.
func NewMemory(convID *pb.ConversationID) *Memory {
	return &Memory{
		convID: proto.Clone(convID).(*pb.ConversationID),
		index:  make(map[indexKey]Offset),
	}
}

// ConversationID returns a clone of the bound conversation ID.
func (m *Memory) ConversationID() *pb.ConversationID {
	return proto.Clone(m.convID).(*pb.ConversationID)
}

// Append stores a clone of envelope.
func (m *Memory) Append(_ context.Context, envelope *pb.Envelope, senderSeq uint64) (Offset, error) {
	if envelope == nil || envelope.GetId() == nil || envelope.GetId().GetSender() == nil {
		return 0, ErrCorrupt
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	off := m.firstOffset + Offset(len(m.entries))
	cloned := proto.Clone(envelope).(*pb.Envelope)
	m.entries = append(m.entries, Entry{Offset: off, Envelope: cloned})
	m.index[indexKey{sender: string(envelope.GetId().GetSender().GetValue()), seq: senderSeq}] = off
	return off, nil
}

// LookupBySender returns the entry for (senderReplica, senderSeq).
func (m *Memory) LookupBySender(_ context.Context, senderReplica []byte, senderSeq uint64) (Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return Entry{}, ErrClosed
	}
	off, ok := m.index[indexKey{sender: string(senderReplica), seq: senderSeq}]
	if !ok {
		return Entry{}, ErrNotFound
	}
	if off < m.firstOffset {
		return Entry{}, ErrNotFound
	}
	e := m.entries[off-m.firstOffset]
	return Entry{Offset: e.Offset, Envelope: proto.Clone(e.Envelope).(*pb.Envelope)}, nil
}

// Range iterates entries in [from, to).
func (m *Memory) Range(_ context.Context, from, to Offset) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		m.mu.RLock()
		if m.closed {
			m.mu.RUnlock()
			yield(Entry{}, ErrClosed)
			return
		}
		// Snapshot the relevant range to avoid holding the lock
		// across yield, which could deadlock if the consumer calls
		// back into the log.
		first := m.firstOffset
		next := first + Offset(len(m.entries))
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
				e := m.entries[i-first]
				snapshot = append(snapshot, Entry{Offset: e.Offset, Envelope: proto.Clone(e.Envelope).(*pb.Envelope)})
			}
		}
		m.mu.RUnlock()
		for _, e := range snapshot {
			if !yield(e, nil) {
				return
			}
		}
	}
}

// FirstOffset returns the current lowest readable offset.
func (m *Memory) FirstOffset() Offset {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.firstOffset
}

// NextOffset returns the offset the next Append will use.
func (m *Memory) NextOffset() Offset {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.firstOffset + Offset(len(m.entries))
}

// Truncate drops entries with offset < belowOffset.
func (m *Memory) Truncate(_ context.Context, belowOffset Offset) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if belowOffset <= m.firstOffset {
		return nil
	}
	maxFirst := m.firstOffset + Offset(len(m.entries))
	if belowOffset > maxFirst {
		belowOffset = maxFirst
	}
	drop := int(belowOffset - m.firstOffset)
	// Drop index entries that point below belowOffset. Iterating the
	// index by offset (rather than recomputing keys from envelopes)
	// avoids re-deriving senderSeq, which the log can't extract from
	// envelope.id.vector_clock without slot info.
	for k, off := range m.index {
		if off < belowOffset {
			delete(m.index, k)
		}
	}
	m.entries = m.entries[drop:]
	m.firstOffset = belowOffset
	return nil
}

// Close releases the log.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.entries = nil
	m.index = nil
	return nil
}
