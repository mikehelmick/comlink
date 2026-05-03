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

package psync

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/stable"
	"google.golang.org/protobuf/proto"
)

// MaskStorageKey is the stable.Storage key under which Mask
// persists its state. Callers using a single conversation per
// replica can use this default; multi-conversation deployments
// (out of scope for v1, see PLAN §2.9) would namespace it.
const MaskStorageKey = "psync.mask"

// Mask tracks which peer replicas this Psync instance is currently
// ignoring. After Maskout(p), no further messages from p are
// accepted; after Maskin(p), they are again. The state is durably
// persisted to stable.Storage on every change, so it survives a
// process restart (PLAN §5.2).
//
// Phase 1 persists eagerly on every mutation. Phase 4's Recovery
// design may instead persist on checkpoint and treat the mask as
// part of the checkpoint blob; the public methods here are
// compatible with either approach.
//
// Safe for concurrent use.
type Mask struct {
	mu      sync.RWMutex
	storage stable.Storage
	key     string
	masked  map[string]struct{}
}

// LoadMask opens (or initializes) a Mask backed by storage at key.
// If a previously-persisted MaskState exists at key, it is loaded;
// otherwise the mask starts empty.
func LoadMask(ctx context.Context, storage stable.Storage, key string) (*Mask, error) {
	if storage == nil {
		return nil, errors.New("psync: LoadMask: nil storage")
	}
	if key == "" {
		key = MaskStorageKey
	}
	m := &Mask{
		storage: storage,
		key:     key,
		masked:  make(map[string]struct{}),
	}
	data, err := storage.Get(ctx, key)
	switch {
	case errors.Is(err, stable.ErrNotFound):
		// Empty mask, no persisted state yet.
		return m, nil
	case err != nil:
		return nil, err
	}
	state := &pb.MaskState{}
	if err := proto.Unmarshal(data, state); err != nil {
		return nil, err
	}
	for _, r := range state.GetMaskedReplicas() {
		m.masked[string(r)] = struct{}{}
	}
	return m, nil
}

// IsMasked reports whether replica is currently masked out.
func (m *Mask) IsMasked(replica *pb.ReplicaID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.masked[string(replica.GetValue())]
	return ok
}

// Maskout marks replica as masked and persists. Idempotent: masking
// an already-masked replica returns nil without touching storage.
func (m *Mask) Maskout(ctx context.Context, replica *pb.ReplicaID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(replica.GetValue())
	if _, already := m.masked[key]; already {
		return nil
	}
	m.masked[key] = struct{}{}
	if err := m.persistLocked(ctx); err != nil {
		// Roll back the in-memory mutation so storage and memory
		// stay in sync.
		delete(m.masked, key)
		return err
	}
	return nil
}

// Maskin removes replica from the mask and persists. Idempotent.
func (m *Mask) Maskin(ctx context.Context, replica *pb.ReplicaID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(replica.GetValue())
	if _, present := m.masked[key]; !present {
		return nil
	}
	delete(m.masked, key)
	if err := m.persistLocked(ctx); err != nil {
		m.masked[key] = struct{}{}
		return err
	}
	return nil
}

// MaskedReplicas returns a sorted snapshot of the currently-masked
// replica byte values.
func (m *Mask) MaskedReplicas() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([][]byte, 0, len(m.masked))
	for k := range m.masked {
		out = append(out, []byte(k))
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

// persistLocked writes the current mask to stable.Storage. Caller
// must hold m.mu.
func (m *Mask) persistLocked(ctx context.Context) error {
	state := &pb.MaskState{
		MaskedReplicas: make([][]byte, 0, len(m.masked)),
	}
	for k := range m.masked {
		state.MaskedReplicas = append(state.MaskedReplicas, []byte(k))
	}
	slices.SortFunc(state.MaskedReplicas, bytes.Compare)
	data, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	return m.storage.Put(ctx, m.key, data)
}
