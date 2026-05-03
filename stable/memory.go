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

package stable

import (
	"context"
	"errors"
	"slices"
	"sync"
)

// errClosed is returned by methods on a closed Memory storage.
var errClosed = errors.New("stable: storage closed")

// Memory is an in-process Storage backed by a map. Useful for tests.
// "Durability" is by definition not provided — the data lives only in
// the process.
type Memory struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemory returns an empty Memory storage.
func NewMemory() *Memory {
	return &Memory{data: make(map[string][]byte)}
}

// Get returns a copy of the stored value.
func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	if !ValidKey(key) {
		return nil, ErrInvalidKey
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errClosed
	}
	v, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return slices.Clone(v), nil
}

// Put stores a copy of value under key.
func (m *Memory) Put(_ context.Context, key string, value []byte) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errClosed
	}
	m.data[key] = slices.Clone(value)
	return nil
}

// Delete removes key.
func (m *Memory) Delete(_ context.Context, key string) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errClosed
	}
	if _, ok := m.data[key]; !ok {
		return ErrNotFound
	}
	delete(m.data, key)
	return nil
}

// Close clears the map and marks the storage closed.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = nil
	m.closed = true
	return nil
}
