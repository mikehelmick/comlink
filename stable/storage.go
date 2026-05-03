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

// Package stable provides a small persistent key/value abstraction
// used for non-message state that must survive a process restart:
// the conversation ID, replica ID, membership list snapshot, view
// checkpoints, replica state snapshots, and the Psync mask state per
// PLAN §2.8 and §5.2.
//
// Stable is deliberately separate from log.MessageLog. Log holds the
// ordered append-only message stream; Storage holds everything else.
// They may share an on-disk directory but the abstractions are
// independent.
//
// The interface exposes one-key-at-a-time Get/Put/Delete with durable
// (fsync) semantics on Put. Multi-key transactions are not in scope
// for v1; if a higher layer needs an atomic multi-key update it
// should encode the group into a single value.
package stable

import (
	"context"
	"errors"
	"regexp"
)

// ErrNotFound is returned by Get and Delete when no value is stored
// under the given key.
var ErrNotFound = errors.New("stable: key not found")

// ErrInvalidKey is returned for keys that violate keyPattern.
var ErrInvalidKey = errors.New("stable: invalid key")

// keyPattern restricts keys to a safe charset so they can be used
// directly as filesystem names by file-backed implementations.
var keyPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,255}$`)

// ValidKey reports whether key is permitted.
func ValidKey(key string) bool { return keyPattern.MatchString(key) }

// Storage is a durable key/value store.
//
// Implementations must be safe for concurrent use. A successful Put
// must guarantee the value survives a process kill (fsync semantics
// on disk-backed impls).
type Storage interface {
	// Get returns the value stored under key, or ErrNotFound if no
	// value is present.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put atomically and durably stores value under key, replacing
	// any prior value.
	Put(ctx context.Context, key string, value []byte) error
	// Delete removes key. Returns ErrNotFound if no value was present.
	Delete(ctx context.Context, key string) error
	// Close releases resources. Subsequent calls return errors.
	Close() error
}
