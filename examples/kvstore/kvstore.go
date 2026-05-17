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

// Package kvstore is the Phase 6 example replicated key-value
// store built on top of comlink. It demonstrates the "user
// application" boundary: app code defines a state machine and
// command schema, hands the SM to a comlink.Substrate, and gets
// a totally-ordered replicated container with no extra plumbing.
//
// The Store supports the classic etcd-style operations:
//
//	Get(k)       - local read (eventually consistent — see below)
//	Set(ctx, k, v) error
//	Delete(ctx, k) error
//	Watch(k)     - subscribe to mutations on a key
//
// Consistency: writes are totally ordered (OrderingTotal) across
// every replica. Local Get returns whatever has been Apply'd at
// this replica — a peer's freshly-committed Set may not yet have
// propagated, but the order in which writes ARE seen is
// identical on every replica.
//
// Determinism: Apply is pure — it consults only the incoming
// command and the prior state. No time, no I/O, no rand. The
// substrate's determinism-violation test (in the root package)
// catches regressions.
package kvstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/mikehelmick/comlink"
)

// ─── command schema ─────────────────────────────────────────────

type opKind string

const (
	opSet    opKind = "set"
	opDelete opKind = "del"
)

// command is the wire-encoded mutation. JSON keeps the demo
// readable; production apps would use protobuf.
type command struct {
	Op opKind `json:"op"`
	K  string `json:"k"`
	V  string `json:"v,omitempty"`
}

// ─── events (Watch) ─────────────────────────────────────────────

// EventType discriminates Watch event variants.
type EventType int

const (
	// EventSet is fired after a Set is Apply'd. Value is the
	// new value.
	EventSet EventType = iota
	// EventDelete is fired after a Delete is Apply'd. Value is
	// the empty string (the prior value is not retained).
	EventDelete
)

// Event is delivered to Watch subscribers when their key
// changes. PriorExists reflects whether the key was present
// immediately before the mutation (useful for distinguishing
// "Set created" from "Set overwrote").
type Event struct {
	Type        EventType
	Key         string
	Value       string
	PriorExists bool
}

// ─── store ──────────────────────────────────────────────────────

// Store is the public API. Construct via New and tear down via
// Close (which also closes the underlying Substrate).
type Store struct {
	sub *comlink.Substrate

	mu   sync.RWMutex
	data map[string]string

	watchMu  sync.Mutex
	watchers map[string]map[*watcher]struct{}
}

// watcher is the internal handle for one Watch call. Channel
// is buffered so a slow consumer can't stall Apply; oldest
// undelivered event is dropped on overflow.
type watcher struct {
	key string
	ch  chan Event
}

const watcherBufferSize = 64

// Config wires a Store into an existing Cluster. The Store
// constructs its own Substrate against the supplied
// ConversationID + Members; callers should treat the Store as
// the owner of that Substrate and not Submit to it directly.
type Config struct {
	Cluster        *comlink.Cluster
	ConversationID comlink.ConversationID
	Members        []comlink.ReplicaID
}

// New constructs a Store and its backing Substrate. Errors from
// Substrate construction surface here.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Cluster == nil {
		return nil, errors.New("kvstore: Config.Cluster is required")
	}
	if len(cfg.ConversationID) == 0 {
		return nil, errors.New("kvstore: Config.ConversationID is required")
	}
	if len(cfg.Members) == 0 {
		return nil, errors.New("kvstore: Config.Members is required")
	}
	s := &Store{
		data:     make(map[string]string),
		watchers: make(map[string]map[*watcher]struct{}),
	}
	sub, err := cfg.Cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: cfg.ConversationID,
		Members:        cfg.Members,
		Ordering:       comlink.OrderingTotal,
		StateMachine:   s,
	})
	if err != nil {
		return nil, fmt.Errorf("kvstore: create substrate: %w", err)
	}
	s.sub = sub
	return s, nil
}

// Close tears down the backing Substrate and closes every
// active Watch channel. Subsequent Set / Delete / Watch return
// errors via the Substrate.
func (s *Store) Close() error {
	s.watchMu.Lock()
	for _, ws := range s.watchers {
		for w := range ws {
			close(w.ch)
		}
	}
	s.watchers = make(map[string]map[*watcher]struct{})
	s.watchMu.Unlock()
	return s.sub.Close()
}

// Apply implements comlink.StateMachine. Runs on every replica
// in the same total order. Must be pure / deterministic.
func (s *Store) Apply(ctx context.Context, msg *comlink.Message) {
	var c command
	if err := json.Unmarshal(msg.Payload, &c); err != nil {
		return // malformed command — ignore deterministically.
	}
	s.mu.Lock()
	prior, had := s.data[c.K]
	_ = prior
	switch c.Op {
	case opSet:
		s.data[c.K] = c.V
	case opDelete:
		delete(s.data, c.K)
	default:
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Fan out to watchers AFTER releasing the data lock so a
	// slow watcher can't block other Apply calls.
	switch c.Op {
	case opSet:
		s.notify(c.K, Event{Type: EventSet, Key: c.K, Value: c.V, PriorExists: had})
	case opDelete:
		if had {
			s.notify(c.K, Event{Type: EventDelete, Key: c.K, PriorExists: true})
		}
	}
}

// Get returns the value stored under key, or (empty, false) if
// absent. Reads are LOCAL — they reflect whatever has been
// Apply'd at this replica, which may trail a peer's recent Set
// by the network roundtrip + ordering pipeline.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Set issues a "set k=v" command. Returns when the command has
// been Apply'd locally (and is therefore guaranteed to be in
// the global order). Peers will see it shortly after via the
// substrate.
func (s *Store) Set(ctx context.Context, key, value string) error {
	bs, err := json.Marshal(command{Op: opSet, K: key, V: value})
	if err != nil {
		return fmt.Errorf("kvstore: marshal Set: %w", err)
	}
	return s.sub.Submit(ctx, bs)
}

// Delete issues a "delete k" command. Returns when Apply'd
// locally. No-op (still ordered) if the key is absent.
func (s *Store) Delete(ctx context.Context, key string) error {
	bs, err := json.Marshal(command{Op: opDelete, K: key})
	if err != nil {
		return fmt.Errorf("kvstore: marshal Delete: %w", err)
	}
	return s.sub.Submit(ctx, bs)
}

// Watch returns a channel that receives Event values whenever
// key mutates. The returned cancel function unsubscribes and
// closes the channel. Channel buffer is small (64); a slow
// consumer that backs up causes oldest-event drop, NOT Apply
// blocking — keep up or you lose intermediate updates (the
// final state is always recoverable via Get).
func (s *Store) Watch(key string) (<-chan Event, func()) {
	w := &watcher{key: key, ch: make(chan Event, watcherBufferSize)}
	s.watchMu.Lock()
	bucket, ok := s.watchers[key]
	if !ok {
		bucket = make(map[*watcher]struct{})
		s.watchers[key] = bucket
	}
	bucket[w] = struct{}{}
	s.watchMu.Unlock()
	cancel := func() {
		s.watchMu.Lock()
		defer s.watchMu.Unlock()
		bucket, ok := s.watchers[key]
		if !ok {
			return
		}
		if _, present := bucket[w]; !present {
			return
		}
		delete(bucket, w)
		if len(bucket) == 0 {
			delete(s.watchers, key)
		}
		close(w.ch)
	}
	return w.ch, cancel
}

func (s *Store) notify(key string, event Event) {
	s.watchMu.Lock()
	bucket, ok := s.watchers[key]
	if !ok {
		s.watchMu.Unlock()
		return
	}
	// Copy the watcher set so we don't hold watchMu during sends
	// (a watcher cancel could deadlock waiting for the same lock).
	ws := make([]*watcher, 0, len(bucket))
	for w := range bucket {
		ws = append(ws, w)
	}
	s.watchMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- event:
		default:
			// Drop oldest, push newest — keeps Apply unblocked.
			select {
			case <-w.ch:
			default:
			}
			select {
			case w.ch <- event:
			default:
			}
		}
	}
}

// Snapshot returns a copy of the current key→value map. Useful
// for tests; production callers should prefer Get to avoid the
// copy cost.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
