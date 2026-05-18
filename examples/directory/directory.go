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

// Package directory is the Phase 6 example replicated directory
// from Consul §3 of the paper — the canonical demonstration of
// SemOrder. A directory is a name→value mapping that supports
// Insert / Update / Delete; operations on DIFFERENT names commute
// (the paper's "k-class" structure), while operations on the
// SAME name do not (insert-then-update yields a different result
// than update-then-insert).
//
// Wiring this through comlink.OrderingSemOrder means:
//   - Two replicas can concurrently Insert("foo", ...) and
//     Insert("bar", ...) and each replica is free to apply them
//     in either order — the SemOrder layer doesn't force a wave
//     barrier between operations on different names.
//   - Two replicas concurrently Update("foo", ...) get a single
//     deterministic ordering — SemOrder classifies them into the
//     same bucket and totally-orders that bucket.
//
// The classifier is hash(name) → int.
package directory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/order"
)

// ─── command schema ─────────────────────────────────────────────

type opKind string

const (
	opInsert opKind = "ins"
	opUpdate opKind = "upd"
	opDelete opKind = "del"
)

type command struct {
	Op    opKind `json:"op"`
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

// classifyName hashes a name to an int — the SemOrder
// commutativity class. Two ops are in the same class IFF they
// target the same name.
//
// Class 1 is SemOrder's "always commutes" bucket — ops in class
// 1 fire eagerly and replicas may apply them in different
// orders. That's WRONG for our directory: same-name Insert and
// Update don't commute. We map every name into class ≥ 2 to
// force the wave-batched + intra-wave-sorted path, which gives
// us deterministic same-name ordering. The +2 offset is the
// minimal safe shift; the upper bits remain unique per name.
func classifyName(name string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int(h.Sum32()) + 2
}

// classifierForDirectory pulls the name out of an encoded
// command and returns its class. Malformed payloads collapse to
// class 0 (everyone agrees on the same nonsense class).
func classifierForDirectory() order.ClassifierFunc {
	return func(payload []byte) int {
		var c command
		if err := json.Unmarshal(payload, &c); err != nil {
			return 0
		}
		return classifyName(c.Name)
	}
}

// ─── directory state ────────────────────────────────────────────

// Directory is the replicated name→value mapping.
type Directory struct {
	sub *comlink.Substrate

	mu      sync.RWMutex
	entries map[string]string
}

// Config wires the Directory into an existing Cluster. The
// Directory creates its own Substrate with OrderingSemOrder.
type Config struct {
	Cluster        *comlink.Cluster
	ConversationID comlink.ConversationID
	Members        []comlink.ReplicaID
}

// New constructs a Directory and its backing Substrate.
func New(ctx context.Context, cfg Config) (*Directory, error) {
	if cfg.Cluster == nil {
		return nil, errors.New("directory: Config.Cluster is required")
	}
	if len(cfg.ConversationID) == 0 {
		return nil, errors.New("directory: Config.ConversationID is required")
	}
	if len(cfg.Members) == 0 {
		return nil, errors.New("directory: Config.Members is required")
	}
	d := &Directory{entries: make(map[string]string)}
	sub, err := cfg.Cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: cfg.ConversationID,
		Members:        cfg.Members,
		Ordering:       comlink.OrderingSemOrder,
		Classifier:     classifierForDirectory(),
		StateMachine:   d,
	})
	if err != nil {
		return nil, fmt.Errorf("directory: create substrate: %w", err)
	}
	d.sub = sub
	return d, nil
}

// Close tears down the backing Substrate.
func (d *Directory) Close() error {
	return d.sub.Close()
}

// FreezeMember propagates a cluster-level eviction down to the
// Directory's underlying Substrate. See Substrate.FreezeMember
// for semantics.
func (d *Directory) FreezeMember(replica comlink.ReplicaID) error {
	return d.sub.FreezeMember(replica)
}

// ─── Apply (StateMachine) ───────────────────────────────────────

// Apply implements comlink.StateMachine. Pure / deterministic.
func (d *Directory) Apply(ctx context.Context, msg *comlink.Message) {
	var c command
	if err := json.Unmarshal(msg.Payload, &c); err != nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch c.Op {
	case opInsert:
		// Insert is "create if absent" — does NOT overwrite.
		if _, exists := d.entries[c.Name]; exists {
			return
		}
		d.entries[c.Name] = c.Value
	case opUpdate:
		// Update is "overwrite if present" — does NOT create.
		if _, exists := d.entries[c.Name]; !exists {
			return
		}
		d.entries[c.Name] = c.Value
	case opDelete:
		delete(d.entries, c.Name)
	}
}

// ─── public ops ─────────────────────────────────────────────────

// Insert adds (name, value) iff the name is absent. Idempotent
// at the SM level — re-inserting the same name is a no-op.
func (d *Directory) Insert(ctx context.Context, name, value string) error {
	bs, err := json.Marshal(command{Op: opInsert, Name: name, Value: value})
	if err != nil {
		return fmt.Errorf("directory: marshal Insert: %w", err)
	}
	return d.sub.Submit(ctx, bs)
}

// Update overwrites (name, value) iff the name is present.
// No-op if absent.
func (d *Directory) Update(ctx context.Context, name, value string) error {
	bs, err := json.Marshal(command{Op: opUpdate, Name: name, Value: value})
	if err != nil {
		return fmt.Errorf("directory: marshal Update: %w", err)
	}
	return d.sub.Submit(ctx, bs)
}

// Delete removes the entry. No-op if absent.
func (d *Directory) Delete(ctx context.Context, name string) error {
	bs, err := json.Marshal(command{Op: opDelete, Name: name})
	if err != nil {
		return fmt.Errorf("directory: marshal Delete: %w", err)
	}
	return d.sub.Submit(ctx, bs)
}

// Lookup returns the value associated with name (and whether
// it's present). Local read; eventually consistent.
func (d *Directory) Lookup(name string) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	v, ok := d.entries[name]
	return v, ok
}

// Snapshot returns a copy of the full name→value map. Useful
// for tests / convergence checks.
func (d *Directory) Snapshot() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]string, len(d.entries))
	for k, v := range d.entries {
		out[k] = v
	}
	return out
}
