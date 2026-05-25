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

// MetadataRegistry (Phase 11(b)) is a replicated map[string]ConvInfo
// built on top of Cluster.SubmitMetadata + MetadataMessages.
// Apps use it to coordinate the existence and member-assignment
// of application conversations across the cluster — the
// canonical "conversation registry" pattern from the developer
// guide.
//
// Consistency: eventually-consistent through the system conv's
// natural causal order. Two replicas Registering the same name
// concurrently resolve to last-writer-wins where "last" is
// defined by the system conv's delivery order. Apps that need
// stricter coordination should add their own concurrency
// control (lease tokens, etc) on top.
//
// Persistence: the registry's state lives only in memory. On
// process restart, the registry rebuilds itself by re-reading
// the system conv's log up to the current trim watermark. Apps
// that need durable registry state beyond the trim horizon
// should snapshot their registry state themselves (e.g., to
// stable.Storage) — the library doesn't drive this today.
//
// Concurrency: all public methods are safe for concurrent use.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ConvInfo is the per-conversation entry replicated through the
// registry. The fields are advisory: apps decide what they mean
// and how strictly they're enforced. The library uses none of
// them — it just replicates the entries through the metadata
// channel.
type ConvInfo struct {
	// Name is the registry key. App-chosen, must be non-empty.
	Name string `json:"name"`
	// Conv is the ConversationID the app uses when creating a
	// Substrate against this entry.
	Conv ConversationID `json:"conv"`
	// Members is the intended substrate membership. Apps that
	// host substrates locally consult this when calling
	// NewSubstrate.
	Members []ReplicaID `json:"members"`
	// CreatedUnixNanos is the originating replica's wall clock
	// at Register time. Informational only — do NOT depend on
	// it for ordering (use causal delivery order instead).
	CreatedUnixNanos int64 `json:"created_unix_nanos"`
	// Properties are app-defined annotations (tenant id,
	// schema version, etc). Opaque to comlink.
	Properties map[string]string `json:"properties,omitempty"`
}

// RegistryEventKind enumerates the kinds of changes a Watch
// channel receives.
type RegistryEventKind int

const (
	// RegistryEventRegistered fires when a Register message is
	// applied (either a new entry or an overwrite of an
	// existing one — apps can compare Info against any prior
	// Get to distinguish).
	RegistryEventRegistered RegistryEventKind = iota
	// RegistryEventUnregistered fires when an Unregister message
	// is applied.
	RegistryEventUnregistered
)

// RegistryEvent is one entry in a Watch channel.
type RegistryEvent struct {
	Kind RegistryEventKind
	Name string
	// Info is the new state (RegistryEventRegistered) or the
	// state immediately before the unregister
	// (RegistryEventUnregistered). Zero-value if not known.
	Info ConvInfo
}

// MetadataRegistry is the replicated registry.
type MetadataRegistry struct {
	cluster *Cluster

	mu      sync.RWMutex
	entries map[string]ConvInfo

	watchMu       sync.Mutex
	watchers      map[*regWatcher]struct{}
	totalWatchers int

	pumpDone chan struct{}
}

const registryWatcherBuffer = 64

type regWatcher struct {
	ch chan RegistryEvent
}

// registryOp is the wire format for a single registry mutation.
type registryOp struct {
	Op   string   `json:"op"` // "register" | "unregister"
	Info ConvInfo `json:"info,omitempty"`
	Name string   `json:"name,omitempty"`
}

// NewMetadataRegistry constructs a registry bound to cluster.
// Starts a background goroutine that consumes
// cluster.MetadataMessages() and applies each registry op to
// local state.
//
// The registry's local state is initially empty — it will
// rebuild by replaying system-conv app messages that arrive
// after construction. Apps that need historical state across
// restarts should consult Phase 11(c)+ snapshot/persistence
// patterns (TBD) OR call Register again on every startup with
// the "intended" entries (the LWW semantics make this safe).
func NewMetadataRegistry(cluster *Cluster) *MetadataRegistry {
	r := &MetadataRegistry{
		cluster:  cluster,
		entries:  make(map[string]ConvInfo),
		watchers: make(map[*regWatcher]struct{}),
		pumpDone: make(chan struct{}),
	}
	go r.pump()
	return r
}

// pump runs in a background goroutine, draining the cluster's
// metadata channel and applying registry ops. Exits when the
// metadata channel closes (which happens at Cluster.Close).
func (r *MetadataRegistry) pump() {
	defer close(r.pumpDone)
	for m := range r.cluster.MetadataMessages() {
		r.applyMessage(m)
	}
}

func (r *MetadataRegistry) applyMessage(m MetadataMessage) {
	var op registryOp
	if err := json.Unmarshal(m.Payload, &op); err != nil {
		// Not a registry op — silently ignore. Other apps may
		// share the metadata channel.
		return
	}
	switch op.Op {
	case "register":
		if op.Info.Name == "" {
			return
		}
		r.mu.Lock()
		r.entries[op.Info.Name] = op.Info
		r.mu.Unlock()
		r.notify(RegistryEvent{
			Kind: RegistryEventRegistered,
			Name: op.Info.Name,
			Info: op.Info,
		})
	case "unregister":
		if op.Name == "" {
			return
		}
		r.mu.Lock()
		prior, had := r.entries[op.Name]
		if had {
			delete(r.entries, op.Name)
		}
		r.mu.Unlock()
		if had {
			r.notify(RegistryEvent{
				Kind: RegistryEventUnregistered,
				Name: op.Name,
				Info: prior,
			})
		}
	}
}

// Register publishes info to the cluster. Every member's
// registry will eventually have entries[info.Name] = info.
//
// Sets info.CreatedUnixNanos to now() if zero. Otherwise the
// caller's value is preserved (useful for test determinism).
//
// Fire-and-forget at the network level. Returns when the
// underlying SubmitMetadata returns; local apply happens
// asynchronously via the pump goroutine. Callers that need
// "wait until locally applied" semantics can Watch the channel
// and look for the matching event.
func (r *MetadataRegistry) Register(ctx context.Context, info ConvInfo) error {
	if info.Name == "" {
		return errors.New("comlink: MetadataRegistry.Register: Info.Name is required")
	}
	if len(info.Conv) == 0 {
		return errors.New("comlink: MetadataRegistry.Register: Info.Conv is required")
	}
	if info.CreatedUnixNanos == 0 {
		info.CreatedUnixNanos = time.Now().UnixNano()
	}
	bs, err := json.Marshal(registryOp{Op: "register", Info: info})
	if err != nil {
		return fmt.Errorf("comlink: registry marshal: %w", err)
	}
	return r.cluster.SubmitMetadata(ctx, bs)
}

// Unregister removes name from the cluster's registry. No-op
// (no error) if the name isn't present.
func (r *MetadataRegistry) Unregister(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("comlink: MetadataRegistry.Unregister: name is required")
	}
	bs, err := json.Marshal(registryOp{Op: "unregister", Name: name})
	if err != nil {
		return fmt.Errorf("comlink: registry marshal: %w", err)
	}
	return r.cluster.SubmitMetadata(ctx, bs)
}

// Get returns the entry for name, or (zero, false) if absent.
// Local read; reflects state Apply'd at this replica so far.
func (r *MetadataRegistry) Get(name string) (ConvInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.entries[name]
	return info, ok
}

// List returns a snapshot of every entry. Order is unspecified.
func (r *MetadataRegistry) List() []ConvInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ConvInfo, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	return out
}

// Watch returns a channel that receives RegistryEvent values
// for every Register / Unregister applied at this replica. The
// returned cancel function unsubscribes and closes the channel.
//
// Buffer is small (64); a slow consumer drops oldest events.
// Final state is always recoverable via Get / List.
func (r *MetadataRegistry) Watch() (<-chan RegistryEvent, func()) {
	w := &regWatcher{ch: make(chan RegistryEvent, registryWatcherBuffer)}
	r.watchMu.Lock()
	r.watchers[w] = struct{}{}
	r.totalWatchers++
	r.watchMu.Unlock()
	cancel := func() {
		r.watchMu.Lock()
		defer r.watchMu.Unlock()
		if _, present := r.watchers[w]; !present {
			return
		}
		delete(r.watchers, w)
		r.totalWatchers--
		close(w.ch)
	}
	return w.ch, cancel
}

func (r *MetadataRegistry) notify(e RegistryEvent) {
	r.watchMu.Lock()
	if len(r.watchers) == 0 {
		r.watchMu.Unlock()
		return
	}
	// Snapshot watchers under the lock so a cancel call from
	// inside a watcher's goroutine doesn't deadlock.
	ws := make([]*regWatcher, 0, len(r.watchers))
	for w := range r.watchers {
		ws = append(ws, w)
	}
	r.watchMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- e:
		default:
			// Buffer full — drop oldest, push newest.
			select {
			case <-w.ch:
			default:
			}
			select {
			case w.ch <- e:
			default:
			}
		}
	}
}

// Close tears down the registry. After Close, Watch channels
// remain open (callers cancel them) but no new events arrive.
// Idempotent.
func (r *MetadataRegistry) Close() error {
	// The pump exits when the cluster's metadata channel
	// closes (Cluster.Close path). Nothing for us to do
	// explicitly — but provide the method for API symmetry and
	// future expansion (persistent snapshotting, etc).
	return nil
}
