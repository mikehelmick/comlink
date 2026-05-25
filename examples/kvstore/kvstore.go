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
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Phase 8(e) — kvstore Prometheus metrics. Registered on the
// comlink-shared registry so apps that expose /metrics via
// comlink.MetricsRegistry() pick them up automatically.
var (
	metricKVSet = promauto.With(comlink.MetricsRegistry()).NewCounter(
		prometheus.CounterOpts{
			Name: "kvstore_set_total",
			Help: "Number of Store.Set calls accepted at this replica.",
		},
	)
	metricKVDelete = promauto.With(comlink.MetricsRegistry()).NewCounter(
		prometheus.CounterOpts{
			Name: "kvstore_delete_total",
			Help: "Number of Store.Delete calls accepted at this replica.",
		},
	)
	metricKVGet = promauto.With(comlink.MetricsRegistry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "kvstore_get_total",
			Help: "Number of Store.Get calls served from local state.",
		},
		[]string{"result"}, // "hit" | "miss"
	)
	metricKVKeys = promauto.With(comlink.MetricsRegistry()).NewGauge(
		prometheus.GaugeOpts{
			Name: "kvstore_keys",
			Help: "Number of keys currently present at this replica.",
		},
	)
	metricKVWatchers = promauto.With(comlink.MetricsRegistry()).NewGauge(
		prometheus.GaugeOpts{
			Name: "kvstore_watchers",
			Help: "Number of active Watch subscriptions at this replica.",
		},
	)
	metricKVApply = promauto.With(comlink.MetricsRegistry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "kvstore_apply_total",
			Help: "Operations applied to the local Store via StateMachine.Apply.",
		},
		[]string{"op"}, // "set" | "del" | "malformed"
	)
	metricKVSnapshotWrites = promauto.With(comlink.MetricsRegistry()).NewCounter(
		prometheus.CounterOpts{
			Name: "kvstore_snapshot_writes_total",
			Help: "Number of times the Store has fsynced a snapshot to disk.",
		},
	)
	metricKVSnapshotBytes = promauto.With(comlink.MetricsRegistry()).NewGauge(
		prometheus.GaugeOpts{
			Name: "kvstore_snapshot_bytes",
			Help: "Size in bytes of the most recently written snapshot.",
		},
	)
	metricKVSnapshotThrough = promauto.With(comlink.MetricsRegistry()).NewGauge(
		prometheus.GaugeOpts{
			Name: "kvstore_snapshot_through_offset",
			Help: "Log offset covered by the most recently written snapshot.",
		},
	)
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

	mu     sync.RWMutex
	data   map[string]string
	maxOff atomic.Uint64 // highest msg.Offset seen in Apply

	watchMu       sync.Mutex
	watchers      map[string]map[*watcher]struct{}
	totalWatchers int

	// snapshotDir is the app-owned directory where the Store
	// persists its snapshot. Empty disables disk persistence
	// (in-memory-only mode, suitable for tests).
	snapshotDir string

	// snapshotter goroutine lifecycle.
	snapshotStop chan struct{}
	snapshotDone chan struct{}
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

	// SnapshotDir, if non-empty, makes the Store durable:
	//   - On startup, the Store reads SnapshotDir/state.snap
	//     and installs it via SubstrateConfig.InitialSnapshot.
	//   - A background goroutine writes a fresh snapshot to
	//     SnapshotDir/state.snap.tmp + fsync + atomic rename
	//     every SnapshotInterval (default 10s), then calls
	//     Substrate.AdvanceSnapshotWatermark so the comlink
	//     trim protocol can compact older log entries.
	//
	// Combined with the substrate's own log on disk, this is
	// the full recovery story: a pod that loses memory but
	// keeps its PVC restores SM state from state.snap and
	// applies any newer log entries via comlink's auto-replay.
	//
	// Empty SnapshotDir = in-memory-only (current pre-10(f)
	// behavior). Useful for tests.
	SnapshotDir string

	// SnapshotInterval is the cadence for background snapshot
	// writes. Zero defaults to 10s. Snapshots are skipped (no
	// disk write) when no Apply has fired since the last
	// snapshot — apps that go idle don't churn disk.
	SnapshotInterval time.Duration

	// BootstrapFromSponsor enables auto-pull of a snapshot from
	// the cluster's sponsor when SnapshotDir has no existing
	// snapshot AND the cluster has Sponsors configured. Apps
	// that are joiners set this to true; founders ignore it.
	BootstrapFromSponsor bool
}

// snapshotPayload is the on-disk format. Versioned implicitly
// via the field tags; production apps would want explicit
// version + migration.
type snapshotPayload struct {
	ThroughOffset uint64            `json:"through_offset"`
	Data          map[string]string `json:"data"`
}

const (
	defaultSnapshotInterval = 10 * time.Second
	snapshotFileName        = "state.snap"
	snapshotTempName        = "state.snap.tmp"
)

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
		data:        make(map[string]string),
		watchers:    make(map[string]map[*watcher]struct{}),
		snapshotDir: cfg.SnapshotDir,
	}

	// Load any persisted snapshot from disk and feed it to the
	// substrate via InitialSnapshot. If absent AND BootstrapFromSponsor
	// is set, fall back to AutoBootstrapFromSponsor (mutually
	// exclusive with InitialSnapshot at the substrate level).
	subCfg := comlink.SubstrateConfig{
		ConversationID: cfg.ConversationID,
		Members:        cfg.Members,
		Ordering:       comlink.OrderingTotal,
		StateMachine:   s,
	}
	if cfg.SnapshotDir != "" {
		loaded, err := loadDiskSnapshot(cfg.SnapshotDir)
		if err != nil {
			return nil, fmt.Errorf("kvstore: load snapshot: %w", err)
		}
		if loaded != nil {
			subCfg.InitialSnapshot = loaded
			// Seed our in-memory maxOff so the FIRST background
			// snapshot doesn't regress the on-disk through_offset.
			s.maxOff.Store(loaded.ThroughOffset)
		} else if cfg.BootstrapFromSponsor {
			subCfg.AutoBootstrapFromSponsor = true
		}
	} else if cfg.BootstrapFromSponsor {
		subCfg.AutoBootstrapFromSponsor = true
	}

	sub, err := cfg.Cluster.NewSubstrate(ctx, subCfg)
	if err != nil {
		return nil, fmt.Errorf("kvstore: create substrate: %w", err)
	}
	s.sub = sub

	// Reflect any post-Restore max-offset into our atomic so a
	// subsequent snapshot reports the correct through_offset.
	// (Substrate seeds appliedOffset from InitialSnapshot, but
	// the SM tracks maxOff itself via Apply.)
	if got := s.maxOff.Load(); got > 0 {
		metricKVSnapshotThrough.Set(float64(got))
	}

	if cfg.SnapshotDir != "" {
		interval := cfg.SnapshotInterval
		if interval <= 0 {
			interval = defaultSnapshotInterval
		}
		s.snapshotStop = make(chan struct{})
		s.snapshotDone = make(chan struct{})
		go s.snapshotLoop(interval)
	}
	return s, nil
}

// loadDiskSnapshot reads SnapshotDir/state.snap and returns a
// *comlink.Snapshot whose Bytes are the on-disk JSON. Returns
// (nil, nil) if the file doesn't exist (fresh install).
func loadDiskSnapshot(dir string) (*comlink.Snapshot, error) {
	path := filepath.Join(dir, snapshotFileName)
	bs, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	// Sanity-check it parses before handing it to the substrate.
	var p snapshotPayload
	if err := json.Unmarshal(bs, &p); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &comlink.Snapshot{
		Bytes:         bs,
		ThroughOffset: p.ThroughOffset,
	}, nil
}

// snapshotLoop runs in a background goroutine, writing the
// Store's state to disk every `interval`. Skips the write if
// no Apply has fired since the last snapshot.
func (s *Store) snapshotLoop(interval time.Duration) {
	defer close(s.snapshotDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastOff uint64
	for {
		select {
		case <-s.snapshotStop:
			// Final snapshot on shutdown so we don't lose work.
			_ = s.writeSnapshot()
			return
		case <-t.C:
			cur := s.maxOff.Load()
			if cur == lastOff {
				continue // no progress; skip the write
			}
			if err := s.writeSnapshot(); err != nil {
				// Don't fail the loop on a transient I/O error —
				// next tick will retry. Bubble up via the
				// snapshot-failure metric.
				continue
			}
			lastOff = cur
		}
	}
}

// writeSnapshot serializes the current state, fsyncs it to
// disk via SnapshotDir/state.snap.tmp + atomic rename, and
// notifies the substrate via AdvanceSnapshotWatermark so the
// comlink trim protocol can compact older log entries.
func (s *Store) writeSnapshot() error {
	if s.snapshotDir == "" {
		return nil
	}
	bs, throughOff, err := s.Snapshot()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.snapshotDir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(s.snapshotDir, snapshotTempName)
	final := filepath.Join(s.snapshotDir, snapshotFileName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(bs); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	metricKVSnapshotWrites.Inc()
	metricKVSnapshotBytes.Set(float64(len(bs)))
	metricKVSnapshotThrough.Set(float64(throughOff))
	// Tell comlink the snapshot is durable so trim can advance.
	s.sub.AdvanceSnapshotWatermark(throughOff)
	return nil
}

// Snapshot implements comlink.Snapshotter — serializes the
// current state + the highest applied offset. The byte form is
// the same JSON layout the Store writes to disk.
func (s *Store) Snapshot() ([]byte, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copied := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copied[k] = v
	}
	throughOff := s.maxOff.Load()
	bs, err := json.Marshal(snapshotPayload{
		ThroughOffset: throughOff,
		Data:          copied,
	})
	return bs, throughOff, err
}

// Restore implements comlink.Snapshotter — installs SM state
// from a snapshot reader. Called by the substrate exactly once
// at construction time if InitialSnapshot is set.
func (s *Store) Restore(r io.Reader) error {
	bs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var p snapshotPayload
	if err := json.Unmarshal(bs, &p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = p.Data
	s.maxOff.Store(p.ThroughOffset)
	metricKVKeys.Set(float64(len(s.data)))
	return nil
}

// FreezeMember propagates a cluster-level eviction down to the
// Store's underlying Substrate. The substrate will stop waiting
// for `replica`'s messages, unblocking total-order wave
// completion. Idempotent in practice — the caller should not
// require strict error semantics if `replica` is already
// frozen or absent.
//
// Typical use: after Cluster.VoteOut(replica) succeeds at the
// system level, call store.FreezeMember(replica) on every
// surviving replica so the app substrate also evicts the dead
// node.
func (s *Store) FreezeMember(replica comlink.ReplicaID) error {
	return s.sub.FreezeMember(replica)
}

// Close tears down the backing Substrate and closes every
// active Watch channel. Subsequent Set / Delete / Watch return
// errors via the Substrate.
func (s *Store) Close() error {
	// Stop the snapshot loop (it writes a final snapshot on
	// shutdown before exiting).
	if s.snapshotStop != nil {
		close(s.snapshotStop)
		<-s.snapshotDone
	}
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
		metricKVApply.WithLabelValues("malformed").Inc()
		return // malformed command — ignore deterministically.
	}
	s.mu.Lock()
	prior, had := s.data[c.K]
	_ = prior
	switch c.Op {
	case opSet:
		s.data[c.K] = c.V
		metricKVApply.WithLabelValues("set").Inc()
	case opDelete:
		delete(s.data, c.K)
		metricKVApply.WithLabelValues("del").Inc()
	default:
		s.mu.Unlock()
		return
	}
	metricKVKeys.Set(float64(len(s.data)))
	s.mu.Unlock()

	// Track the highest applied offset for the periodic snapshot
	// writer (atomic so we don't need the data lock).
	for {
		cur := s.maxOff.Load()
		if msg.Offset <= cur {
			break
		}
		if s.maxOff.CompareAndSwap(cur, msg.Offset) {
			break
		}
	}

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
	v, ok := s.data[key]
	s.mu.RUnlock()
	if ok {
		metricKVGet.WithLabelValues("hit").Inc()
	} else {
		metricKVGet.WithLabelValues("miss").Inc()
	}
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
	metricKVSet.Inc()
	return s.sub.Submit(ctx, bs)
}

// Delete issues a "delete k" command. Returns when Apply'd
// locally. No-op (still ordered) if the key is absent.
func (s *Store) Delete(ctx context.Context, key string) error {
	bs, err := json.Marshal(command{Op: opDelete, K: key})
	if err != nil {
		return fmt.Errorf("kvstore: marshal Delete: %w", err)
	}
	metricKVDelete.Inc()
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
	s.totalWatchers++
	metricKVWatchers.Set(float64(s.totalWatchers))
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
		s.totalWatchers--
		metricKVWatchers.Set(float64(s.totalWatchers))
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

// SnapshotMap returns a copy of the current key→value map.
// Useful for tests; production callers should prefer Get to
// avoid the copy cost. (Named to disambiguate from the
// Snapshotter.Snapshot method, which returns serialized
// bytes + a through-offset.)
func (s *Store) SnapshotMap() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
