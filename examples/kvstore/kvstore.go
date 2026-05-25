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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	kvpb "github.com/mikehelmick/comlink/internal/pb/kvstore/v1"
	"google.golang.org/protobuf/proto"
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
	metricKVBatchFlushOps = promauto.With(comlink.MetricsRegistry()).NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kvstore_batch_flush_ops",
			Help:    "Number of commands in each Submit'd batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		},
	)
	metricKVBatchFlushBytes = promauto.With(comlink.MetricsRegistry()).NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kvstore_batch_flush_bytes",
			Help:    "Marshaled size in bytes of each Submit'd batch.",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 8),
		},
	)
)

// ─── command schema (proto kvstore.v1.Command) ──────────────────
//
// On-the-wire mutations are protobuf (kvpb.Command). Wire-format
// previously was JSON; protobuf cut both encode time and per-
// message bytes substantially, which directly relieves substrate
// genserver contention under heavy concurrent writers.

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

	// Batcher — coalesces concurrent Set/Delete calls into one
	// substrate Submit. Nil when Config.Batching.Disabled=true.
	batcher *batcher
}

// batchEntry is one queued mutation waiting for the batch loop
// to flush. done is closed (with err set) when the batch this
// entry was bundled into has finished its Submit call.
type batchEntry struct {
	cmd  *kvpb.Command
	bytes int // approx wire size for byte-trigger accounting
	done chan struct{}
	err  error
}

// batcher owns the per-Store flush loop. One per Store.
type batcher struct {
	sub *comlink.Substrate
	cfg BatchingConfig

	in       chan *batchEntry
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

func newBatcher(sub *comlink.Substrate, cfg BatchingConfig) *batcher {
	// Fill in defaults. Zero values picked to be sensible for
	// a 3-replica OrderingTotal substrate.
	if cfg.MinWindow <= 0 {
		cfg.MinWindow = 1 * time.Millisecond
	}
	if cfg.MaxWindow <= 0 {
		cfg.MaxWindow = 50 * time.Millisecond
	}
	if cfg.MaxBatchOps <= 0 {
		cfg.MaxBatchOps = 256
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = 4 << 20 // 4 MiB
	}
	if cfg.RateHalfLife <= 0 {
		cfg.RateHalfLife = 500 * time.Millisecond
	}
	b := &batcher{
		sub:  sub,
		cfg:  cfg,
		in:   make(chan *batchEntry, 1024),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go b.loop()
	return b
}

// submit enqueues a Command, waits for the batch to flush, and
// returns the per-batch Submit error.
func (b *batcher) submit(ctx context.Context, c *kvpb.Command, approxBytes int) error {
	e := &batchEntry{
		cmd:   c,
		bytes: approxBytes,
		done:  make(chan struct{}),
	}
	select {
	case b.in <- e:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return errors.New("kvstore: batcher closed")
	}
	select {
	case <-e.done:
		return e.err
	case <-ctx.Done():
		// Note: even if ctx fires, the entry may still get
		// flushed (we don't / can't pull it back out of the
		// queue). The caller-visible result honors the ctx
		// deadline; the batch may still Submit.
		return ctx.Err()
	}
}

// loop is the single-goroutine flush loop using a drain-then-
// flush model:
//
//   - Block on the first arrival.
//   - Greedily drain any other entries already queued (non-
//     blocking reads from in) up to MaxBatchOps / MaxBatchBytes.
//   - Flush.
//
// Under sustained load, while the loop is blocked inside
// Submit (waiting for local Apply), concurrent callers queue
// entries in b.in. The next loop iteration reads them all at
// once and flushes them as a single batch — so batch size
// scales naturally with concurrency × Submit latency, without
// adding any artificial time-window delay.
//
// Under light load, batches are 1-element (no concurrent
// arrivals to drain). That's fine: the only "cost" of going
// through the batcher in that case is one extra channel hop.
func (b *batcher) loop() {
	defer close(b.done)

	// Submits get a context that's cancelled when b.stop fires,
	// so a wedged Submit at shutdown returns instead of hanging
	// the Close path.
	submitCtx, submitCancel := context.WithCancel(context.Background())
	defer submitCancel()
	go func() {
		<-b.stop
		submitCancel()
	}()

	pending := make([]*batchEntry, 0, b.cfg.MaxBatchOps)
	var batchBytes int

	resetBatch := func() {
		pending = pending[:0]
		batchBytes = 0
	}

	abortPending := func(err error) {
		for _, e := range pending {
			e.err = err
			close(e.done)
		}
		resetBatch()
	}

	flush := func() {
		if len(pending) == 0 {
			return
		}
		commands := make([]*kvpb.Command, len(pending))
		for i, e := range pending {
			commands[i] = e.cmd
		}
		payload, err := proto.Marshal(&kvpb.CommandBatch{Commands: commands})
		if err == nil {
			err = b.sub.Submit(submitCtx, payload)
			metricKVBatchFlushOps.Observe(float64(len(commands)))
			metricKVBatchFlushBytes.Observe(float64(len(payload)))
		}
		for _, e := range pending {
			e.err = err
			close(e.done)
		}
		resetBatch()
	}

	// drainAvailable greedily reads everything currently queued
	// in b.in (non-blocking) without exceeding the size caps.
	// Returns when b.in has nothing more to give OR when a
	// cap is hit.
	drainAvailable := func() {
		for {
			if len(pending) >= b.cfg.MaxBatchOps || batchBytes >= b.cfg.MaxBatchBytes {
				return
			}
			select {
			case e, ok := <-b.in:
				if !ok {
					return
				}
				pending = append(pending, e)
				batchBytes += e.bytes
			default:
				return
			}
		}
	}

	for {
		select {
		case <-b.stop:
			abortPending(errors.New("kvstore: batcher closed"))
			return

		case e := <-b.in:
			pending = append(pending, e)
			batchBytes += e.bytes
			// Greedily absorb anything else already queued —
			// these are concurrent callers blocked on the
			// previous flush, so we get a "natural" batch with
			// zero added latency.
			drainAvailable()
			flush()
		}
	}
}

// Close stops the batcher loop. Draining of in-flight entries
// happens via the final flush() call in the loop's stop path.
// Idempotent — multiple Close calls are safe.
func (b *batcher) Close() error {
	b.stopOnce.Do(func() {
		close(b.stop)
		<-b.done
	})
	return nil
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

	// Batching coalesces concurrent Set/Delete calls into one
	// substrate message. Zero values pick defaults appropriate
	// for a 3-replica OrderingTotal substrate where each Submit
	// costs one wave-gate roundtrip. Set Batching.Disabled=true
	// to bypass entirely (every Set/Delete becomes its own
	// 1-element batch posted directly).
	Batching BatchingConfig
}

// BatchingConfig tunes the application-level write batcher.
//
// The batcher accumulates concurrent Set/Delete calls into one
// kvpb.CommandBatch and Submits that batch to the substrate
// once. Each caller blocks until the batch is locally Apply'd
// (same Set/Delete semantics as before — what changes is the
// number of substrate messages, not the per-call contract).
//
// The flush window adapts to incoming arrival rate: under
// heavy load, the batch fills quickly so the time-trigger
// shrinks toward MinWindow; under light load, time stretches
// toward MaxWindow so a stream of one-off writes still bundles
// well. The size + byte triggers cap latency from blowing up
// regardless of arrival rate.
type BatchingConfig struct {
	// Disabled bypasses the batcher entirely. Every Set/Delete
	// posts its own one-element batch directly. Useful for
	// debugging / measuring the no-batching baseline without
	// having to rebuild.
	Disabled bool

	// MinWindow is the floor for the adaptive time trigger.
	// Even under saturating load the batcher waits at least
	// this long after the first queued entry before flushing,
	// so writes get a chance to accumulate. Zero = 1 ms.
	MinWindow time.Duration

	// MaxWindow is the ceiling for the adaptive time trigger.
	// A burst of one-off writes flushes no later than this
	// after the first queued entry. Zero = 50 ms.
	MaxWindow time.Duration

	// MaxBatchOps flushes the batch when the queue reaches this
	// many ops, regardless of time. Zero = 256.
	MaxBatchOps int

	// MaxBatchBytes flushes the batch when the cumulative
	// value-bytes reach this size. Caps a single substrate
	// payload's size so the apply pump doesn't choke on a
	// pathological 100-MiB batch. Zero = 4 MiB.
	MaxBatchBytes int

	// RateHalfLife is kept for API stability — currently unused
	// by the drain-then-flush loop. Reserved for a future
	// adaptive variant.
	RateHalfLife time.Duration
}

// On-disk format: protobuf-encoded kvpb.Snapshot. The file is
// named state.snap.pb so it can't be confused with the previous
// JSON-encoded state.snap from earlier versions; mixing them
// would silently produce a parse error at startup.
const (
	defaultSnapshotInterval = 10 * time.Second
	snapshotFileName        = "state.snap.pb"
	snapshotTempName        = "state.snap.pb.tmp"
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

	// Spin up the batcher unless explicitly disabled. With
	// batching off, Set/Delete fall back to direct one-element
	// substrate Submits (kept on the same code path for parity).
	if !cfg.Batching.Disabled {
		s.batcher = newBatcher(sub, cfg.Batching)
	}

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

// loadDiskSnapshot reads SnapshotDir/state.snap.pb and returns
// a *comlink.Snapshot whose Bytes are the on-disk protobuf.
// Returns (nil, nil) if the file doesn't exist (fresh install).
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
	var p kvpb.Snapshot
	if err := proto.Unmarshal(bs, &p); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &comlink.Snapshot{
		Bytes:         bs,
		ThroughOffset: p.GetThroughOffset(),
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
// kvpb.Snapshot (protobuf).
//
// IMPORTANT: the data lock is released BEFORE the
// proto.Marshal so the (still nontrivial) serialization of
// large state maps doesn't block Apply / Set / Delete. Apply
// callers see the post-copy state machine; the marshal sees
// a frozen snapshot of the data at copy time.
func (s *Store) Snapshot() ([]byte, uint64, error) {
	s.mu.RLock()
	entries := make([]*kvpb.SnapshotEntry, 0, len(s.data))
	for k, v := range s.data {
		entries = append(entries, &kvpb.SnapshotEntry{
			Key:   k,
			Value: []byte(v),
		})
	}
	throughOff := s.maxOff.Load()
	s.mu.RUnlock()

	bs, err := proto.Marshal(&kvpb.Snapshot{
		ThroughOffset: throughOff,
		Entries:       entries,
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
	var p kvpb.Snapshot
	if err := proto.Unmarshal(bs, &p); err != nil {
		return err
	}
	rebuilt := make(map[string]string, len(p.GetEntries()))
	for _, e := range p.GetEntries() {
		rebuilt[e.GetKey()] = string(e.GetValue())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = rebuilt
	s.maxOff.Store(p.GetThroughOffset())
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
	// Stop the batcher first so its loop drains any pending
	// entries before we tear down the substrate underneath it.
	if s.batcher != nil {
		_ = s.batcher.Close()
	}
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
//
// Every Apply payload is a kvpb.CommandBatch — single-write
// callers produce 1-element batches, the batcher coalesces
// many. Commands within a batch are applied in proto order
// (which is the order Set/Delete callers were enqueued in on
// the source replica).
func (s *Store) Apply(ctx context.Context, msg *comlink.Message) {
	var batch kvpb.CommandBatch
	if err := proto.Unmarshal(msg.Payload, &batch); err != nil {
		metricKVApply.WithLabelValues("malformed").Inc()
		return // malformed payload — ignore deterministically.
	}
	cmds := batch.GetCommands()
	if len(cmds) == 0 {
		return
	}

	// Hold the data lock once for the whole batch — within a
	// batch the writes are applied as one logical unit. This
	// also amortizes the lock acquire/release across many
	// mutations.
	type evt struct {
		op   kvpb.Op
		key  string
		val  string
		had  bool
	}
	events := make([]evt, 0, len(cmds))
	s.mu.Lock()
	for _, c := range cmds {
		key := c.GetKey()
		val := string(c.GetValue())
		_, had := s.data[key]
		switch c.GetOp() {
		case kvpb.Op_OP_SET:
			s.data[key] = val
			metricKVApply.WithLabelValues("set").Inc()
			events = append(events, evt{kvpb.Op_OP_SET, key, val, had})
		case kvpb.Op_OP_DELETE:
			delete(s.data, key)
			metricKVApply.WithLabelValues("del").Inc()
			events = append(events, evt{kvpb.Op_OP_DELETE, key, "", had})
		default:
			// Unknown op — skip, keep the rest of the batch atomic.
		}
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
	for _, e := range events {
		switch e.op {
		case kvpb.Op_OP_SET:
			s.notify(e.key, Event{Type: EventSet, Key: e.key, Value: e.val, PriorExists: e.had})
		case kvpb.Op_OP_DELETE:
			if e.had {
				s.notify(e.key, Event{Type: EventDelete, Key: e.key, PriorExists: true})
			}
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
//
// Internally: routes through the batcher (unless disabled).
// The batcher coalesces concurrent Set/Delete calls into one
// substrate Submit; the caller's blocking-until-Apply'd
// contract is preserved.
func (s *Store) Set(ctx context.Context, key, value string) error {
	cmd := &kvpb.Command{
		Op:    kvpb.Op_OP_SET,
		Key:   key,
		Value: []byte(value),
	}
	metricKVSet.Inc()
	return s.submitCommand(ctx, cmd, len(key)+len(value)+8)
}

// Delete issues a "delete k" command. Returns when Apply'd
// locally. No-op (still ordered) if the key is absent.
func (s *Store) Delete(ctx context.Context, key string) error {
	cmd := &kvpb.Command{
		Op:  kvpb.Op_OP_DELETE,
		Key: key,
	}
	metricKVDelete.Inc()
	return s.submitCommand(ctx, cmd, len(key)+8)
}

// submitCommand sends one Command to the substrate, either via
// the batcher (if enabled) or as a one-element batch directly.
// Both code paths produce kvpb.CommandBatch payloads on the
// wire so the Apply path is uniform.
func (s *Store) submitCommand(ctx context.Context, c *kvpb.Command, approxBytes int) error {
	if s.batcher != nil {
		return s.batcher.submit(ctx, c, approxBytes)
	}
	bs, err := proto.Marshal(&kvpb.CommandBatch{Commands: []*kvpb.Command{c}})
	if err != nil {
		return fmt.Errorf("kvstore: marshal CommandBatch: %w", err)
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
