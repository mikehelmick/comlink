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

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/failure"
	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/order"
	"github.com/mikehelmick/comlink/psync"
)

// StateMachine is the application-implemented contract for
// applying ordered commands at every replica (PLAN §5,
// collaborative design pass).
//
// Apply is invoked once per delivered command on this replica
// in the substrate's chosen ordering. It MUST be deterministic:
// same prior state + same Message at every replica yields the
// same post-state. Apply is INFALLIBLE — there is no return
// value and no error to propagate. The application handles its
// own errors internally (logs, drops bad commands, whatever);
// returning an error from Apply isn't possible because divergent
// error handling across replicas would silently break
// replication.
//
// Apply runs in the substrate's apply goroutine, serialized per
// substrate. It must not block for long; long-running work
// should be dispatched off the apply path.
type StateMachine interface {
	Apply(ctx context.Context, msg *Message)
}

// Message is what the substrate hands to StateMachine.Apply.
type Message struct {
	ID      *MessageID
	Payload []byte
	Sender  ReplicaID
	// Offset is the position of this command in this replica's
	// local log. The application records last-applied-offset and
	// can pass it to Substrate.SetWatermark once durable state
	// covers it (PLAN §2.8 trim protocol).
	Offset uint64
	// Wave is psync's wave number for this message
	// (max(vector_clock)). Useful for observability.
	Wave uint64
}

// OrderingKind selects the substrate's ordering policy.
type OrderingKind int

const (
	// OrderingPartial passes psync's natural causal-order
	// delivery directly to the StateMachine. Useful when app
	// commands fully commute or already self-handle ordering.
	OrderingPartial OrderingKind = iota
	// OrderingTotal sorts each wave's messages by sender
	// ReplicaID byte order so every replica sees the same
	// sequence (paper §2.3 / §3 Total).
	OrderingTotal
	// OrderingSemOrder applies the §3 semantic-dependent
	// ordering with k-class commutativity. Requires
	// SubstrateConfig.Classifier.
	OrderingSemOrder
)

// SubstrateConfig configures one application Substrate created
// via Cluster.NewSubstrate.
type SubstrateConfig struct {
	// ConversationID is application-chosen. Use
	// comlink.NewConversationID() to mint a fresh one.
	ConversationID ConversationID

	// Members is the participant set for this substrate. Should
	// be a subset of the parent Cluster's current members.
	Members []ReplicaID

	// Ordering selects the policy for delivering commands to
	// the StateMachine.
	Ordering OrderingKind

	// Classifier is required when Ordering == OrderingSemOrder
	// and ignored otherwise.
	Classifier order.Classifier

	// StateMachine is the application's apply target.
	StateMachine StateMachine

	// Logger; defaults to the parent Cluster's logger.
	Logger *slog.Logger
	// Clock; defaults to the parent Cluster's clock.
	Clock clock.Clock

	// InitialSnapshot, when non-nil, is installed via the SM's
	// Restore method before the apply pump starts. Apply for
	// messages whose Offset is <= InitialSnapshot.ThroughOffset
	// is suppressed (already covered by the snapshot). Requires
	// StateMachine to implement Snapshotter; if it doesn't, the
	// config is rejected at NewSubstrate.
	//
	// Phase 10(b): apps load this from their own persistent
	// storage on startup. Phase 10(d) extends sponsor handshake
	// to deliver a snapshot for joiners.
	InitialSnapshot *Snapshot

	// AutoEvict, when non-nil, enables substrate-level failure
	// detection: if a peer's heartbeats stop for longer than
	// SuspicionInterval, the substrate freezes that peer's slot
	// (Substrate.FreezeMember) so the Order layer's wave gates
	// can continue to make progress without waiting for the dead
	// peer. Eviction is permanent for the lifetime of this
	// Substrate — a re-joining replica would need to come back
	// via a fresh Substrate (e.g. through Cluster.VoteIn +
	// substrate re-creation).
	//
	// nil (default) preserves the historical behavior: the
	// substrate's heartbeats fire but suspicion is effectively
	// disabled, and Submits block on wave completion across the
	// full original membership.
	AutoEvict *AutoEvictConfig
}

// AutoEvictConfig configures Substrate.AutoEvict.
//
// The substrate's heartbeat-only failure detector emits a
// ConvFrame.heartbeat every QuietInterval of local-send silence.
// On the receive side, if no message arrives from a peer for
// SuspicionInterval, the substrate freezes that peer's slot
// in its psync.Membership.
type AutoEvictConfig struct {
	// QuietInterval — emit a heartbeat after this much local
	// outbound silence. Should be << SuspicionInterval so peers
	// see liveness signals well before timing out. Default 150ms.
	QuietInterval time.Duration

	// SuspicionInterval — declare a peer dead after this much
	// silence and freeze its slot. Default 10s. Tune to your
	// fault model: too short and a transient network hiccup
	// causes a permanent eviction; too long and write
	// availability suffers during pod restarts.
	SuspicionInterval time.Duration

	// OnEvict is invoked synchronously when this Substrate
	// auto-freezes `peer`. The local Members snapshot is the
	// post-freeze view. Optional — useful for application-level
	// metrics or logging.
	OnEvict func(peer ReplicaID, members []ReplicaID)
}

// Substrate is one application's handle to a replicated state
// machine running on a specific conversation. Created via
// Cluster.NewSubstrate.
type Substrate struct {
	cfg     SubstrateConfig
	cluster *Cluster
	logger  *slog.Logger

	conv  *psync.Conversation
	log   clog.MessageLog
	ord   order.Order
	hb    *failure.Detector // heartbeat-only; suspicion disabled
	close func()

	stopped   chan struct{}
	pumpDone  chan struct{}
	closeOnce sync.Once

	// snapshotThrough is the offset boundary for SM Apply
	// suppression. Apply is skipped for any message whose log
	// Offset is <= this value. Initialized from
	// SubstrateConfig.InitialSnapshot at construction; never
	// mutates after.
	snapshotThrough uint64

	// snapshotWatermark tracks how far the app has DURABLY
	// snapshotted (via AdvanceSnapshotWatermark). Published to
	// peers via the trim protocol (Phase 10(c)).
	snapshotWmk snapshotWatermark

	// Submit waiter machinery. Submit may register its waiter
	// AFTER the apply pump has already fired for that
	// sender_seq (the conv's deliver channel is faster than the
	// caller's bookkeeping). To handle that race, handleApplied
	// records its sender_seq in appliedSelf when no matching
	// waiter is yet registered, AND Submit checks appliedSelf
	// immediately after registering. Either path completes the
	// Submit.
	mu             sync.Mutex
	pendingApplies map[indexKey]chan struct{}
	appliedSelf    map[indexKey]struct{}
	closed         bool
}

type indexKey struct {
	sender string
	seq    uint64
}

// NewSubstrate constructs an application Substrate on this
// Cluster. The Cluster owns the underlying transport; the
// Substrate gets its own multiplex view bound to
// cfg.ConversationID, its own log, and its own
// psync.Conversation + Order layer.
func (c *Cluster) NewSubstrate(ctx context.Context, cfg SubstrateConfig) (*Substrate, error) {
	if err := validateSubstrateConfig(cfg); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = c.logger.With("conv", cfg.ConversationID.String()[:8])
	}
	clk := cfg.Clock
	if clk == nil {
		clk = c.clk
	}

	convNet := c.mux.ForConversation(cfg.ConversationID.toPB())
	logDir := filepath.Join(c.cfg.DataDir, "conversations", cfg.ConversationID.String())
	mlog, err := clog.OpenFile(logDir, cfg.ConversationID.toPB())
	if err != nil {
		return nil, fmt.Errorf("comlink: open substrate log: %w", err)
	}
	cleanup := []func(){func() { _ = mlog.Close() }}
	rollback := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}

	pbMembers := make([]*pb.ReplicaID, len(cfg.Members))
	for i, m := range cfg.Members {
		pbMembers[i] = m.toPB()
	}
	// Validate InitialSnapshot before allocating anything else:
	// it's a fast caller-error check.
	if cfg.InitialSnapshot != nil {
		if _, ok := cfg.StateMachine.(Snapshotter); !ok {
			return nil, errors.New("comlink: SubstrateConfig.InitialSnapshot set but StateMachine does not implement Snapshotter")
		}
	}

	// Construct the Substrate skeleton FIRST so we can wire its
	// noteReceive method into psync's OnReceive callback. The
	// hb field is filled in below; noteReceive guards on nil.
	s := &Substrate{
		cfg:            cfg,
		cluster:        c,
		logger:         logger,
		log:            mlog,
		stopped:        make(chan struct{}),
		pumpDone:       make(chan struct{}),
		pendingApplies: make(map[indexKey]chan struct{}),
		appliedSelf:    make(map[indexKey]struct{}),
	}
	if cfg.InitialSnapshot != nil {
		s.snapshotThrough = cfg.InitialSnapshot.ThroughOffset
		s.snapshotWmk.advance(cfg.InitialSnapshot.ThroughOffset)
	}

	// Bind psync to the Cluster's lifetime ctx, NOT the caller's
	// NewSubstrate ctx — the latter is typically a short-lived
	// bootstrap context that would otherwise kill the conv's pumps
	// the moment NewSubstrate returns.
	conv, err := psync.New(c.runCtx, psync.Config{
		ConversationID:  cfg.ConversationID.toPB(),
		Self:            c.cfg.Self.toPB(),
		Members:         pbMembers,
		Network:         convNet,
		Log:             mlog,
		Storage:         c.storage,
		Logger:          logger,
		Clock:           clk,
		DeliveryBufSize: 1024,
		// Receive-path liveness signal — keeps the FD's view of
		// peers fresh even when the Order wave gate is stalled.
		OnReceive: s.noteReceive,
	})
	if err != nil {
		rollback()
		return nil, fmt.Errorf("comlink: create substrate Conversation: %w", err)
	}
	cleanup = append(cleanup, func() { _ = conv.Close() })

	ord, err := buildOrder(conv, cfg)
	if err != nil {
		rollback()
		return nil, err
	}
	cleanup = append(cleanup, func() { _ = ord.Close() })

	s.conv = conv
	s.ord = ord
	s.close = rollback

	// Heartbeat-only failure.Detector. When AutoEvict is nil
	// (default) suspicion is disabled and the substrate waits
	// for every original member to advance the wave gate. When
	// AutoEvict is set, on-suspicion the substrate freezes the
	// peer's slot so the Order layer can keep making progress.
	quietInterval := 150 * time.Millisecond
	suspInterval := 100 * 365 * 24 * time.Hour // ~"never"
	onSuspect := func(*pb.ReplicaID) {}
	if cfg.AutoEvict != nil {
		if cfg.AutoEvict.QuietInterval > 0 {
			quietInterval = cfg.AutoEvict.QuietInterval
		}
		suspInterval = 10 * time.Second
		if cfg.AutoEvict.SuspicionInterval > 0 {
			suspInterval = cfg.AutoEvict.SuspicionInterval
		}
		onSuspect = s.handleAutoEvict
	}
	s.hb = failure.New(failure.Config{
		Self:              c.cfg.Self.toPB(),
		Members:           pbMembers,
		Clock:             clk,
		QuietInterval:     quietInterval,
		SuspicionInterval: suspInterval,
		TickInterval:      25 * time.Millisecond,
		SendHeartbeat:     s.sendHeartbeat,
		OnSuspect:         onSuspect,
	})
	cleanup = append(cleanup, func() { _ = s.hb.Close() })

	// Install the initial snapshot BEFORE the apply pump starts
	// — Restore must complete before any Apply could fire.
	if cfg.InitialSnapshot != nil {
		snap := cfg.InitialSnapshot
		if err := cfg.StateMachine.(Snapshotter).Restore(snap.Bytes); err != nil {
			rollback()
			return nil, fmt.Errorf("comlink: StateMachine.Restore: %w", err)
		}
		logger.Info("substrate: restored from snapshot",
			"through_offset", snap.ThroughOffset,
			"size_bytes", len(snap.Bytes))
	}

	go s.applyPump()

	// Phase 7(b): replay the local log so the StateMachine
	// rebuilds pre-crash state. Each replayed envelope is pushed
	// to the deliver channel and flows through Order → SM exactly
	// as if it had just been received. No-op for fresh substrates
	// (log is empty).
	//
	// Async: ReplayLog pushes through the bounded (1024-entry)
	// deliver channel and the Order layer's wave gates may defer
	// some applies until live peer heartbeats catch up. If the
	// peers aren't up yet (cold-start of every pod simultaneously
	// in K8s), inline ReplayLog would deadlock NewSubstrate. So
	// we fire it on a goroutine and return — SM state is rebuilt
	// "eventually". Callers that need full convergence should
	// poll their own SM state.
	go func() {
		replayed, err := conv.ReplayLog(c.runCtx)
		if err != nil {
			logger.Warn("substrate: log replay error", "err", err)
		} else if replayed > 0 {
			logger.Info("substrate: replayed log entries", "count", replayed)
		}
	}()

	return s, nil
}

// noteReceive is wired into psync as OnReceive. Fires on every
// successfully-decoded inbound frame, regardless of whether
// the Order wave gate is open. Phase 10(a): keeps FD's
// lastReceived for the peer fresh so auto-evict doesn't fire
// spuriously when waves are stalled.
//
// Tolerates s.hb being nil during the narrow construction
// window between Substrate skeleton creation and FD wiring.
func (s *Substrate) noteReceive(from *pb.ReplicaID) {
	if s.hb != nil {
		s.hb.NoteReceived(from)
	}
}

// handleAutoEvict fires (synchronously, from the FD tick
// goroutine) on every alive→suspected transition when AutoEvict
// is configured. It freezes the peer's slot in psync so the
// Order layer's wave gates can advance without it, and removes
// the peer from the substrate's own FD member set so we don't
// re-fire on the same transition.
//
// Idempotent: psync.Membership.Freeze on an already-frozen
// slot returns a non-nil error which we log-and-swallow.
func (s *Substrate) handleAutoEvict(peer *pb.ReplicaID) {
	peerID := replicaIDFromPB(peer)
	if err := s.conv.FreezeMember(peer); err != nil {
		// Likely "already frozen" on a duplicate fire; quiet.
		s.logger.Debug("substrate: auto-evict freeze (likely already frozen)",
			"peer", peerID.String(), "err", err)
	} else {
		s.logger.Warn("substrate: auto-evicting silent peer",
			"peer", peerID.String(),
			"suspicion_interval", s.cfg.AutoEvict.SuspicionInterval)
	}
	// Stop watching the peer so we don't re-fire.
	s.hb.RemoveMember(peer)
	if cb := s.cfg.AutoEvict.OnEvict; cb != nil {
		cb(peerID, s.Members())
	}
	metricSubstrateAutoEvict.WithLabelValues(shortConvID(s.cfg.ConversationID)).Inc()
}

// sendHeartbeat emits a ConvFrame.heartbeat through the
// conversation. Idempotent and quiet — used by the embedded
// failure.Detector when no other traffic has flowed for
// QuietInterval. Spawned in a goroutine to avoid blocking the
// Detector's tick goroutine on conv.Send.
func (s *Substrate) sendHeartbeat() {
	go func() {
		bs, err := frame.MarshalHeartbeat()
		if err != nil {
			return
		}
		_, err = s.conv.Send(bs)
		if err == nil {
			s.hb.NoteSent()
		}
	}()
}

func validateSubstrateConfig(cfg SubstrateConfig) error {
	if len(cfg.ConversationID) != idLen {
		return errors.New("comlink: SubstrateConfig.ConversationID is required")
	}
	if len(cfg.Members) == 0 {
		return errors.New("comlink: SubstrateConfig.Members must be non-empty")
	}
	if cfg.StateMachine == nil {
		return errors.New("comlink: SubstrateConfig.StateMachine is required")
	}
	if cfg.Ordering == OrderingSemOrder && cfg.Classifier == nil {
		return errors.New("comlink: SubstrateConfig.Classifier required for OrderingSemOrder")
	}
	return nil
}

func buildOrder(conv *psync.Conversation, cfg SubstrateConfig) (order.Order, error) {
	switch cfg.Ordering {
	case OrderingPartial:
		return order.NewPartial(conv), nil
	case OrderingTotal:
		return order.NewTotal(conv), nil
	case OrderingSemOrder:
		return order.NewSemOrder(conv, cfg.Classifier), nil
	default:
		return nil, fmt.Errorf("comlink: unknown OrderingKind %d", cfg.Ordering)
	}
}

// Submit submits payload to the substrate. Blocks until this
// replica has applied the command via StateMachine.Apply (or
// ctx is done). Returns the Apply's effective error if any
// (currently always nil since StateMachine.Apply is infallible).
//
// The same payload will be applied at every replica in the
// substrate's chosen ordering — all replicas converge.
func (s *Substrate) Submit(ctx context.Context, payload []byte) error {
	submitStart := time.Now()
	convLabel := shortConvID(s.cfg.ConversationID)
	defer func() {
		metricSubstrateSubmitDuration.WithLabelValues(convLabel).Observe(time.Since(submitStart).Seconds())
	}()
	wrapped, err := frame.MarshalApp(payload)
	if err != nil {
		return fmt.Errorf("comlink: Submit: wrap: %w", err)
	}
	id, err := s.conv.Send(wrapped)
	if err != nil {
		return fmt.Errorf("comlink: Submit: send: %w", err)
	}
	metricSubstrateSubmitted.WithLabelValues(convLabel).Inc()
	s.hb.NoteSent()
	// Compute sender_seq from the returned MessageID + our known
	// member list. Self's slot index is its position in cfg.Members.
	selfSlot := -1
	for i, m := range s.cfg.Members {
		if m.Equal(s.cluster.Self()) {
			selfSlot = i
			break
		}
	}
	if selfSlot < 0 {
		return errors.New("comlink: Submit: self not in substrate members (config bug)")
	}
	vc := id.GetVectorClock()
	if selfSlot >= len(vc) {
		return errors.New("comlink: Submit: assigned vector clock too short for self slot")
	}
	senderSeq := vc[selfSlot]

	wait := make(chan struct{})
	key := indexKey{sender: string(s.cluster.Self()), seq: senderSeq}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("comlink: Submit: substrate closed")
	}
	if _, alreadyApplied := s.appliedSelf[key]; alreadyApplied {
		// The apply pump fired for this sender_seq before we got
		// here (deliver channel was faster than our bookkeeping).
		// Consume the marker and return.
		delete(s.appliedSelf, key)
		s.mu.Unlock()
		return nil
	}
	s.pendingApplies[key] = wait
	s.mu.Unlock()

	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pendingApplies, key)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.stopped:
		return errors.New("comlink: Submit: substrate closed")
	}
}

// SetWatermark advertises the lowest log offset this replica
// still needs for its own recovery (PLAN §2.8). The application
// calls this after it has durably persisted application state
// covering up through `offset`. Group-wide trim safety = min of
// every active member's watermark.
func (s *Substrate) SetWatermark(offset uint64) {
	// For Phase 5(e) we don't yet plumb a Manager into the
	// app substrate (per the design note in the file header —
	// app substrates skip Manager because they don't need
	// per-conv failure detection). The watermark protocol thus
	// isn't operational at the app substrate layer in v1; the
	// system conv still has one via its Manager. A future
	// commit may add a lightweight watermark-only path on the
	// app substrate's psync.Conversation if real workloads
	// need it.
	_ = offset
}

// Members returns the substrate's current member set.
func (s *Substrate) Members() []ReplicaID {
	out := make([]ReplicaID, len(s.cfg.Members))
	copy(out, s.cfg.Members)
	return out
}

// AdvanceSnapshotWatermark informs the substrate that the
// application has DURABLY persisted a snapshot through log
// offset `through`. The substrate uses this:
//
//   - To extend the trim safe-frontier so older log entries
//     can be compacted (Phase 10(c) — not yet wired through to
//     trim itself).
//   - To answer sponsor-handshake snapshot requests with a
//     pointer to the latest covered offset (Phase 10(d)).
//
// Monotonic — calls with a lower offset than the current
// watermark are silently ignored. Safe to call concurrently
// with everything else (atomic update).
//
// Apps should call this AFTER fsync of their snapshot bytes
// completes. Calling it earlier risks losing snapshotted
// data on crash and re-needing log entries that the cluster
// has since trimmed.
func (s *Substrate) AdvanceSnapshotWatermark(through uint64) {
	s.snapshotWmk.advance(through)
}

// SnapshotWatermark returns the most recent offset the
// application has reported as durably snapshotted.
func (s *Substrate) SnapshotWatermark() uint64 {
	return s.snapshotWmk.get()
}

// FreezeMember marks `replica`'s slot as frozen in this
// Substrate's psync membership — the ordering layer will no
// longer wait for this replica's messages. The replica is NOT
// removed (the slot is kept for vector-clock alignment).
//
// Intended use: an application learns that a replica is dead at
// the cluster level (e.g. via Cluster.VoteOut) and must mirror
// the eviction on each app substrate to unblock total-order
// wave completion. Currently this is a manual step; Phase 7 may
// automate cluster→substrate membership propagation.
//
// Re-freezing a frozen replica returns an error from psync;
// callers can typically ignore it.
func (s *Substrate) FreezeMember(replica ReplicaID) error {
	return s.conv.FreezeMember(replica.toPB())
}

// ConversationID returns this substrate's conversation id.
func (s *Substrate) ConversationID() ConversationID { return s.cfg.ConversationID }

// Close stops the apply pump and releases per-substrate
// resources. Idempotent.
func (s *Substrate) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.stopped)
		s.close()
	})
	<-s.pumpDone
	return nil
}

// applyPump is the goroutine that reads ordered deliveries from
// the Order layer, looks up the local log offset, dispatches to
// the StateMachine, and signals any matching Submit waiter.
func (s *Substrate) applyPump() {
	defer close(s.pumpDone)
	ctx := context.Background()
	for {
		select {
		case applied, ok := <-s.ord.Apply():
			if !ok {
				return
			}
			s.handleApplied(ctx, applied)
		case <-s.stopped:
			return
		}
	}
}

func (s *Substrate) handleApplied(ctx context.Context, applied order.Applied) {
	env := applied.Envelope
	if env == nil || env.GetId() == nil {
		return
	}
	sender := env.GetId().GetSender()
	// Inform the heartbeat tracker we got a frame from this
	// peer; idle peers' heartbeats reset the same NoteReceived
	// path, advancing local quiet/suspicion timers.
	s.hb.NoteReceived(sender)
	// Decode the substrate-level frame. App data flows to
	// StateMachine.Apply; heartbeats are credited (already done
	// via NoteReceived above) and otherwise dropped.
	dec, err := frame.Unmarshal(env.GetPayload())
	if err != nil {
		s.logger.Warn("substrate: bad ConvFrame", "err", err)
		return
	}
	if dec.App == nil {
		// Heartbeat / membership / watermark variants don't
		// reach the application — they exist for stability /
		// substrate bookkeeping only.
		return
	}

	// Compute sender's slot in our member list, then sender_seq.
	senderSlot := -1
	for i, m := range s.cfg.Members {
		if m.Equal(replicaIDFromPB(sender)) {
			senderSlot = i
			break
		}
	}
	if senderSlot < 0 {
		// Apply for an unknown sender; skip.
		return
	}
	vc := env.GetId().GetVectorClock()
	if senderSlot >= len(vc) {
		return
	}
	senderSeq := vc[senderSlot]

	// Look up offset from the local log.
	var offset uint64
	if entry, err := s.log.LookupBySender(ctx, sender.GetValue(), senderSeq); err == nil {
		offset = uint64(entry.Offset)
	}

	msg := &Message{
		ID:      messageIDFromPB(env.GetId()),
		Payload: dec.App,
		Sender:  replicaIDFromPB(sender),
		Offset:  offset,
		Wave:    waveOfVC(vc),
	}
	convLabel := shortConvID(s.cfg.ConversationID)
	// Skip apply for messages already covered by the initial
	// snapshot (Phase 10(b)). The Order layer / log still
	// processes them for graph / stability bookkeeping, but the
	// SM doesn't double-apply.
	if msg.Offset != 0 && msg.Offset <= s.snapshotThrough {
		metricSubstrateApplySkipped.WithLabelValues(convLabel).Inc()
	} else {
		applyStart := time.Now()
		s.cfg.StateMachine.Apply(ctx, msg)
		metricSubstrateApplyDuration.WithLabelValues(convLabel).Observe(time.Since(applyStart).Seconds())
		metricSubstrateApplied.WithLabelValues(convLabel).Inc()
	}

	// Signal any matching Submit waiter, OR record that this
	// sender_seq has been applied so a soon-to-register Submit
	// waiter can short-circuit. We only track our own sends —
	// peers' sends never have a Submit waiter on this replica.
	key := indexKey{sender: string(sender.GetValue()), seq: senderSeq}
	isSelf := s.cluster.Self().Equal(replicaIDFromPB(sender))
	s.mu.Lock()
	wait, ok := s.pendingApplies[key]
	if ok {
		delete(s.pendingApplies, key)
	} else if isSelf {
		s.appliedSelf[key] = struct{}{}
	}
	s.mu.Unlock()
	if ok {
		close(wait)
	}
}

func waveOfVC(vc []uint64) uint64 {
	var m uint64
	for _, x := range vc {
		if x > m {
			m = x
		}
	}
	return m
}
