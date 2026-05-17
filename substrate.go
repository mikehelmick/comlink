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

	s := &Substrate{
		cfg:            cfg,
		cluster:        c,
		logger:         logger,
		conv:           conv,
		log:            mlog,
		ord:            ord,
		stopped:        make(chan struct{}),
		pumpDone:       make(chan struct{}),
		pendingApplies: make(map[indexKey]chan struct{}),
		appliedSelf:    make(map[indexKey]struct{}),
	}
	s.close = rollback

	// Heartbeat-only failure.Detector: emits ConvFrame.heartbeat
	// when this conversation has been quiet, advancing stability
	// for the Order layer. Suspicion is disabled (interval set
	// effectively-infinite, OnSuspect is no-op) — peer liveness
	// is the system conv's responsibility, not this substrate's.
	s.hb = failure.New(failure.Config{
		Self:              c.cfg.Self.toPB(),
		Members:           pbMembers,
		Clock:             clk,
		QuietInterval:     150 * time.Millisecond,
		SuspicionInterval: 100 * 365 * 24 * time.Hour,
		TickInterval:      25 * time.Millisecond,
		SendHeartbeat:     s.sendHeartbeat,
		OnSuspect:         func(*pb.ReplicaID) {},
	})
	cleanup = append(cleanup, func() { _ = s.hb.Close() })

	go s.applyPump()
	return s, nil
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
	wrapped, err := frame.MarshalApp(payload)
	if err != nil {
		return fmt.Errorf("comlink: Submit: wrap: %w", err)
	}
	id, err := s.conv.Send(wrapped)
	if err != nil {
		return fmt.Errorf("comlink: Submit: send: %w", err)
	}
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
	s.cfg.StateMachine.Apply(ctx, msg)

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
