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

// Package membership implements paper §4 group membership.
//
// Manager is the primary type — it wraps a psync.Conversation,
// owns a failure.Detector, and dispatches incoming frames
// (substrate ConvFrame variants) to the appropriate internal
// handler. Apps interact with Manager via SendApp / Recv; the
// raw psync layer is hidden.
//
// Phase 3 land in stages:
//   - 3(c) (this file): skeleton — frame routing, heartbeat
//     wiring, raw suspicion handling. The membership-protocol
//     state machine (sf-groups, ack/nack, SuspectDownList /
//     SuspectUpList) is stubbed.
//   - 3(d): sf-group construction + ML mutation per paper §4.2.
//   - 3(e): membership-only stability hook (paper §4.2.2 second
//     stability definition).
//   - 3(f): partition handling per PLAN §2.11.
//   - 3(g): recovery-side (p is up) handling.
//   - 3(h): the four §4.1 invariants tested + benchmark.
package membership

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/failure"
	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/trim"
	"google.golang.org/protobuf/proto"
)

// Config configures a Manager.
type Config struct {
	// Conversation is the underlying psync layer. Must be non-nil
	// and not yet have any other consumer reading Recv().
	Conversation *psync.Conversation
	// Self is this replica's ID; must be in Members.
	Self *pb.ReplicaID
	// Members is the initial participant set.
	Members []*pb.ReplicaID

	// Clock for scheduling heartbeats and suspicion timers. Default
	// clock.NewSystem().
	Clock clock.Clock
	// Logger; default slog.Default().
	Logger *slog.Logger

	// QuietInterval / SuspicionInterval / TickInterval are passed
	// straight to the embedded failure.Detector (see that package
	// for defaults).
	QuietInterval     time.Duration
	SuspicionInterval time.Duration
	TickInterval      time.Duration

	// AppBufSize is the buffer of the application Recv channel.
	// Default 256.
	AppBufSize int

	// InitialGroupSize is N from PLAN §2.11 — the original
	// participant count used for quorum decisions on VoteOut /
	// VoteIn. If zero, defaults to len(Members) (i.e. assumes the
	// Manager is constructed at conversation creation time and
	// the initial Members list IS the full original group).
	//
	// A replica is considered "in the majority partition" iff
	// len(currentML) > InitialGroupSize / 2. Minority replicas
	// refuse to initiate VoteOut / VoteIn (ErrPartitionMinority);
	// they still process incoming events, ack/nack, and apply
	// accepted decisions when those reach them via the
	// conversation.
	InitialGroupSize int

	// Log is the underlying psync.MessageLog. Required for the
	// trim protocol (PLAN §2.8): Manager.SetWatermark broadcasts
	// the local watermark and, when the safe-trim frontier
	// advances, calls Log.Truncate. Should be the SAME instance
	// passed into the underlying psync.Conversation.
	Log clog.MessageLog

	// OnMembershipChange is invoked after an accepted membership
	// change has been applied locally (after the ML mutation and
	// any psync reshape). Optional. The callback runs on an
	// internal goroutine; it MUST NOT call back into the Manager
	// (re-entrant deadlock). The intended use is to persist or
	// propagate the change to other layers (transport routing,
	// stable.Storage, etc).
	//
	// addr is populated only for MembershipChangeAdded; for
	// MembershipChangeRemoved it is the empty string.
	OnMembershipChange func(event MembershipChange)
}

// MembershipChangeKind enumerates the kinds of membership change
// events fired through Config.OnMembershipChange.
type MembershipChangeKind int

const (
	// MembershipChangeAdded indicates a replica was added via
	// VoteIn and the MemberAdd commit message has been applied
	// locally.
	MembershipChangeAdded MembershipChangeKind = iota
	// MembershipChangeRemoved indicates a replica was removed
	// via an accepted VoteOut.
	MembershipChangeRemoved
)

// MembershipChange is the event delivered to
// Config.OnMembershipChange. Replica is the affected replica;
// Addr is its network address (set only for Added).
type MembershipChange struct {
	Kind    MembershipChangeKind
	Replica *pb.ReplicaID
	Addr    string
}

// AppMessage is the unit handed to applications via Manager.Recv.
type AppMessage struct {
	From     *pb.ReplicaID
	Payload  []byte
	Envelope *pb.Envelope
}

// Manager is the membership orchestrator.
type Manager struct {
	cfg    Config
	conv   *psync.Conversation
	fd     *failure.Detector
	logger *slog.Logger
	appCh  chan AppMessage

	// membershipList is the active ML view. Phase 3(d) does not
	// mutate it; Phase 3(e)'s VoteOut/VoteIn protocols will.
	mu             sync.Mutex
	membershipList []*pb.ReplicaID
	// suspectDownList tracks replicas this Manager currently
	// considers suspected (PLAN §2.13). Membership in the set
	// implies a corresponding Maskout is in place on the
	// underlying conversation.
	suspectDownList map[string]struct{}
	// voteOutSessions / voteInSessions track in-flight votes
	// keyed by target ReplicaID bytes. One session per target
	// per kind at a time.
	voteOutSessions map[string]*voteOutSession
	voteInSessions  map[string]*voteInSession
	// trim tracks per-replica watermarks for the trim protocol
	// (PLAN §2.8 / Phase 4).
	trim *trim.Tracker

	closeOnce sync.Once
	stopped   chan struct{}
	pumpDone  chan struct{}
	closed    bool
}

// New constructs a Manager bound to cfg.Conversation. The Manager
// takes over reading conv.Recv() — apps must use Manager.Recv
// after this point, not conv.Recv directly.
func New(cfg Config) (*Manager, error) {
	if cfg.Conversation == nil {
		return nil, errors.New("membership: Config.Conversation is required")
	}
	if cfg.Self == nil {
		return nil, errors.New("membership: Config.Self is required")
	}
	if len(cfg.Members) == 0 {
		return nil, errors.New("membership: Config.Members is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	bufSize := cfg.AppBufSize
	if bufSize <= 0 {
		bufSize = 256
	}
	if cfg.InitialGroupSize <= 0 {
		cfg.InitialGroupSize = len(cfg.Members)
	}

	m := &Manager{
		cfg:             cfg,
		conv:            cfg.Conversation,
		logger:          logger,
		appCh:           make(chan AppMessage, bufSize),
		stopped:         make(chan struct{}),
		pumpDone:        make(chan struct{}),
		suspectDownList: make(map[string]struct{}),
		voteOutSessions: make(map[string]*voteOutSession),
		voteInSessions:  make(map[string]*voteInSession),
		trim:            trim.New(),
	}
	m.membershipList = make([]*pb.ReplicaID, len(cfg.Members))
	for i, r := range cfg.Members {
		m.membershipList[i] = proto.Clone(r).(*pb.ReplicaID)
	}

	m.fd = failure.New(failure.Config{
		Self:              cfg.Self,
		Members:           cfg.Members,
		Clock:             cfg.Clock,
		QuietInterval:     cfg.QuietInterval,
		SuspicionInterval: cfg.SuspicionInterval,
		TickInterval:      cfg.TickInterval,
		SendHeartbeat:     m.sendHeartbeat,
		OnSuspect:         m.onSuspect,
	})

	go m.pump()
	return m, nil
}

// SendApp wraps payload in a ConvFrame.app and sends it through
// the underlying Conversation. Returns the assigned MessageID.
func (m *Manager) SendApp(payload []byte) (*pb.MessageID, error) {
	bs, err := frame.MarshalApp(payload)
	if err != nil {
		return nil, fmt.Errorf("membership: marshal app frame: %w", err)
	}
	id, err := m.conv.Send(bs)
	if err == nil {
		m.fd.NoteSent()
	}
	return id, err
}

// Recv returns the application-facing delivery channel. Closed
// after Close.
func (m *Manager) Recv() <-chan AppMessage {
	return m.appCh
}

// Members returns a snapshot of the current membership list.
func (m *Manager) Members() []*pb.ReplicaID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*pb.ReplicaID, len(m.membershipList))
	for i, r := range m.membershipList {
		out[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	return out
}

// IsSuspected reports whether replica is currently in this
// Manager's local SuspectDownList (PLAN §2.13).
func (m *Manager) IsSuspected(replica *pb.ReplicaID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.suspectDownList[string(replica.GetValue())]
	return ok
}

// SuspectedReplicas returns clones of every currently-suspected
// replica in arbitrary order. Useful for tests and observability.
func (m *Manager) SuspectedReplicas() []*pb.ReplicaID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*pb.ReplicaID, 0, len(m.suspectDownList))
	for k := range m.suspectDownList {
		out = append(out, &pb.ReplicaID{Value: []byte(k)})
	}
	return out
}

// InMajorityPartition reports whether this replica believes it is
// in the majority partition (PLAN §2.11): len(currentML) >
// InitialGroupSize/2. Strict majority — a tied split (|ML| ==
// N/2) is treated as minority to avoid split-brain.
func (m *Manager) InMajorityPartition() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.membershipList)*2 > m.cfg.InitialGroupSize
}

// SetWatermark is the application's checkpoint API (PLAN §2.8 /
// Phase 4): the application calls this when it has snapshotted
// its state and no longer needs log entries below `offset` for
// its own recovery.
//
// Effect:
//   - Updates the local watermark in the trim tracker.
//   - Broadcasts a Watermark(offset) frame so peers can update
//     their view of our watermark.
//   - Recomputes the safe-trim frontier; if it advanced, truncates
//     the local log.
func (m *Manager) SetWatermark(offset uint64) {
	if m.cfg.Log == nil {
		return
	}
	advanced := m.trim.Update(m.cfg.Self, clog.Offset(offset))
	if !advanced {
		return
	}
	bs, err := frame.MarshalWatermark(offset)
	if err != nil {
		m.logger.Warn("membership: marshal Watermark", "err", err)
		return
	}
	go m.asyncSend(bs, "Watermark")
	m.maybeTrim()
}

// handleWatermark records a peer's watermark advertisement and
// re-evaluates the safe-trim frontier.
func (m *Manager) handleWatermark(w *pb.Watermark, sender *pb.ReplicaID) {
	if m.trim == nil {
		return
	}
	if !m.trim.Update(sender, clog.Offset(w.GetOffset())) {
		return
	}
	m.maybeTrim()
}

// maybeTrim computes the current safe-trim frontier and truncates
// the local log to it if it has advanced. Called whenever any
// replica's watermark advances.
func (m *Manager) maybeTrim() {
	if m.cfg.Log == nil {
		return
	}
	m.mu.Lock()
	active := make([]*pb.ReplicaID, len(m.membershipList))
	for i, r := range m.membershipList {
		active[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	m.mu.Unlock()
	frontier, ok := m.trim.SafeFrontier(active)
	if !ok || frontier <= m.cfg.Log.FirstOffset() {
		return
	}
	if err := m.cfg.Log.Truncate(context.Background(), frontier); err != nil {
		m.logger.Warn("membership: log.Truncate", "frontier", frontier, "err", err)
	}
}

// Close stops the Manager. The underlying Conversation is NOT
// closed; the caller owns it. Close is independent of Conversation
// lifecycle — Manager.Close exits cleanly whether or not the
// Conversation has been closed yet.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		close(m.stopped) // signal pump to exit
		_ = m.fd.Close()
	})
	<-m.pumpDone
	return nil
}

// pump drains the Conversation's delivery channel and routes each
// envelope by ConvFrame body type. Exits when the Conversation's
// Recv closes OR when Close has been called.
func (m *Manager) pump() {
	defer close(m.pumpDone)
	defer close(m.appCh)
	for {
		select {
		case d, ok := <-m.conv.Recv():
			if !ok {
				return
			}
			sender := d.Envelope.GetId().GetSender()
			// Treat any incoming envelope as proof-of-life from
			// sender. This includes implicit recovery from soft
			// suspicion (PLAN §2.13): if we'd previously masked
			// sender out, the act of receiving from them clears
			// the suspicion and unmasks them.
			m.fd.NoteReceived(sender)
			m.clearSuspicion(sender)

			dec, err := frame.Unmarshal(d.Envelope.GetPayload())
			if err != nil {
				m.logger.Warn("membership: bad frame", "from", fmt.Sprintf("%x", sender.GetValue()), "err", err)
				continue
			}
			m.dispatch(d, dec, sender)
		case <-m.stopped:
			return
		}
	}
}

func (m *Manager) dispatch(d psync.Delivery, dec frame.Decoded, sender *pb.ReplicaID) {
	switch {
	case dec.App != nil:
		select {
		case m.appCh <- AppMessage{From: sender, Payload: dec.App, Envelope: d.Envelope}:
		default:
			// App buffer full; drop & log. Production would block
			// instead, but blocking here would deadlock the pump.
			m.logger.Warn("membership: app channel full; dropping", "from", fmt.Sprintf("%x", sender.GetValue()))
		}
	case dec.Heartbeat:
		// Liveness signal only; NoteReceived already credited.
	case dec.SuspectDown != nil:
		m.handleSuspectDown(dec.SuspectDown, sender)
	case dec.Watermark != nil:
		m.handleWatermark(dec.Watermark, sender)
	case dec.VoteOut != nil:
		m.handleVoteOut(dec.VoteOut, sender)
	case dec.VoteOutAck != nil:
		m.handleVoteOutAck(dec.VoteOutAck, sender)
	case dec.VoteOutNack != nil:
		m.handleVoteOutNack(dec.VoteOutNack, sender)
	case dec.VoteIn != nil:
		m.handleVoteIn(dec.VoteIn, sender)
	case dec.VoteInAck != nil:
		m.handleVoteInAck(dec.VoteInAck, sender)
	case dec.VoteInNack != nil:
		m.handleVoteInNack(dec.VoteInNack, sender)
	case dec.MemberAdd != nil:
		m.handleMemberAdd(dec.MemberAdd, sender)
	}
}

// ─── failure.Detector callbacks ───────────────────────────────────

// sendHeartbeat is called by the FailureDetector when the
// conversation has been quiet for QuietInterval.
//
// Sends spawn a goroutine because callers of this method may be
// holding locks or sitting on the receive path (the FailureDetector
// callback fires from a tick goroutine; we don't want it to block
// on conv.Send, which would re-enter the genserver). This is the
// general pattern for any Manager-internal Send: spawn so the
// caller is never re-entrantly blocked.
func (m *Manager) sendHeartbeat() {
	if m.isClosed() {
		return
	}
	bs, err := frame.MarshalHeartbeat()
	if err != nil {
		m.logger.Warn("membership: marshal heartbeat", "err", err)
		return
	}
	go m.asyncSend(bs, "heartbeat")
}

// asyncSend issues conv.Send in a goroutine, logging errors. The
// goroutine ensures the calling code path (pump, FD callback,
// etc.) does not re-enter the underlying psync genserver while
// holding its delivery loop blocked.
func (m *Manager) asyncSend(bs []byte, kind string) {
	if m.isClosed() {
		return
	}
	if _, err := m.conv.Send(bs); err != nil {
		if !m.isClosed() {
			m.logger.Warn("membership: send "+kind, "err", err)
		}
		return
	}
	m.fd.NoteSent()
}

// onSuspect is called by the FailureDetector once per "alive ->
// suspected" transition. PLAN §2.13: SuspectDown is informational;
// we mark replica as suspected locally (Maskout via psync) and
// broadcast SuspectDown(replica) so peers can do the same. No
// Ack/Nack response is expected. Recovery happens implicitly when
// a subsequent message arrives from the suspect (clearSuspicion).
func (m *Manager) onSuspect(replica *pb.ReplicaID) {
	if m.isClosed() {
		return
	}
	m.markSuspected(replica)
	bs, err := frame.MarshalSuspectDown(replica)
	if err != nil {
		m.logger.Warn("membership: marshal SuspectDown", "err", err)
		return
	}
	go m.asyncSend(bs, "SuspectDown")
}

// markSuspected adds replica to the local SuspectDownList. PLAN
// §2.13: soft suspicion is informational and does NOT call
// psync.Maskout — masking would prevent the very recovery message
// that should clear the suspicion from reaching us. Maskout is
// reserved for the hard-removal path (VoteOut, Phase 3(e)).
//
// Idempotent.
func (m *Manager) markSuspected(replica *pb.ReplicaID) {
	if bytes.Equal(replica.GetValue(), m.cfg.Self.GetValue()) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suspectDownList[string(replica.GetValue())] = struct{}{}
}

// clearSuspicion removes replica from the local SuspectDownList
// (if present). Called when a message arrives from replica —
// implicit recovery from soft suspicion (PLAN §2.13).
func (m *Manager) clearSuspicion(replica *pb.ReplicaID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.suspectDownList, string(replica.GetValue()))
}

func (m *Manager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// ─── membership-event handlers (stubs for Phase 3(d+)) ────────────

// handleSuspectDown is the receiver-side of the informational
// SuspectDown notification. PLAN §2.13: add `suspect` to local
// SuspectDownList. Recovery happens implicitly when subsequent
// traffic arrives from suspect (handled by the pump's
// clearSuspicion call).
//
// Self-suspicion (someone announcing they suspect themselves) and
// suspicion-of-the-sender (the SuspectDown's sender naming itself
// as the suspect) are both pathological cases that we ignore.
func (m *Manager) handleSuspectDown(susp *pb.SuspectDown, sender *pb.ReplicaID) {
	target := susp.GetSuspect()
	if target == nil {
		return
	}
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		// Someone suspects us. We obviously aren't down; the next
		// message we send will clear their suspicion via the
		// pump's clearSuspicion path on their side. We don't need
		// to act locally.
		return
	}
	if bytes.Equal(target.GetValue(), sender.GetValue()) {
		// Sender is suspecting itself — pathological; ignore.
		return
	}
	m.markSuspected(target)
}

// VoteOut/VoteIn handlers live in voteout.go and votein.go.
