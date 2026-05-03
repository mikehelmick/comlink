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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/failure"
	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
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

	// Mutable membership-protocol state will land in 3(d). For
	// now Manager just holds the original Members; ML-mutation
	// methods (Remove / Incorporate) are placeholders.
	mu             sync.Mutex
	membershipList []*pb.ReplicaID

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

	m := &Manager{
		cfg:      cfg,
		conv:     cfg.Conversation,
		logger:   logger,
		appCh:    make(chan AppMessage, bufSize),
		stopped:  make(chan struct{}),
		pumpDone: make(chan struct{}),
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
			// Treat any incoming envelope as proof-of-life from sender.
			m.fd.NoteReceived(sender)

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
	case dec.Recovering != nil:
		m.handleRecovering(dec.Recovering, sender)
	case dec.RecoveryAck != nil:
		m.handleRecoveryAck(dec.RecoveryAck, sender)
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
	}
}

// ─── failure.Detector callbacks ───────────────────────────────────

// sendHeartbeat is called by the FailureDetector when the
// conversation has been quiet for QuietInterval.
func (m *Manager) sendHeartbeat() {
	if m.isClosed() {
		return
	}
	bs, err := frame.MarshalHeartbeat()
	if err != nil {
		m.logger.Warn("membership: marshal heartbeat", "err", err)
		return
	}
	if _, err := m.conv.Send(bs); err != nil {
		m.logger.Warn("membership: send heartbeat", "err", err)
		return
	}
	m.fd.NoteSent()
}

// onSuspect is called by the FailureDetector once per "alive ->
// suspected" transition. Phase 3(c) just emits a SuspectDown
// message into the conversation; Phase 3(d) will run the full
// sf-group + ack/nack flow off this signal.
func (m *Manager) onSuspect(replica *pb.ReplicaID) {
	if m.isClosed() {
		return
	}
	bs, err := frame.MarshalSuspectDown(replica)
	if err != nil {
		m.logger.Warn("membership: marshal SuspectDown", "err", err)
		return
	}
	if _, err := m.conv.Send(bs); err != nil {
		m.logger.Warn("membership: send SuspectDown", "err", err)
		return
	}
	m.fd.NoteSent()
}

func (m *Manager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// ─── membership-event handlers (stubs for Phase 3(d+)) ────────────

// handleSuspectDown is the receiver-side of the informational
// SuspectDown notification. PLAN §2.13: peers add `suspect` to
// SuspectDownList and Maskout(suspect); recovery happens implicitly
// when subsequent traffic arrives from suspect. Phase 3(d) wires
// this in.
func (m *Manager) handleSuspectDown(_ *pb.SuspectDown, _ *pb.ReplicaID) {}

func (m *Manager) handleRecovering(_ *pb.Recovering, _ *pb.ReplicaID)   {}
func (m *Manager) handleRecoveryAck(_ *pb.RecoveryAck, _ *pb.ReplicaID) {}

// VoteOut/VoteIn handlers — Phase 3(e) implements the explicit
// ML-mutation protocols.
func (m *Manager) handleVoteOut(_ *pb.VoteOut, _ *pb.ReplicaID)         {}
func (m *Manager) handleVoteOutAck(_ *pb.VoteOutAck, _ *pb.ReplicaID)   {}
func (m *Manager) handleVoteOutNack(_ *pb.VoteOutNack, _ *pb.ReplicaID) {}
func (m *Manager) handleVoteIn(_ *pb.VoteIn, _ *pb.ReplicaID)           {}
func (m *Manager) handleVoteInAck(_ *pb.VoteInAck, _ *pb.ReplicaID)     {}
func (m *Manager) handleVoteInNack(_ *pb.VoteInNack, _ *pb.ReplicaID)   {}
