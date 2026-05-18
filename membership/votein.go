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

package membership

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Errors returned by VoteIn.
var (
	// ErrVoteInTargetIsSelf rejects voting yourself in.
	ErrVoteInTargetIsSelf = errors.New("membership: VoteIn target is self")
	// ErrVoteInTargetAlreadyMember means target is already in ML.
	ErrVoteInTargetAlreadyMember = errors.New("membership: VoteIn target already in membership list")
	// ErrVoteInNacked means a peer disagreed with the addition.
	ErrVoteInNacked = errors.New("membership: VoteIn rejected by Nack")
	// ErrVoteInTimeout means the vote timed out before quorum.
	ErrVoteInTimeout = errors.New("membership: VoteIn timed out before quorum")
	// ErrVoteInInProgress means a VoteIn for this target is already in flight.
	ErrVoteInInProgress = errors.New("membership: VoteIn already in progress for target")
)

// voteInSession tracks an in-flight VoteIn (initiator-side
// state). Two-phase: collect quorum Ack on VoteIn, then broadcast
// MemberAdd as the commit/anchor point.
type voteInSession struct {
	target         *pb.ReplicaID
	addr           string
	membersAtStart []*pb.ReplicaID
	ackedBy        map[string]struct{}
	nackedBy       map[string]struct{}
	committed      bool // true once we've broadcast MemberAdd
	done           sync.Once
	err            error
	completed      chan struct{}
	mu             sync.Mutex
}

// VoteIn initiates the permanent addition of target to the
// conversation's membership list. addr is the network address
// peers should use to reach target (e.g. "host:port" for the gRPC
// transport).
//
// Decision rule: any VoteInNack aborts; strict majority of voters
// must Ack to accept.
//
// On accepted: every replica adds target to its local ML and
// emits a MembershipEvent on its events channel. Wiring up the
// transport routing for target, growing psync's vector-clock
// shape, and onboarding target itself (which involves target
// running its own Manager.New + Restart against the leaf set)
// are out of scope for this commit — see PLAN §2.10.1 and the
// Phase 5 composition layer.
//
// This call blocks until the vote completes.
func (m *Manager) VoteIn(ctx context.Context, target *pb.ReplicaID, addr string) error {
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		return ErrVoteInTargetIsSelf
	}
	m.mu.Lock()
	if m.isMemberLocked(target) {
		m.mu.Unlock()
		return ErrVoteInTargetAlreadyMember
	}
	if !m.inMajorityLocked() {
		m.mu.Unlock()
		return ErrPartitionMinority
	}
	if _, exists := m.voteInSessions[string(target.GetValue())]; exists {
		m.mu.Unlock()
		return ErrVoteInInProgress
	}
	session := newVoteInSession(target, addr, m.membershipList)
	// Initiator implicitly Acks.
	session.ackedBy[string(m.cfg.Self.GetValue())] = struct{}{}
	m.voteInSessions[string(target.GetValue())] = session
	m.mu.Unlock()

	if err := m.broadcastVoteIn(target, addr); err != nil {
		m.cancelVoteInSession(target, fmt.Errorf("broadcast VoteIn: %w", err))
		<-session.completed
		return session.err
	}

	m.checkVoteInDecision(session)

	timer := m.clk().NewTimer(DefaultVoteTimeout)
	defer timer.Stop()
	select {
	case <-session.completed:
		return session.err
	case <-ctx.Done():
		m.cancelVoteInSession(target, ctx.Err())
		<-session.completed
		return session.err
	case <-timer.C():
		m.cancelVoteInSession(target, ErrVoteInTimeout)
		<-session.completed
		return session.err
	}
}

func newVoteInSession(target *pb.ReplicaID, addr string, members []*pb.ReplicaID) *voteInSession {
	clones := make([]*pb.ReplicaID, len(members))
	for i, r := range members {
		clones[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	return &voteInSession{
		target:         proto.Clone(target).(*pb.ReplicaID),
		addr:           addr,
		membersAtStart: clones,
		ackedBy:        make(map[string]struct{}),
		nackedBy:       make(map[string]struct{}),
		completed:      make(chan struct{}),
	}
}

func (m *Manager) broadcastVoteIn(target *pb.ReplicaID, addr string) error {
	bs, err := frame.MarshalVoteIn(target, addr)
	if err != nil {
		return err
	}
	if _, err := m.conv.Send(bs); err != nil {
		return err
	}
	m.fd.NoteSent()
	return nil
}

func (m *Manager) cancelVoteInSession(target *pb.ReplicaID, err error) {
	m.mu.Lock()
	session, ok := m.voteInSessions[string(target.GetValue())]
	if ok {
		delete(m.voteInSessions, string(target.GetValue()))
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	session.done.Do(func() {
		session.err = err
		close(session.completed)
	})
}

// checkVoteInDecision evaluates the proposer-side session.
// Two-phase: when quorum Acks land with no Nacks, broadcast a
// MemberAdd commit message. Local apply happens via the pump
// when MemberAdd arrives back at us as a self-delivery.
func (m *Manager) checkVoteInDecision(session *voteInSession) {
	session.mu.Lock()
	if len(session.nackedBy) > 0 {
		session.mu.Unlock()
		m.cancelVoteInSession(session.target, ErrVoteInNacked)
		return
	}
	if session.committed {
		session.mu.Unlock()
		return
	}
	// Voter set is membersAtStart (target was NOT in ML at session
	// creation, so all current members are voters). Initiator's
	// implicit Ack is already in ackedBy; we count it.
	voterCount := len(session.membersAtStart)
	needed := voterCount/2 + 1
	if needed < 1 {
		needed = 1
	}
	ackCount := len(session.ackedBy)
	if ackCount < needed {
		session.mu.Unlock()
		return
	}
	session.committed = true
	target := session.target
	addr := session.addr
	session.mu.Unlock()

	// Quorum reached. Broadcast MemberAdd. The commit signal
	// (session.completed) fires when our own pump applies the
	// MemberAdd via handleMemberAdd.
	bs, err := frame.MarshalMemberAdd(target, addr)
	if err != nil {
		m.cancelVoteInSession(target, fmt.Errorf("marshal MemberAdd: %w", err))
		return
	}
	go m.asyncSend(bs, "MemberAdd")
}

// applyMemberAdd is invoked when handleMemberAdd processes a
// MemberAdd for `target`. It performs the local reshape and
// completes any matching session.
func (m *Manager) applyMemberAdd(target *pb.ReplicaID, addr string) {
	m.mu.Lock()
	if m.isMemberLocked(target) {
		// Already added — duplicate MemberAdd, no-op.
		m.mu.Unlock()
		return
	}
	session := m.voteInSessions[string(target.GetValue())]
	if session != nil {
		delete(m.voteInSessions, string(target.GetValue()))
	}
	m.addToMLLocked(target)
	m.mu.Unlock()

	// Grow the underlying psync.Membership at the new slot.
	if _, err := m.conv.AddMember(target); err != nil {
		m.logger.Warn("membership: psync.AddMember failed",
			"target", fmt.Sprintf("%x", target.GetValue()), "err", err)
	}

	// Start tracking heartbeats from the new replica so the FD
	// can detect failures going forward.
	m.fd.AddMember(target)

	m.notifyAdded(target, addr)

	if session != nil {
		session.done.Do(func() {
			session.err = nil
			close(session.completed)
		})
	}
}

// addToMLLocked appends target to membershipList at the end
// (insertion-order, matching psync.Membership.Add per PLAN
// §2.10.1). Caller must hold m.mu.
func (m *Manager) addToMLLocked(target *pb.ReplicaID) {
	m.membershipList = append(m.membershipList, proto.Clone(target).(*pb.ReplicaID))
}

// notifyAdded emits an Added event for downstream layers to wire
// up transport routing, etc. Logs unconditionally and invokes
// Config.OnMembershipChange (if set) SYNCHRONOUSLY — callers
// rely on routing/persistence being in place before the VoteIn
// waiter completes.
func (m *Manager) notifyAdded(target *pb.ReplicaID, addr string) {
	m.logger.Info("membership: replica added",
		"target", fmt.Sprintf("%x", target.GetValue()),
		"addr", addr)
	if cb := m.cfg.OnMembershipChange; cb != nil {
		cb(MembershipChange{
			Kind:    MembershipChangeAdded,
			Replica: proto.Clone(target).(*pb.ReplicaID),
			Addr:    addr,
		})
	}
}

// ─── receive-side handlers ────────────────────────────────────────

// handleVoteIn responds to a peer's VoteIn proposal (Phase A:
// quorum gate). Default policy: Ack iff target is not already a
// member. Receivers don't track session state — the proposer
// owns the session and broadcasts MemberAdd once quorum is
// reached. Receivers apply the addition when MemberAdd arrives
// (handleMemberAdd).
func (m *Manager) handleVoteIn(req *pb.VoteIn, sender *pb.ReplicaID) {
	target := req.GetTarget()
	if target == nil {
		return
	}
	if bytes.Equal(sender.GetValue(), m.cfg.Self.GetValue()) {
		// Self-delivery of our own VoteIn — initiator's implicit
		// Ack is already in the session.
		return
	}
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		// Someone is voting US in. We're already a member; ignore.
		return
	}
	m.mu.Lock()
	alreadyMember := m.isMemberLocked(target)
	m.mu.Unlock()
	if alreadyMember {
		// Already a member; the addition is a no-op. Acknowledge
		// so the proposer can clean up.
		m.sendVoteInAck(target)
		return
	}
	// Default policy: Ack.
	m.sendVoteInAck(target)
}

// handleMemberAdd is the commit-phase receiver: applies the
// reshape locally. PLAN §2.10.1 — MemberAdd's vector clock
// anchors the partial-order point at which the slot grows.
func (m *Manager) handleMemberAdd(req *pb.MemberAdd, sender *pb.ReplicaID) {
	target := req.GetTarget()
	if target == nil {
		return
	}
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		// Someone is announcing our addition. We're already a
		// member; ignore.
		return
	}
	m.applyMemberAdd(target, req.GetAddr())
	_ = sender
}

func (m *Manager) handleVoteInAck(ack *pb.VoteInAck, sender *pb.ReplicaID) {
	target := ack.GetTarget()
	if target == nil {
		return
	}
	m.mu.Lock()
	session, exists := m.voteInSessions[string(target.GetValue())]
	m.mu.Unlock()
	if !exists {
		return
	}
	session.mu.Lock()
	session.ackedBy[string(sender.GetValue())] = struct{}{}
	session.mu.Unlock()
	m.checkVoteInDecision(session)
}

func (m *Manager) handleVoteInNack(nack *pb.VoteInNack, sender *pb.ReplicaID) {
	target := nack.GetTarget()
	if target == nil {
		return
	}
	m.mu.Lock()
	session, exists := m.voteInSessions[string(target.GetValue())]
	m.mu.Unlock()
	if !exists {
		return
	}
	session.mu.Lock()
	session.nackedBy[string(sender.GetValue())] = struct{}{}
	session.mu.Unlock()
	m.checkVoteInDecision(session)
}

func (m *Manager) sendVoteInAck(target *pb.ReplicaID) {
	bs, err := frame.MarshalVoteInAck(target)
	if err != nil {
		m.logger.Warn("membership: marshal VoteInAck", "err", err)
		return
	}
	go m.asyncSend(bs, "VoteInAck")
}

func (m *Manager) sendVoteInNack(target *pb.ReplicaID) {
	bs, err := frame.MarshalVoteInNack(target)
	if err != nil {
		m.logger.Warn("membership: marshal VoteInNack", "err", err)
		return
	}
	go m.asyncSend(bs, "VoteInNack")
}

// _ keeps sendVoteInNack referenced; future commits will use it
// when an application policy hook says to Nack.
var _ = (*Manager)(nil).sendVoteInNack
