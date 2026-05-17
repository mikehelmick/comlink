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
	"time"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// DefaultVoteTimeout is how long Manager.VoteOut waits for
// responses from peers before giving up.
const DefaultVoteTimeout = 10 * time.Second

// Errors returned by VoteOut.
var (
	// ErrVoteOutTargetNotMember means target is not in current ML.
	ErrVoteOutTargetNotMember = errors.New("membership: VoteOut target not in current membership list")
	// ErrVoteOutTargetIsSelf means the caller tried to vote
	// themselves out via this API. Use a separate Leave
	// operation (out of scope for v1).
	ErrVoteOutTargetIsSelf = errors.New("membership: VoteOut target is self")
	// ErrVoteOutNacked means at least one peer disagreed with the
	// removal (Nack received) — vote aborted.
	ErrVoteOutNacked = errors.New("membership: VoteOut rejected by Nack")
	// ErrVoteOutTimeout means the vote timed out before quorum
	// was reached.
	ErrVoteOutTimeout = errors.New("membership: VoteOut timed out before quorum")
	// ErrVoteOutInProgress means another VoteOut for the same
	// target is already in flight; only one at a time per target.
	ErrVoteOutInProgress = errors.New("membership: VoteOut already in progress for target")
	// ErrPartitionMinority means this replica is in the minority
	// partition (PLAN §2.11) and refuses to initiate ML-mutating
	// operations until quorum is restored.
	ErrPartitionMinority = errors.New("membership: refusing operation in minority partition")
)

// voteOutSession tracks an in-flight VoteOut. Sessions are keyed
// by target (string of ReplicaID bytes). One session per target
// at a time.
type voteOutSession struct {
	target *pb.ReplicaID
	// membersAtStart is the snapshot of ML at the moment the
	// session was created. The voter set is membersAtStart
	// minus target.
	membersAtStart []*pb.ReplicaID
	ackedBy        map[string]struct{}
	nackedBy       map[string]struct{}
	// done is closed once the session is decided (accepted /
	// rejected / timed out). The decision waiter blocks on it.
	done sync.Once
	// final outcome — set before done is closed.
	err          error
	completed    chan struct{}
	// mu serializes mutations to ackedBy/nackedBy so concurrent
	// receive paths don't race.
	mu sync.Mutex
}

// VoteOut initiates a permanent removal of target from the
// conversation's membership list. Per PLAN §2.13:
//
//   - Caller must not be the target (use a separate Leave
//     operation if/when we add one).
//   - The Manager broadcasts a VoteOut(target) event into the
//     conversation. Peers respond with VoteOutAck (they don't
//     know target is alive — they "agree to remove") or
//     VoteOutNack (they have evidence target is alive).
//   - Quorum rule: any Nack aborts the vote. Strict majority of
//     voters (members minus target) must Ack.
//   - On accepted vote: target is frozen in psync's Membership
//     (slot stays in place per §2.10.1) and removed from the
//     local ML; the FailureDetector stops tracking target.
//
// This call blocks until the vote completes (all expected
// responses arrived OR ctx is done OR DefaultVoteTimeout
// elapsed). Returns nil on accepted, an error variant on rejected.
func (m *Manager) VoteOut(ctx context.Context, target *pb.ReplicaID) error {
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		return ErrVoteOutTargetIsSelf
	}
	m.mu.Lock()
	if !m.isMemberLocked(target) {
		m.mu.Unlock()
		return ErrVoteOutTargetNotMember
	}
	if !m.inMajorityLocked() {
		m.mu.Unlock()
		return ErrPartitionMinority
	}
	if _, exists := m.voteOutSessions[string(target.GetValue())]; exists {
		m.mu.Unlock()
		return ErrVoteOutInProgress
	}
	session := newVoteOutSession(target, m.membershipList)
	// As initiator we implicitly Ack our own vote.
	session.ackedBy[string(m.cfg.Self.GetValue())] = struct{}{}
	m.voteOutSessions[string(target.GetValue())] = session
	m.mu.Unlock()

	// Broadcast VoteOut.
	if err := m.broadcastVoteOut(target); err != nil {
		m.cancelSession(target, fmt.Errorf("broadcast VoteOut: %w", err))
		<-session.completed
		return session.err
	}

	// Maybe we already have quorum (1-replica conversation, etc.).
	m.checkVoteOutDecision(session)

	// Wait for decision, ctx, or timeout.
	timeout := DefaultVoteTimeout
	timer := m.clk().NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-session.completed:
		return session.err
	case <-ctx.Done():
		m.cancelSession(target, ctx.Err())
		<-session.completed
		return session.err
	case <-timer.C():
		m.cancelSession(target, ErrVoteOutTimeout)
		<-session.completed
		return session.err
	}
}

func newVoteOutSession(target *pb.ReplicaID, membersAtStart []*pb.ReplicaID) *voteOutSession {
	clones := make([]*pb.ReplicaID, len(membersAtStart))
	for i, r := range membersAtStart {
		clones[i] = proto.Clone(r).(*pb.ReplicaID)
	}
	return &voteOutSession{
		target:         proto.Clone(target).(*pb.ReplicaID),
		membersAtStart: clones,
		ackedBy:        make(map[string]struct{}),
		nackedBy:       make(map[string]struct{}),
		completed:      make(chan struct{}),
	}
}

// isMemberLocked reports whether replica is in the current ML.
// Caller must hold m.mu.
func (m *Manager) isMemberLocked(replica *pb.ReplicaID) bool {
	for _, mem := range m.membershipList {
		if bytes.Equal(mem.GetValue(), replica.GetValue()) {
			return true
		}
	}
	return false
}

// inMajorityLocked reports whether this replica is in the majority
// partition (PLAN §2.11). Caller must hold m.mu.
func (m *Manager) inMajorityLocked() bool {
	return len(m.membershipList)*2 > m.cfg.InitialGroupSize
}

// broadcastVoteOut sends VoteOut(target) into the conversation.
// VoteOut is initiated from a caller goroutine (Manager.VoteOut),
// not from inside the pump, so a synchronous Send is fine here.
func (m *Manager) broadcastVoteOut(target *pb.ReplicaID) error {
	bs, err := frame.MarshalVoteOut(target)
	if err != nil {
		return err
	}
	if _, err := m.conv.Send(bs); err != nil {
		return err
	}
	m.fd.NoteSent()
	return nil
}

// cancelSession marks the named target's session as decided with
// err and closes its completed channel. If no session exists, this
// is a no-op.
func (m *Manager) cancelSession(target *pb.ReplicaID, err error) {
	m.mu.Lock()
	session, ok := m.voteOutSessions[string(target.GetValue())]
	if ok {
		delete(m.voteOutSessions, string(target.GetValue()))
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

// checkVoteOutDecision evaluates session and acts if a decision
// can be made. Called whenever the session's tally changes.
func (m *Manager) checkVoteOutDecision(session *voteOutSession) {
	session.mu.Lock()
	if len(session.nackedBy) > 0 {
		session.mu.Unlock()
		m.cancelSession(session.target, ErrVoteOutNacked)
		return
	}
	voterCount := len(session.membersAtStart) - 1 // exclude target
	needed := voterCount/2 + 1
	if needed < 1 {
		needed = 1
	}
	ackCount := len(session.ackedBy)
	session.mu.Unlock()
	if ackCount < needed {
		return
	}

	// Decision: accepted. Apply removal locally.
	m.mu.Lock()
	delete(m.voteOutSessions, string(session.target.GetValue()))
	m.removeFromMLLocked(session.target)
	m.mu.Unlock()

	// Tell psync to freeze the target's slot.
	if err := m.conv.FreezeMember(session.target); err != nil {
		m.logger.Warn("membership: psync FreezeMember failed",
			"target", fmt.Sprintf("%x", session.target.GetValue()), "err", err)
	}
	// Stop the FailureDetector from tracking target.
	m.fd.RemoveMember(session.target)
	// Clear any soft-suspicion entry for target.
	m.clearSuspicion(session.target)
	// Drop target's watermark from the trim tracker so the
	// safe-trim frontier no longer waits on them.
	if m.trim != nil {
		m.trim.Forget(session.target)
		m.maybeTrim()
	}

	m.notifyRemoved(session.target)

	session.done.Do(func() {
		session.err = nil
		close(session.completed)
	})
}

// notifyRemoved emits a Removed event for downstream layers
// (transport routing teardown, stable.Storage persistence, etc).
// Callback runs on a goroutine — must not re-enter the Manager.
func (m *Manager) notifyRemoved(target *pb.ReplicaID) {
	m.logger.Info("membership: replica removed",
		"target", fmt.Sprintf("%x", target.GetValue()))
	if cb := m.cfg.OnMembershipChange; cb != nil {
		event := MembershipChange{
			Kind:    MembershipChangeRemoved,
			Replica: proto.Clone(target).(*pb.ReplicaID),
		}
		go cb(event)
	}
}

// removeFromMLLocked drops target from membershipList.
// Caller must hold m.mu.
func (m *Manager) removeFromMLLocked(target *pb.ReplicaID) {
	kept := m.membershipList[:0]
	for _, mem := range m.membershipList {
		if !bytes.Equal(mem.GetValue(), target.GetValue()) {
			kept = append(kept, mem)
		}
	}
	m.membershipList = kept
}

// ─── receive-side handlers (overrides Phase 3(c) stubs) ───────────

// handleVoteOut decides our response (Ack or Nack) and tracks the
// session locally so we can apply the removal when the decision
// arrives. PLAN §2.13: Ack iff we also currently suspect target.
//
// If the sender of the VoteOut is self, this is the self-delivery
// of our own initiated vote — the initiator's implicit Ack was
// already recorded at session-creation time, and we don't generate
// a fresh response (that would risk double-counting and, worse,
// self-Nacking).
func (m *Manager) handleVoteOut(req *pb.VoteOut, sender *pb.ReplicaID) {
	target := req.GetTarget()
	if target == nil {
		return
	}
	if bytes.Equal(target.GetValue(), m.cfg.Self.GetValue()) {
		// Someone is trying to vote us out. We're alive (otherwise
		// we wouldn't be processing this), so disagree.
		m.sendVoteOutNack(target)
		return
	}
	if bytes.Equal(sender.GetValue(), m.cfg.Self.GetValue()) {
		// Self-delivery of our own VoteOut. No response needed.
		return
	}

	// Local session bookkeeping (so we can apply the decision).
	m.mu.Lock()
	if !m.isMemberLocked(target) {
		m.mu.Unlock()
		return
	}
	session, exists := m.voteOutSessions[string(target.GetValue())]
	if !exists {
		session = newVoteOutSession(target, m.membershipList)
		m.voteOutSessions[string(target.GetValue())] = session
	}
	// Record the initiator's implicit Ack.
	session.mu.Lock()
	session.ackedBy[string(sender.GetValue())] = struct{}{}
	session.mu.Unlock()
	m.mu.Unlock()

	// Decide: do we agree?
	ack := m.fd.Suspected(target)
	// Record our own response in the session.
	session.mu.Lock()
	if ack {
		session.ackedBy[string(m.cfg.Self.GetValue())] = struct{}{}
	} else {
		session.nackedBy[string(m.cfg.Self.GetValue())] = struct{}{}
	}
	session.mu.Unlock()

	// Send our response.
	if ack {
		m.sendVoteOutAck(target)
	} else {
		m.sendVoteOutNack(target)
	}

	m.checkVoteOutDecision(session)
}

// handleVoteOutAck records a peer's Ack and re-checks decision.
func (m *Manager) handleVoteOutAck(ack *pb.VoteOutAck, sender *pb.ReplicaID) {
	target := ack.GetTarget()
	if target == nil {
		return
	}
	m.mu.Lock()
	session, exists := m.voteOutSessions[string(target.GetValue())]
	m.mu.Unlock()
	if !exists {
		return
	}
	session.mu.Lock()
	session.ackedBy[string(sender.GetValue())] = struct{}{}
	session.mu.Unlock()
	m.checkVoteOutDecision(session)
}

// handleVoteOutNack records a Nack — single Nack aborts the vote.
func (m *Manager) handleVoteOutNack(nack *pb.VoteOutNack, sender *pb.ReplicaID) {
	target := nack.GetTarget()
	if target == nil {
		return
	}
	m.mu.Lock()
	session, exists := m.voteOutSessions[string(target.GetValue())]
	m.mu.Unlock()
	if !exists {
		return
	}
	session.mu.Lock()
	session.nackedBy[string(sender.GetValue())] = struct{}{}
	session.mu.Unlock()
	m.checkVoteOutDecision(session)
}

// sendVoteOutAck/sendVoteOutNack are called from inside the
// pump's dispatch path (handleVoteOut). They MUST be async
// (asyncSend goroutine) to avoid the re-entrant deadlock — the
// pump is draining psync's deliver channel and conv.Send would
// block waiting for the genserver, which is itself waiting to
// push the next delivery onto the channel the pump is supposed
// to drain.
func (m *Manager) sendVoteOutAck(target *pb.ReplicaID) {
	bs, err := frame.MarshalVoteOutAck(target)
	if err != nil {
		m.logger.Warn("membership: marshal VoteOutAck", "err", err)
		return
	}
	go m.asyncSend(bs, "VoteOutAck")
}

func (m *Manager) sendVoteOutNack(target *pb.ReplicaID) {
	bs, err := frame.MarshalVoteOutNack(target)
	if err != nil {
		m.logger.Warn("membership: marshal VoteOutNack", "err", err)
		return
	}
	go m.asyncSend(bs, "VoteOutNack")
}

// clk returns the configured clock or a system clock fallback.
func (m *Manager) clk() clock.Clock {
	if m.cfg.Clock != nil {
		return m.cfg.Clock
	}
	return clock.NewSystem()
}

// _ keeps the time import live (DefaultVoteTimeout is time.Duration).
var _ = time.Duration(0)
