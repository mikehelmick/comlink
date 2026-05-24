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

package psync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// DefaultRestartRetryInterval is the period between RestartMessage
// re-broadcasts when no peer has acknowledged yet.
const DefaultRestartRetryInterval = 250 * time.Millisecond

// ErrRestartTimedOut is returned by Restart when ctx expires before
// any peer acknowledges.
var ErrRestartTimedOut = errors.New("psync: restart timed out without any peer ack")

// Restart announces this replica is rebuilding state after a crash
// (paper §2.3). It performs a three-step recovery:
//
//  1. Replay the local MessageLog into the in-memory context graph.
//     This recovers everything that was durable at crash time.
//  2. Broadcast a RestartMessage to every other active member,
//     retrying at intervals until at least one peer responds with
//     a RestartAck.
//  3. Issue a LostMessageRequest for each leaf in the received
//     ack(s) — the existing lost-message protocol transitively
//     pulls in ancestors that aren't already in the graph (the
//     "pruned region" recovery from PLAN §1 exit criteria).
//
// Returns once the first RestartAck has been processed. If no peer
// has acknowledged by the time ctx is done, returns
// ErrRestartTimedOut. Background ack collection continues until
// the Conversation is closed.
func (c *Conversation) Restart(ctx context.Context) error {
	// 1. Replay local log into the graph.
	resp := c.srv.Call(replayLogRequest{deliver: false})
	if rr := resp.(replayLogResponse); rr.err != nil {
		return fmt.Errorf("psync: Restart: log replay: %w", rr.err)
	}
	// Subscribe to RestartAcks for the duration of this Restart.
	ackCh := make(chan *pb.RestartAck, 16)
	c.restartMu.Lock()
	if c.restartAckChan != nil {
		c.restartMu.Unlock()
		return errors.New("psync: Restart already in progress")
	}
	c.restartAckChan = ackCh
	c.restartMu.Unlock()
	defer func() {
		c.restartMu.Lock()
		c.restartAckChan = nil
		c.restartMu.Unlock()
	}()

	timer := c.clk.NewTimer(0) // fire immediately for the first broadcast
	defer timer.Stop()

	for {
		select {
		case <-timer.C():
			c.broadcastRestartMessage(ctx)
			timer.Reset(DefaultRestartRetryInterval)

		case ack := <-ackCh:
			// Got our first ack. Trigger lost-message fetches for
			// each leaf — the lost-message protocol will pull in
			// ancestors transitively.
			c.requestLeavesAfterRestart(ctx, ack)
			return nil

		case <-ctx.Done():
			return ErrRestartTimedOut
		}
	}
}

// broadcastRestartMessage sends a RestartMessage to every other
// member. Errors are logged and swallowed; the retry loop will fire
// again.
func (c *Conversation) broadcastRestartMessage(ctx context.Context) {
	// We need access to the membership list. The conversation's
	// genserver owns it; use a synchronous Call to fetch the peer
	// set rather than duplicating membership tracking on the
	// outside. Simpler: we already hold cfg.Members, but that's
	// the original (unsorted) input. Re-derive sorted membership
	// from cfg.Members to skip ourselves.
	wireBytes, err := MarshalRestartMessage(c.cfg.Self)
	if err != nil {
		c.logger.Warn("psync: marshal RestartMessage", "err", err)
		return
	}
	for _, peer := range c.cfg.Members {
		if bytes.Equal(peer.GetValue(), c.cfg.Self.GetValue()) {
			continue
		}
		if err := c.cfg.Network.Send(ctx, peer, wireBytes); err != nil {
			c.logger.Debug("psync: send RestartMessage failed",
				"peer", fmt.Sprintf("%x", peer.GetValue()), "err", err)
		}
	}
}

// requestLeavesAfterRestart issues a LostMessageRequest for each
// leaf in ack. Each request goes to ack.Responder (the peer that
// sent us the leaf set, who is guaranteed to have those messages
// in its log).
func (c *Conversation) requestLeavesAfterRestart(ctx context.Context, ack *pb.RestartAck) {
	responder := ack.GetResponder()
	if responder == nil {
		return
	}
	for _, leaf := range ack.GetLeaves() {
		// senderSeq is encoded into the leaf's vector_clock at the
		// sender's slot. Compute via a fresh Membership view of the
		// configured members; this works because Phase 1 assumes
		// static membership.
		senderSeq, err := membershipSenderSeqFromConfig(c.cfg.Members, leaf)
		if err != nil {
			c.logger.Warn("psync: cannot derive sender_seq for leaf",
				"leaf", leaf, "err", err)
			continue
		}
		wireBytes, err := MarshalLostMessageRequest(leaf.GetSender(), senderSeq)
		if err != nil {
			c.logger.Warn("psync: marshal LostMessageRequest for leaf", "err", err)
			continue
		}
		if err := c.cfg.Network.Send(ctx, responder, wireBytes); err != nil {
			c.logger.Warn("psync: request leaf after restart failed", "err", err)
		}
	}
}

// membershipSenderSeqFromConfig derives the sender's seq from a
// MessageID's vector_clock using the configured (static) member
// set. Phase 3 will replace this with a runtime membership lookup.
func membershipSenderSeqFromConfig(members []*pb.ReplicaID, id *pb.MessageID) (uint64, error) {
	m := NewMembership(members)
	return m.SenderSeq(id)
}

// ─── serverImpl handlers ──────────────────────────────────────────

// handleRestartMessage responds to a peer's RestartMessage by
// sending back our current leaf set as a RestartAck.
func (s *serverImpl) handleRestartMessage(st *state, msg *pb.RestartMessage, _ *pb.ReplicaID) {
	restarter := msg.GetRestarter()
	if restarter == nil {
		return
	}
	leaves := st.graph.Leaves()
	leafIDs := make([]*pb.MessageID, 0, len(leaves))
	for _, n := range leaves {
		leafIDs = append(leafIDs, proto.Clone(n.Envelope.GetId()).(*pb.MessageID))
	}
	wireBytes, err := MarshalRestartAck(s.self, leafIDs)
	if err != nil {
		s.logger.Warn("psync: marshal RestartAck", "err", err)
		return
	}
	if err := s.network.Send(context.Background(), restarter, wireBytes); err != nil {
		s.logger.Warn("psync: send RestartAck failed", "err", err)
	}
}

// handleRestartAckIncoming forwards an incoming RestartAck to the
// onRestartAck callback (set by Conversation while a Restart is in
// progress).
func (s *serverImpl) handleRestartAckIncoming(_ *state, ack *pb.RestartAck, _ *pb.ReplicaID) {
	if s.onRestartAck != nil {
		s.onRestartAck(ack)
	}
}
