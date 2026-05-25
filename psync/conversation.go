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
	"log/slog"
	"sync"

	"github.com/mikehelmick/go-functional/genserver"

	"github.com/mikehelmick/comlink/clock"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport"
	"google.golang.org/protobuf/proto"
)

// Delivery is the unit handed to the application's Recv channel
// when Psync delivers an envelope. Node is included for callers
// that want to inspect graph context (parents, wave) without a
// separate lookup.
type Delivery struct {
	Envelope *pb.Envelope
	Node     *Node
}

// Config configures a Conversation. All fields are required unless
// noted otherwise.
type Config struct {
	// Conversation identity (PLAN §2.10).
	ConversationID *pb.ConversationID
	// This replica's ID; must appear in Members.
	Self *pb.ReplicaID
	// The full participant set. Phase 1 assumes static membership;
	// reshape comes in Phase 3.
	Members []*pb.ReplicaID

	// Transport.
	Network transport.Network
	// Durable message log; the Conversation is bound to its
	// ConversationID() at construction.
	Log clog.MessageLog
	// Stable storage for non-message persistent state (mask, etc.).
	Storage stable.Storage

	// Capacity of the application-facing delivery channel. If the
	// application doesn't drain promptly, the conversation
	// backpressures up through the network input pump.
	// Default 256.
	DeliveryBufSize int

	// Optional logger. If nil, slog.Default() is used.
	Logger *slog.Logger

	// Optional clock; used by Restart's retry loop. Defaults to
	// clock.NewSystem(). Tests use clock.Manual to drive restart
	// retries deterministically.
	Clock clock.Clock

	// OnReceive, if non-nil, is called on EVERY successfully-
	// decoded inbound envelope, BEFORE any graph-Insert /
	// deferral / dedup. Hook for liveness signals that must
	// survive Order-layer back-pressure: even if the Order
	// wave gate is closed and deliveries are stalled, a peer's
	// heartbeats can still keep the receiver's failure detector
	// happy.
	//
	// Runs synchronously in the pump goroutine. Must be fast
	// and non-blocking — no I/O, no genserver re-entry.
	OnReceive func(from *pb.ReplicaID)
}

// Conversation is one replica's view of a Psync conversation.
//
// PLAN §2.2: each protocol layer is a per-replica GenServer. We use
// github.com/mikehelmick/go-functional/genserver: the Server
// implementation lives in serverImpl (immutable handler), and the
// mutable state (graph, deferred queue, etc.) is the generic state
// type the genserver carries between callbacks.
//
// Public methods (Send, Maskin, Maskout) marshal a Request onto the
// genserver via Call; the network input pump goroutine forwards
// inbound transport frames via Cast.
type Conversation struct {
	srv      *genserver.GenServer[*state, request, response]
	cfg      Config
	logger   *slog.Logger
	clk      clock.Clock
	deliver  chan Delivery
	pumpDone chan struct{}
	closeMu  sync.Mutex
	closed   bool

	// Restart subscription. Set by Restart() while a restart is in
	// progress; serverImpl pushes incoming RestartAcks here.
	restartMu       sync.Mutex
	restartAckChan  chan *pb.RestartAck
}

// state is the mutable per-conversation data the genserver carries
// across callbacks.
type state struct {
	graph *Graph
	// Deferred envelopes — keyed by (sender, seq) of the deferred
	// message itself.
	deferred map[indexKey]*pendingEnvelope
	// Index for "what was waiting on this parent?" lookups.
	deferredByNeed map[indexKey]map[indexKey]struct{}
	// Outstanding lost-message requests we've sent — used to
	// suppress duplicate requests for the same parent.
	outstanding map[indexKey]struct{}
}

type pendingEnvelope struct {
	env  *pb.Envelope
	from *pb.ReplicaID
	// waiting: (sender, seq) tuples this envelope still needs.
	waiting map[indexKey]struct{}
}

type indexKey struct {
	sender string
	seq    uint64
}

func makeKey(sender []byte, seq uint64) indexKey {
	return indexKey{sender: string(sender), seq: seq}
}

// ─── request / response types (genserver Req / Resp) ──────────────
//
// Cast-only requests deliver no reply (their "response" is empty).
// Call requests carry their reply payload in their Response variant.

type request interface{ isRequest() }

type sendRequest struct{ payload []byte }
type incomingRequest struct {
	from *pb.ReplicaID
	data []byte
}
type maskRequest struct {
	in      bool
	replica *pb.ReplicaID
	ctx     context.Context
}
type replayLogRequest struct {
	// deliver controls whether each successfully-inserted envelope
	// is also pushed to the deliver channel. Restart (peer catch-up
	// protocol) sets false — it only rebuilds the graph so we can
	// answer LostMessageRequest queries. Substrate's auto-recover
	// path (ReplayLog) sets true so messages re-flow through Order
	// → SM, rebuilding SM state from scratch.
	deliver bool
}
type waveCompleteRequest struct{ wave uint64 }
type messagesInWaveRequest struct{ wave uint64 }
type stableMessageIDsRequest struct{}
type freezeMemberRequest struct{ replica *pb.ReplicaID }
type membershipRequest struct{}
type addMemberRequest struct{ replica *pb.ReplicaID }

func (sendRequest) isRequest()             {}
func (incomingRequest) isRequest()         {}
func (maskRequest) isRequest()             {}
func (replayLogRequest) isRequest()        {}
func (waveCompleteRequest) isRequest()     {}
func (messagesInWaveRequest) isRequest()   {}
func (stableMessageIDsRequest) isRequest() {}
func (freezeMemberRequest) isRequest()     {}
func (membershipRequest) isRequest()       {}
func (addMemberRequest) isRequest()        {}

type response interface{ isResponse() }

type sendResponse struct {
	id  *pb.MessageID
	err error
}
type maskResponse struct{ err error }
type replayLogResponse struct {
	inserted int
	err      error
	// envelopes is the in-order set of replayed envelopes that
	// the caller should re-deliver to the application's Order
	// layer. Populated only when replayLogRequest.deliver is
	// true. ReplayLog drains this on a separate goroutine
	// AFTER the genserver returns, so the genserver is not
	// blocked while the deliver channel might be full (which
	// could starve concurrent Sends — see Phase 9 bug
	// investigation).
	envelopes []*pb.Envelope
}
type waveCompleteResponse struct{ complete bool }
type messagesInWaveResponse struct {
	envelopes []*pb.Envelope
}
type stableMessageIDsResponse struct {
	ids []*pb.MessageID
}
type freezeMemberResponse struct{ err error }
type membershipResponse struct{ membership *Membership }
type addMemberResponse struct {
	slot int
	err  error
}
type emptyResponse struct{}

func (sendResponse) isResponse()             {}
func (maskResponse) isResponse()             {}
func (replayLogResponse) isResponse()        {}
func (waveCompleteResponse) isResponse()     {}
func (messagesInWaveResponse) isResponse()   {}
func (stableMessageIDsResponse) isResponse() {}
func (freezeMemberResponse) isResponse()     {}
func (membershipResponse) isResponse()       {}
func (addMemberResponse) isResponse()        {}
func (emptyResponse) isResponse()            {}

// serverImpl is the immutable handler that implements
// genserver.Server. It owns no mutable state — everything runtime
// lives in `*state` carried across callbacks. References here are
// for the network/log/storage/etc. handles that don't mutate after
// construction.
type serverImpl struct {
	convID     *pb.ConversationID
	self       *pb.ReplicaID
	selfSlot   int
	membership *Membership
	log        clog.MessageLog
	storage    stable.Storage
	network    transport.Network
	mask       *Mask
	deliver    chan<- Delivery
	logger     *slog.Logger
	// onRestartAck, if non-nil, is invoked on every received
	// RestartAck so the Conversation's Restart() can collect them.
	// Stored as a func to avoid coupling serverImpl to Conversation
	// directly.
	onRestartAck func(*pb.RestartAck)
	// onReceive, if non-nil, fires on every successfully-decoded
	// inbound frame BEFORE graph/deferral logic. Phase 10(a):
	// substrates wire this to FD.NoteReceived so peer liveness
	// signals survive Order back-pressure.
	onReceive func(from *pb.ReplicaID)
}

// Init creates the initial mutable state.
func (s *serverImpl) Init() *state {
	return &state{
		graph:          NewGraph(s.membership),
		deferred:       make(map[indexKey]*pendingEnvelope),
		deferredByNeed: make(map[indexKey]map[indexKey]struct{}),
		outstanding:    make(map[indexKey]struct{}),
	}
}

// HandleCall processes synchronous requests.
func (s *serverImpl) HandleCall(req request, st *state) (response, *state) {
	switch r := req.(type) {
	case sendRequest:
		id, err := s.handleSend(st, r.payload)
		return sendResponse{id: id, err: err}, st
	case maskRequest:
		var err error
		if r.in {
			err = s.mask.Maskin(r.ctx, r.replica)
		} else {
			err = s.mask.Maskout(r.ctx, r.replica)
		}
		return maskResponse{err: err}, st
	case replayLogRequest:
		n, envs, err := s.handleReplay(st, r.deliver)
		return replayLogResponse{inserted: n, err: err, envelopes: envs}, st
	case waveCompleteRequest:
		return waveCompleteResponse{
			complete: WaveComplete(st.graph, r.wave, StandardChecker{}),
		}, st
	case messagesInWaveRequest:
		nodes := st.graph.MessagesInWave(r.wave)
		envs := make([]*pb.Envelope, 0, len(nodes))
		for _, n := range nodes {
			envs = append(envs, proto.Clone(n.Envelope).(*pb.Envelope))
		}
		return messagesInWaveResponse{envelopes: envs}, st
	case stableMessageIDsRequest:
		stable := StableNodes(st.graph, StandardChecker{})
		ids := make([]*pb.MessageID, 0, len(stable))
		for _, n := range stable {
			ids = append(ids, proto.Clone(n.Envelope.GetId()).(*pb.MessageID))
		}
		return stableMessageIDsResponse{ids: ids}, st
	case freezeMemberRequest:
		err := s.membership.Freeze(r.replica)
		return freezeMemberResponse{err: err}, st
	case membershipRequest:
		return membershipResponse{membership: s.membership.Clone()}, st
	case addMemberRequest:
		slot, err := s.membership.Add(r.replica)
		return addMemberResponse{slot: slot, err: err}, st
	default:
		return emptyResponse{}, st
	}
}

// HandleCast processes asynchronous notifications.
func (s *serverImpl) HandleCast(msg request, st *state) *state {
	if r, ok := msg.(incomingRequest); ok {
		s.handleIncoming(st, r.from, r.data)
	}
	return st
}

// ─── construction & lifecycle ─────────────────────────────────────

// New constructs a Conversation. Self must be a member of cfg.Members.
// The Log must be bound to cfg.ConversationID; the Mask is loaded
// from cfg.Storage.
func New(ctx context.Context, cfg Config) (*Conversation, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if !proto.Equal(cfg.Log.ConversationID(), cfg.ConversationID) {
		return nil, fmt.Errorf("psync: log is bound to a different ConversationID")
	}
	mask, err := LoadMask(ctx, cfg.Storage, MaskStorageKey)
	if err != nil {
		return nil, fmt.Errorf("psync: load mask: %w", err)
	}
	membership := NewMembership(cfg.Members)
	selfSlot := membership.SlotOf(cfg.Self)
	if selfSlot < 0 {
		return nil, fmt.Errorf("psync: Self %x not in Members", cfg.Self.GetValue())
	}
	bufSize := cfg.DeliveryBufSize
	if bufSize <= 0 {
		bufSize = 256
	}
	deliver := make(chan Delivery, bufSize)

	clk := cfg.Clock
	if clk == nil {
		clk = clock.NewSystem()
	}

	impl := &serverImpl{
		convID:     proto.Clone(cfg.ConversationID).(*pb.ConversationID),
		self:       proto.Clone(cfg.Self).(*pb.ReplicaID),
		selfSlot:   selfSlot,
		membership: membership,
		log:        cfg.Log,
		storage:    cfg.Storage,
		network:    cfg.Network,
		mask:       mask,
		deliver:    deliver,
		logger:     logger,
		onReceive:  cfg.OnReceive,
	}

	c := &Conversation{
		cfg:      cfg,
		logger:   logger,
		clk:      clk,
		deliver:  deliver,
		pumpDone: make(chan struct{}),
	}
	// Wire the restart-ack callback before starting the server so we
	// don't race with any incoming RestartAck.
	impl.onRestartAck = c.deliverRestartAck
	c.srv = genserver.Start[*state, request, response](impl)

	go c.pumpInput(ctx)
	return c, nil
}

// deliverRestartAck pushes ack to the active restart listener (if
// any). Non-blocking — drops if no one is listening or the channel
// is full.
func (c *Conversation) deliverRestartAck(ack *pb.RestartAck) {
	c.restartMu.Lock()
	ch := c.restartAckChan
	c.restartMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- ack:
	default:
	}
}

func validateConfig(cfg Config) error {
	if cfg.ConversationID == nil {
		return errors.New("psync: Config.ConversationID is required")
	}
	if cfg.Self == nil {
		return errors.New("psync: Config.Self is required")
	}
	if len(cfg.Members) == 0 {
		return errors.New("psync: Config.Members must be non-empty")
	}
	if cfg.Network == nil {
		return errors.New("psync: Config.Network is required")
	}
	if cfg.Log == nil {
		return errors.New("psync: Config.Log is required")
	}
	if cfg.Storage == nil {
		return errors.New("psync: Config.Storage is required")
	}
	return nil
}

// pumpInput reads from the network and casts incomingRequest into
// the genserver. Exits when the network's Recv channel closes or
// ctx is done.
func (c *Conversation) pumpInput(ctx context.Context) {
	defer close(c.pumpDone)
	for {
		select {
		case in, ok := <-c.cfg.Network.Recv():
			if !ok {
				return
			}
			c.srv.Cast(incomingRequest{from: in.From, data: in.Payload})
		case <-ctx.Done():
			return
		}
	}
}

// ─── public API ───────────────────────────────────────────────────

// Send durably appends payload to the local log and multicasts it
// to every other active member. Self-delivers to this replica's
// Recv channel as well.
func (c *Conversation) Send(payload []byte) (*pb.MessageID, error) {
	resp := c.srv.Call(sendRequest{payload: payload})
	r := resp.(sendResponse)
	return r.id, r.err
}

// Recv returns the application-facing delivery channel. Closed
// after Close.
func (c *Conversation) Recv() <-chan Delivery {
	return c.deliver
}

// Maskout marks replica as masked.
func (c *Conversation) Maskout(ctx context.Context, replica *pb.ReplicaID) error {
	resp := c.srv.Call(maskRequest{in: false, replica: replica, ctx: ctx})
	return resp.(maskResponse).err
}

// Maskin removes replica from the mask.
func (c *Conversation) Maskin(ctx context.Context, replica *pb.ReplicaID) error {
	resp := c.srv.Call(maskRequest{in: true, replica: replica, ctx: ctx})
	return resp.(maskResponse).err
}

// ReplayLog replays every entry in the local message log: each
// envelope is inserted into the in-memory graph AND pushed to the
// deliver channel. Used by Substrate.NewSubstrate to rebuild
// StateMachine state on restart — replayed envelopes flow through
// the Order layer exactly as if they had just been received,
// giving the SM the same prefix it had pre-crash.
//
// Returns the number of envelopes successfully replayed. Safe to
// call once during construction; calling it after live traffic has
// begun is allowed but most callers won't want to (everything in
// the log is already in the graph by then, so all inserts dedup
// and nothing is delivered).
//
// Caveat: replay pushes to the (buffered) deliver channel. If the
// Order layer is not actively draining, the genserver Call will
// block until it does. Callers must have the Order pump running
// before invoking ReplayLog.
func (c *Conversation) ReplayLog(ctx context.Context) (int, error) {
	resp := c.srv.Call(replayLogRequest{deliver: true})
	rr := resp.(replayLogResponse)
	if rr.err != nil {
		return rr.inserted, rr.err
	}
	// Drain the replayed envelopes into the deliver channel on a
	// goroutine — pushing here would block this call if deliver
	// is full, but the genserver is already free (we got our
	// response). The goroutine binds to ctx so it cleans up on
	// Conversation Close. Per-envelope push with select-on-ctx
	// avoids stranded goroutines if the deliver channel is
	// blocked indefinitely (the Order layer's gates haven't
	// opened, etc).
	go func(envs []*pb.Envelope) {
		for _, env := range envs {
			select {
			case c.deliver <- Delivery{Envelope: env}:
			case <-ctx.Done():
				return
			}
		}
	}(rr.envelopes)
	return rr.inserted, nil
}

// Membership returns this conversation's LIVE membership view —
// reflecting any post-construction FreezeMember / AddMember
// mutations. Internally synchronized via a genserver Call so the
// returned snapshot is always consistent with the conversation's
// internal state (Phase 7(a) fix: earlier versions returned a
// fresh NewMembership(cfg.Members) here, which never reflected
// freezes and led to Order layers stalling on stale membership).
//
// The returned Membership is a SNAPSHOT — safe to read but not
// mutated by the caller; subsequent Freeze/Add do not retroactively
// update returned snapshots. Callers that need a live view should
// re-call this method.
func (c *Conversation) Membership() *Membership {
	resp := c.srv.Call(membershipRequest{}).(membershipResponse)
	return resp.membership
}

// WaveComplete reports whether wave w meets the standard wave-
// completion condition (paper §2.3): some message in wave w is
// stable. Order layers (Phase 2) use this to decide when a wave's
// messages can be applied as a group.
//
// Note PLAN's stability §2.10 caveat: this uses the standard
// stability rule; Membership-protocol-specific stability (§4.2.2)
// is a separate concern handled in Phase 3.
func (c *Conversation) WaveComplete(w uint64) bool {
	resp := c.srv.Call(waveCompleteRequest{wave: w})
	return resp.(waveCompleteResponse).complete
}

// MessagesInWave returns clones of every envelope in wave w.
// Useful to Order layers iterating wave-at-a-time.
func (c *Conversation) MessagesInWave(w uint64) []*pb.Envelope {
	resp := c.srv.Call(messagesInWaveRequest{wave: w})
	return resp.(messagesInWaveResponse).envelopes
}

// StableMessageIDs returns the IDs of every currently-stable node
// in the local graph. Used by Phase 4's trim watermark protocol.
func (c *Conversation) StableMessageIDs() []*pb.MessageID {
	resp := c.srv.Call(stableMessageIDsRequest{})
	return resp.(stableMessageIDsResponse).ids
}

// FreezeMember marks replica's slot as frozen in the underlying
// Membership view (PLAN §2.10.1). Future messages purportedly
// from replica are rejected; the slot stays in place so existing
// vector clocks remain valid. Used by membership.Manager when a
// VoteOut is decided.
func (c *Conversation) FreezeMember(replica *pb.ReplicaID) error {
	resp := c.srv.Call(freezeMemberRequest{replica: replica})
	return resp.(freezeMemberResponse).err
}

// AddMember appends replica to the underlying Membership at a
// new slot (PLAN §2.10.1 — insertion order, always at the end).
// Returns the new slot index. Used by membership.Manager when a
// VoteIn's MemberAdd commit is processed.
//
// Vectors of in-flight messages from before this Add are
// shorter than the new shape; psync's vector helpers handle
// them via lazy zero-padding at the end.
func (c *Conversation) AddMember(replica *pb.ReplicaID) (int, error) {
	resp := c.srv.Call(addMemberRequest{replica: replica}).(addMemberResponse)
	return resp.slot, resp.err
}

// Close stops the conversation. Idempotent.
func (c *Conversation) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	c.srv.Stop()
	close(c.deliver)
	return nil
}

// ─── handlers (run inside the genserver goroutine) ────────────────

func (s *serverImpl) handleSend(st *state, payload []byte) (*pb.MessageID, error) {
	view := currentView(st.graph, s.membership)
	newVec := Increment(view, s.selfSlot)
	id := &pb.MessageID{
		ConversationId: proto.Clone(s.convID).(*pb.ConversationID),
		Sender:         proto.Clone(s.self).(*pb.ReplicaID),
		VectorClock:    []uint64(newVec),
	}
	env := &pb.Envelope{Id: id, Payload: bytes.Clone(payload)}
	senderSeq := newVec[s.selfSlot]

	if _, err := s.log.Append(context.Background(), env, senderSeq); err != nil {
		return nil, fmt.Errorf("psync: log.Append on Send: %w", err)
	}
	node, missing, err := st.graph.Insert(env)
	if err != nil || missing != nil {
		return nil, fmt.Errorf("psync: graph.Insert on Send: %w (missing=%v)", err, missing)
	}
	s.deliver <- Delivery{Envelope: env, Node: node}

	wireBytes, err := MarshalEnvelope(env)
	if err != nil {
		return nil, err
	}
	// Snapshot the peer list while we still hold the genserver
	// goroutine so the broadcast goroutine sees a consistent
	// membership view at SEND time. The actual network.Send
	// calls happen OFF the genserver — they're independent of
	// the conversation's protected state, so serializing them
	// here would just throttle Submit throughput.
	peers := s.membership.Replicas()
	go s.broadcastEnvelope(peers, wireBytes)
	return id, nil
}

// broadcastEnvelope is the fire-and-forget peer fan-out for a
// Send. Runs OUTSIDE the genserver goroutine (the caller spawns
// it) so concurrent Submits don't serialize behind network I/O.
//
// Each peer Send runs in its own goroutine so a slow peer
// doesn't delay sends to faster ones. Failures are logged and
// dropped; recovery is the peer's job via the lost-message
// protocol (peer detects the gap on its next inbound message
// and requests the missing entry).
//
// Ordering note: with broadcast off the genserver, two Submits
// S1 → S2 may arrive at a peer in either order. psync already
// handles out-of-order arrival via vector-clock gap detection +
// deferral, so this doesn't violate the protocol's guarantees.
func (s *serverImpl) broadcastEnvelope(peers []*pb.ReplicaID, wireBytes []byte) {
	for _, peer := range peers {
		if bytes.Equal(peer.GetValue(), s.self.GetValue()) {
			continue
		}
		go func(p *pb.ReplicaID) {
			if err := s.network.Send(context.Background(), p, wireBytes); err != nil {
				s.logger.Warn("psync: send to peer failed",
					"peer", fmt.Sprintf("%x", p.GetValue()),
					"err", err)
			}
		}(peer)
	}
}

func (s *serverImpl) handleIncoming(st *state, from *pb.ReplicaID, data []byte) {
	got, err := UnmarshalWire(data)
	if err != nil {
		s.logger.Warn("psync: unmarshal wire", "err", err)
		return
	}
	// Fire the liveness callback BEFORE any graph / deferral
	// logic so heartbeats keep peer FDs happy even when the
	// Order wave gate is stalled. Substrates wire this to their
	// failure.Detector.NoteReceived (Phase 10(a)).
	if s.onReceive != nil {
		s.onReceive(from)
	}
	switch {
	case got.Envelope != nil:
		s.handleEnvelope(st, got.Envelope, from)
	case got.LostMessageRequest != nil:
		s.handleLostRequest(st, got.LostMessageRequest, from)
	case got.RestartMessage != nil:
		s.handleRestartMessage(st, got.RestartMessage, from)
	case got.RestartAck != nil:
		s.handleRestartAckIncoming(st, got.RestartAck, from)
	}
}

func (s *serverImpl) handleEnvelope(st *state, env *pb.Envelope, from *pb.ReplicaID) {
	if !proto.Equal(env.GetId().GetConversationId(), s.convID) {
		return
	}
	if env.GetId().GetSender() == nil {
		return
	}
	if s.membership.SlotOf(env.GetId().GetSender()) < 0 {
		s.logger.Debug("psync: envelope from unknown sender", "sender", fmt.Sprintf("%x", env.GetId().GetSender().GetValue()))
		return
	}
	if s.mask.IsMasked(env.GetId().GetSender()) {
		return
	}
	senderSeq, err := s.membership.SenderSeq(env.GetId())
	if err != nil {
		s.logger.Warn("psync: malformed vector_clock", "err", err)
		return
	}
	senderBytes := env.GetId().GetSender().GetValue()
	if st.graph.Has(senderBytes, senderSeq) {
		return
	}
	_, missing, err := st.graph.Insert(env)
	switch {
	case errors.Is(err, ErrAlreadyPresent):
		return
	case errors.Is(err, ErrMissingParents):
		s.deferEnvelope(st, env, missing, from)
		return
	case err != nil:
		s.logger.Warn("psync: graph.Insert error", "err", err)
		return
	}
	if _, err := s.log.Append(context.Background(), env, senderSeq); err != nil {
		s.logger.Error("psync: log.Append failed; the message is in-memory but not durable", "err", err)
	}
	node := st.graph.Lookup(senderBytes, senderSeq)
	s.deliver <- Delivery{Envelope: env, Node: node}
	s.processNewlyAvailable(st, senderBytes, senderSeq)
}

func (s *serverImpl) handleLostRequest(st *state, req *pb.LostMessageRequest, from *pb.ReplicaID) {
	missingSender := req.GetMissingSender().GetValue()
	missingSeq := req.GetMissingSeq()
	entry, err := s.log.LookupBySender(context.Background(), missingSender, missingSeq)
	if err != nil {
		return
	}
	wireBytes, err := MarshalEnvelope(entry.Envelope)
	if err != nil {
		s.logger.Warn("psync: marshal envelope for retransmit", "err", err)
		return
	}
	if err := s.network.Send(context.Background(), from, wireBytes); err != nil {
		s.logger.Warn("psync: retransmit to requester failed", "err", err)
	}
}

func (s *serverImpl) deferEnvelope(st *state, env *pb.Envelope, missing []MissingParent, from *pb.ReplicaID) {
	senderSeq, err := s.membership.SenderSeq(env.GetId())
	if err != nil {
		return
	}
	envKey := makeKey(env.GetId().GetSender().GetValue(), senderSeq)
	if _, already := st.deferred[envKey]; already {
		return
	}
	pe := &pendingEnvelope{
		env:     env,
		from:    proto.Clone(from).(*pb.ReplicaID),
		waiting: make(map[indexKey]struct{}, len(missing)),
	}
	for _, m := range missing {
		pkey := makeKey(m.Sender.GetValue(), m.Seq)
		pe.waiting[pkey] = struct{}{}
		set, ok := st.deferredByNeed[pkey]
		if !ok {
			set = make(map[indexKey]struct{})
			st.deferredByNeed[pkey] = set
		}
		set[envKey] = struct{}{}

		if _, sent := st.outstanding[pkey]; !sent {
			st.outstanding[pkey] = struct{}{}
			s.sendLostMessageRequest(m.Sender, m.Seq, from)
		}
	}
	st.deferred[envKey] = pe
}

func (s *serverImpl) sendLostMessageRequest(missingSender *pb.ReplicaID, missingSeq uint64, askPeer *pb.ReplicaID) {
	wireBytes, err := MarshalLostMessageRequest(missingSender, missingSeq)
	if err != nil {
		s.logger.Warn("psync: marshal LostMessageRequest", "err", err)
		return
	}
	if err := s.network.Send(context.Background(), askPeer, wireBytes); err != nil {
		s.logger.Warn("psync: send LostMessageRequest", "err", err)
	}
}

func (s *serverImpl) processNewlyAvailable(st *state, senderBytes []byte, seq uint64) {
	pkey := makeKey(senderBytes, seq)
	delete(st.outstanding, pkey)
	dependents, ok := st.deferredByNeed[pkey]
	if !ok {
		return
	}
	delete(st.deferredByNeed, pkey)

	keys := make([]indexKey, 0, len(dependents))
	for k := range dependents {
		keys = append(keys, k)
	}
	for _, k := range keys {
		pe, exists := st.deferred[k]
		if !exists {
			continue
		}
		delete(pe.waiting, pkey)
		if len(pe.waiting) == 0 {
			delete(st.deferred, k)
			s.handleEnvelope(st, pe.env, pe.from)
		}
	}
}

// handleReplay iterates the local log and inserts each entry into
// the in-memory graph. Used by Restart() to reconstruct state after
// a process crash.
//
// Entries that are already present (e.g. previous partial replay,
// or the same entry seen via the network meanwhile) are skipped.
// Entries whose causal predecessors are missing — possible if the
// log has been trimmed below their wave — are also skipped; the
// post-replay leaf-set + lost-message exchange recovers them via
// peers per PLAN §1 pruned-region invariant.
//
// When deliverReplayed is true, successfully-inserted envelopes
// are COLLECTED INTO A SLICE for the caller to drain into the
// deliver channel separately (after the genserver returns).
// Pushing to deliver directly here would block the genserver
// if the deliver channel is full — that starves concurrent
// Sends and was the root cause of the Phase 9 soak hang:
// pod restarts → replay → genserver stuck → no heartbeats →
// no wave progress → no Sends → deliver never drains →
// deadlock.
//
// Returns (inserted_count, replayed_envelopes_for_redelivery, err).
func (s *serverImpl) handleReplay(st *state, deliverReplayed bool) (int, []*pb.Envelope, error) {
	ctx := context.Background()
	inserted := 0
	var envelopes []*pb.Envelope
	for entry, err := range s.log.Range(ctx, s.log.FirstOffset(), clog.EndOfLog) {
		if err != nil {
			return inserted, envelopes, err
		}
		env := entry.Envelope
		if env.GetId().GetSender() == nil {
			continue
		}
		// Dedup against existing graph contents.
		senderSeq, err := s.membership.SenderSeq(env.GetId())
		if err != nil {
			s.logger.Warn("psync: replay: malformed log entry", "err", err)
			continue
		}
		if st.graph.Has(env.GetId().GetSender().GetValue(), senderSeq) {
			continue
		}
		_, _, err = st.graph.Insert(env)
		switch {
		case err == nil:
			inserted++
			if deliverReplayed {
				envelopes = append(envelopes, env)
			}
		case errors.Is(err, ErrMissingParents):
			// Pruned predecessor; will be recovered by leaf
			// exchange + lost-message protocol after replay.
			s.logger.Debug("psync: replay: missing parent (will recover via peers)",
				"sender", fmt.Sprintf("%x", env.GetId().GetSender().GetValue()),
				"seq", senderSeq)
		case errors.Is(err, ErrAlreadyPresent):
			// Already-handled.
		default:
			s.logger.Warn("psync: replay: insert error", "err", err)
		}
	}
	return inserted, envelopes, nil
}

// currentView returns the conversation-wide vector clock from this
// replica's perspective: the elementwise max over the latest message
// from each participant.
func currentView(g *Graph, m *Membership) Vector {
	out := make(Vector, m.Len())
	for slot := 0; slot < m.Len(); slot++ {
		r := m.Replica(slot)
		latestSeq := g.LatestSeq(r.GetValue())
		if latestSeq == 0 {
			continue
		}
		n := g.Lookup(r.GetValue(), latestSeq)
		out = Max(out, Vector(n.Envelope.GetId().GetVectorClock()))
	}
	return out
}
