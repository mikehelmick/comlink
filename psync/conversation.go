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
type replayLogRequest struct{}
type waveCompleteRequest struct{ wave uint64 }
type messagesInWaveRequest struct{ wave uint64 }
type stableMessageIDsRequest struct{}

func (sendRequest) isRequest()             {}
func (incomingRequest) isRequest()         {}
func (maskRequest) isRequest()             {}
func (replayLogRequest) isRequest()        {}
func (waveCompleteRequest) isRequest()     {}
func (messagesInWaveRequest) isRequest()   {}
func (stableMessageIDsRequest) isRequest() {}

type response interface{ isResponse() }

type sendResponse struct {
	id  *pb.MessageID
	err error
}
type maskResponse struct{ err error }
type replayLogResponse struct {
	inserted int
	err      error
}
type waveCompleteResponse struct{ complete bool }
type messagesInWaveResponse struct {
	envelopes []*pb.Envelope
}
type stableMessageIDsResponse struct {
	ids []*pb.MessageID
}
type emptyResponse struct{}

func (sendResponse) isResponse()             {}
func (maskResponse) isResponse()             {}
func (replayLogResponse) isResponse()        {}
func (waveCompleteResponse) isResponse()     {}
func (messagesInWaveResponse) isResponse()   {}
func (stableMessageIDsResponse) isResponse() {}
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
		n, err := s.handleReplay(st)
		return replayLogResponse{inserted: n, err: err}, st
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

// Membership returns this conversation's sorted membership view.
// Phase 1 assumes static membership; the returned object reflects
// the (immutable) Members from Config.
func (c *Conversation) Membership() *Membership {
	return NewMembership(c.cfg.Members)
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
	for _, peer := range s.membership.Replicas() {
		if bytes.Equal(peer.GetValue(), s.self.GetValue()) {
			continue
		}
		if err := s.network.Send(context.Background(), peer, wireBytes); err != nil {
			s.logger.Warn("psync: send to peer failed",
				"peer", fmt.Sprintf("%x", peer.GetValue()),
				"err", err)
		}
	}
	return id, nil
}

func (s *serverImpl) handleIncoming(st *state, from *pb.ReplicaID, data []byte) {
	got, err := UnmarshalWire(data)
	if err != nil {
		s.logger.Warn("psync: unmarshal wire", "err", err)
		return
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
// Replayed entries are NOT pushed to the application's Recv
// channel: re-delivery is the application's concern (Phase 4 will
// add a checkpoint mechanism so the application can know what was
// already applied pre-crash).
//
// Returns the number of entries successfully inserted.
func (s *serverImpl) handleReplay(st *state) (int, error) {
	ctx := context.Background()
	inserted := 0
	for entry, err := range s.log.Range(ctx, s.log.FirstOffset(), clog.EndOfLog) {
		if err != nil {
			return inserted, err
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
	return inserted, nil
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
