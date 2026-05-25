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
	"sync" //nolint:gci

	"github.com/mikehelmick/comlink/clock"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/membership"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport"
	cgrpc "github.com/mikehelmick/comlink/transport/grpc"
)

// Cluster is one node's handle to a comlink cluster (PLAN §5).
// It owns the system conversation (the substrate-internal
// conversation through which cluster admin operations like
// VoteIn/VoteOut flow) plus the shared transport and storage
// that all application Substrates created via NewSubstrate piggy-
// back on.
//
// Cluster is constructed once at process startup. Application
// Substrates are added via Cluster.NewSubstrate (Phase 5(g)).
type Cluster struct {
	cfg ClusterConfig

	clusterID ClusterID
	sysConvID ConversationID
	logger    *slog.Logger
	clk       clock.Clock

	storage stable.Storage
	members *memberStore

	network        transport.Network
	networkOwnedBy bool // true iff Cluster constructed the gRPC
	mux            *transport.Multiplex

	sysConv *psync.Conversation
	sysLog  clog.MessageLog
	sysMgr  *membership.Manager

	// snapshotSources tracks per-conversation snapshot producers
	// (one per Substrate that opts in). When a joiner asks for
	// the snapshot of conv X via StreamSnapshot, the sponsor
	// looks up the source here and streams its output.
	// Phase 10(c)+(d).
	snapshotMu      sync.Mutex
	snapshotSources map[string]*Substrate

	// metadataCh is the public-facing channel for system-conv
	// application messages (Phase 11(a)). Lazy-initialized on
	// first call to MetadataMessages. Closed in
	// adaptMetadataChannel when the system Manager closes.
	metadataCh chan MetadataMessage

	// runCtx is the Cluster's own lifetime context, cancelled by
	// Close. Internal goroutines (psync pumps, membership pump,
	// transport mux) MUST tie themselves to runCtx — not the
	// caller's NewCluster context, which is typically a short-lived
	// bootstrap timeout that fires the moment NewCluster returns.
	runCtx    context.Context
	runCancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error
}

// NewCluster constructs a Cluster from cfg. On first startup with
// Bootstrap.Force=true, this mints a fresh ClusterID and persists
// it. On subsequent startups (or any startup without Force), the
// existing ClusterID is loaded from stable.Storage.
//
// Construction is all-or-nothing: any failure cleans up resources
// already initialized and returns the error.
func NewCluster(ctx context.Context, cfg ClusterConfig) (*Cluster, error) {
	if err := validateClusterConfig(cfg); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.NewSystem()
	}

	// runCtx outlives NewCluster — internal goroutines bind to it.
	// Cancelled by Close, NOT by the caller's bootstrap ctx.
	runCtx, runCancel := context.WithCancel(context.Background())

	storage, err := stable.NewFile(filepath.Join(cfg.DataDir, "cluster_state"))
	if err != nil {
		runCancel()
		return nil, fmt.Errorf("comlink: open stable.Storage: %w", err)
	}
	cleanup := []func(){func() { _ = storage.Close() }, runCancel}
	rollback := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}

	// Build the Network (bound listener, gRPC server constructed,
	// but NOT yet serving — we register the Cluster service and
	// SetClusterID before Start). For sponsor bootstrap we need
	// the actual listen addr early to send in the JoinRequest.
	network, ownedNetwork, err := buildNetwork(cfg)
	if err != nil {
		rollback()
		return nil, err
	}
	if ownedNetwork {
		cleanup = append(cleanup, func() { _ = network.Close() })
	}
	selfAddr := networkAddr(network, cfg)

	memStore, err := loadMemberStore(ctx, storage)
	if err != nil {
		rollback()
		return nil, err
	}

	// Bootstrap discipline (PLAN §5):
	//   - If persisted ClusterID exists: load it, persisted ML is
	//     authoritative.
	//   - If Force: mint or install BootstrapConfig.ClusterID,
	//     seed memStore from cfg.Members.
	//   - If sponsors present (no Force, no persisted): do sponsor
	//     handshake — dial sponsor, call Cluster.Join. Sponsor
	//     VoteIns us and returns (ClusterID, post-admission ML).
	//     Persist both locally.
	//   - Otherwise: ErrBootstrapRequired.
	clusterID, err := loadClusterIDOrJoin(ctx, cfg, storage, memStore, selfAddr, logger)
	if err != nil {
		rollback()
		return nil, err
	}
	sysConvID := SystemConversationID(clusterID)

	// Seed memStore from cfg.Members on first non-joiner startup.
	if memStore.Empty() {
		if len(cfg.Members) == 0 {
			rollback()
			return nil, errors.New("comlink: no persisted ML and no cfg.Members to seed from (joiner mode requires sponsors)")
		}
		seed := make([]*pb.ClusterMember, 0, len(cfg.Members))
		for _, m := range cfg.Members {
			addr := ""
			if m.Equal(cfg.Self) {
				addr = selfAddr
			} else {
				for _, sp := range cfg.Transport.Sponsors {
					if sp.ID.Equal(m) {
						addr = sp.Addr
						break
					}
				}
			}
			seed = append(seed, &pb.ClusterMember{Id: m.toPB(), Addr: addr})
		}
		if err := memStore.SetAll(ctx, seed); err != nil {
			rollback()
			return nil, err
		}
	}

	// Cluster must be in persisted ML.
	persisted := memStore.Members()
	if !containsReplica(persisted, cfg.Self) {
		rollback()
		return nil, fmt.Errorf("comlink: Self %s not in persisted ML (cluster state may be corrupt or this node was voted out)", cfg.Self)
	}

	mux := transport.NewMultiplex(network, 0)
	cleanup = append(cleanup, func() { _ = mux.Close() })

	sysNetwork := mux.ForConversation(sysConvID.toPB())

	sysLogDir := filepath.Join(cfg.DataDir, "conversations", sysConvID.String())
	sysLog, err := clog.OpenFile(sysLogDir, sysConvID.toPB())
	if err != nil {
		rollback()
		return nil, fmt.Errorf("comlink: open system log: %w", err)
	}
	cleanup = append(cleanup, func() { _ = sysLog.Close() })

	// Initial ML for the system conv = persisted membership.
	members := make([]*pb.ReplicaID, len(persisted))
	for i, m := range persisted {
		members[i] = m.GetId()
	}

	sysConv, err := psync.New(runCtx, psync.Config{
		ConversationID:  sysConvID.toPB(),
		Self:            cfg.Self.toPB(),
		Members:         members,
		Network:         sysNetwork,
		Log:             sysLog,
		Storage:         storage,
		Logger:          logger.With("conv", "system"),
		Clock:           clk,
		DeliveryBufSize: 1024,
	})
	if err != nil {
		rollback()
		return nil, fmt.Errorf("comlink: create system Conversation: %w", err)
	}
	cleanup = append(cleanup, func() { _ = sysConv.Close() })

	c := &Cluster{
		cfg:             cfg,
		clusterID:       clusterID,
		sysConvID:       sysConvID,
		logger:          logger,
		clk:             clk,
		storage:         storage,
		members:         memStore,
		network:         network,
		networkOwnedBy:  ownedNetwork,
		mux:             mux,
		sysConv:         sysConv,
		sysLog:          sysLog,
		runCtx:          runCtx,
		runCancel:       runCancel,
		snapshotSources: make(map[string]*Substrate),
	}

	sysMgr, err := membership.New(membership.Config{
		Conversation:       sysConv,
		Self:               cfg.Self.toPB(),
		Members:            members,
		Log:                sysLog,
		Clock:              clk,
		Logger:             logger.With("mgr", "system"),
		InitialGroupSize:   len(members),
		OnMembershipChange: c.onMembershipChange,
	})
	if err != nil {
		rollback()
		return nil, fmt.Errorf("comlink: create system Manager: %w", err)
	}
	cleanup = append(cleanup, func() { _ = sysMgr.Close() })
	c.sysMgr = sysMgr

	// Apply persisted routing into the gRPC network (no-op for
	// the in-memory escape hatch).
	if gn, ok := network.(*cgrpc.Network); ok {
		gn.SetClusterID(clusterID)
		gn.RegisterService(&pb.Cluster_ServiceDesc, &joinService{owner: c})
		for _, m := range persisted {
			if cfg.Self.Equal(ReplicaID(m.GetId().GetValue())) {
				continue
			}
			if m.GetAddr() != "" {
				gn.AddPeer(m.GetId(), m.GetAddr())
			}
		}
		gn.Start()
	}

	c.refreshMembersGauge()

	logger.Info("comlink: cluster started",
		"cluster_id", clusterID.String(),
		"self", cfg.Self.String(),
		"members", len(persisted))
	return c, nil
}

// networkAddr returns the local network address for use in
// sponsor handshakes and persisted self-addr. Preference order:
//
//   1. cfg.Transport.Advertise — explicit override for
//      deployments where the bind address (0.0.0.0:N) isn't a
//      usable peer address. K8s deployments set this to the
//      pod's stable DNS name.
//   2. For the gRPC Network, the actual listener addr (resolved
//      if ":0").
//   3. cfg.Transport.Listen — fallback for non-gRPC networks /
//      tests that bind to a usable address directly.
func networkAddr(n transport.Network, cfg ClusterConfig) string {
	if cfg.Transport.Advertise != "" {
		return cfg.Transport.Advertise
	}
	if gn, ok := n.(*cgrpc.Network); ok {
		return gn.Addr()
	}
	return cfg.Transport.Listen
}

// containsReplica reports whether members contains an entry for r.
func containsReplica(members []*pb.ClusterMember, r ReplicaID) bool {
	for _, m := range members {
		if r.Equal(ReplicaID(m.GetId().GetValue())) {
			return true
		}
	}
	return false
}

// loadClusterIDOrJoin implements the bootstrap discipline.
// Returns the (possibly newly-installed) ClusterID. Side effects:
// on the sponsor-handshake path, persists ClusterID and seeds
// memStore from the sponsor's response.
func loadClusterIDOrJoin(
	ctx context.Context,
	cfg ClusterConfig,
	storage stable.Storage,
	memStore *memberStore,
	selfAddr string,
	logger *slog.Logger,
) (ClusterID, error) {
	var bootstrap BootstrapConfig
	if cfg.Bootstrap != nil {
		bootstrap = *cfg.Bootstrap
	}
	// First try standard load (or Force-mint).
	id, err := loadOrCreateClusterID(ctx, storage, bootstrap)
	if err == nil {
		return id, nil
	}
	// ErrBootstrapRequired is the only error we can recover from
	// via sponsor handshake. Anything else (override conflict,
	// generation failure, etc) propagates.
	if !errors.Is(err, ErrBootstrapRequired) {
		return nil, err
	}
	// No persisted ID, not Force. Try sponsors.
	if len(cfg.Transport.Sponsors) == 0 {
		return nil, err // ErrBootstrapRequired
	}
	logger.Info("comlink: attempting sponsor bootstrap",
		"sponsors", len(cfg.Transport.Sponsors),
		"self_addr", selfAddr)
	var lastErr error
	for _, sp := range cfg.Transport.Sponsors {
		resp, joinErr := dialSponsorJoin(ctx, sp.Addr, cfg.Self, selfAddr)
		if joinErr != nil {
			logger.Warn("comlink: sponsor Join failed",
				"sponsor", sp.Addr, "err", joinErr)
			lastErr = joinErr
			continue
		}
		// Install ClusterID + ML.
		clusterID := ClusterID(resp.GetClusterId().GetValue())
		if err := persistClusterID(ctx, storage, clusterID); err != nil {
			return nil, fmt.Errorf("comlink: persist ClusterID from sponsor: %w", err)
		}
		if err := memStore.SetAll(ctx, resp.GetMembers()); err != nil {
			return nil, fmt.Errorf("comlink: persist members from sponsor: %w", err)
		}
		logger.Info("comlink: sponsor bootstrap succeeded",
			"sponsor", sp.Addr,
			"cluster_id", clusterID.String(),
			"members", len(resp.GetMembers()))
		return clusterID, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("comlink: all sponsors failed: %w", lastErr)
	}
	return nil, err
}

func validateClusterConfig(cfg ClusterConfig) error {
	if len(cfg.Self) != idLen {
		return errors.New("comlink: ClusterConfig.Self is required and must be 16 bytes")
	}
	if cfg.DataDir == "" {
		return errors.New("comlink: ClusterConfig.DataDir is required")
	}
	// Members may be empty for joiner mode (sponsors will supply
	// the ML). If non-empty, Self must be present.
	if len(cfg.Members) > 0 {
		selfFound := false
		for _, m := range cfg.Members {
			if m.Equal(cfg.Self) {
				selfFound = true
				break
			}
		}
		if !selfFound {
			return fmt.Errorf("comlink: ClusterConfig.Self %s not in Members", cfg.Self)
		}
	}
	if cfg.Transport.Network == nil && cfg.Transport.Listen == "" {
		return errors.New("comlink: ClusterConfig.Transport: must provide Network or Listen")
	}
	return nil
}

// buildNetwork returns the transport.Network for the Cluster.
// Returns (network, owned, err) where owned=true means Cluster
// constructed it and is responsible for Close.
func buildNetwork(cfg ClusterConfig) (transport.Network, bool, error) {
	if cfg.Transport.Network != nil {
		return cfg.Transport.Network, false, nil
	}
	peers := make([]cgrpc.Peer, 0, len(cfg.Transport.Sponsors))
	for _, s := range cfg.Transport.Sponsors {
		peers = append(peers, cgrpc.Peer{ID: s.ID.toPB(), Addr: s.Addr})
	}
	gn, err := cgrpc.Listen(cfg.Self.toPB(), cfg.Transport.Listen, peers)
	if err != nil {
		return nil, false, fmt.Errorf("comlink: gRPC Listen: %w", err)
	}
	return gn, true, nil
}

// ClusterID returns this cluster's ClusterID.
func (c *Cluster) ClusterID() ClusterID { return c.clusterID }

// Self returns this replica's ID.
func (c *Cluster) Self() ReplicaID { return c.cfg.Self }

// SystemConversationID returns the well-known ID of the system
// conversation derived from ClusterID.
func (c *Cluster) SystemConversationID() ConversationID { return c.sysConvID }

// ListenAddr returns the local gRPC listener address, useful for
// configuring peers' Sponsors lists in tests / orchestration.
// Returns "" if Cluster was built against a non-gRPC Network (the
// in-memory escape hatch used in tests).
func (c *Cluster) ListenAddr() string {
	if gn, ok := c.network.(*cgrpc.Network); ok {
		return gn.Addr()
	}
	return ""
}

// DataDir returns this Cluster's configured DataDir. Useful for
// restart tests that need to reopen against the same on-disk
// state.
func (c *Cluster) DataDir() string { return c.cfg.DataDir }

// registerSnapshotSource wires a Substrate up as the source of
// snapshots for its ConversationID. Called from NewSubstrate
// when the SM implements Snapshotter — only then can the
// substrate respond to a StreamSnapshot RPC.
//
// Internal API; not user-callable.
func (c *Cluster) registerSnapshotSource(sub *Substrate) {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	c.snapshotSources[string(sub.cfg.ConversationID)] = sub
}

func (c *Cluster) unregisterSnapshotSource(convID ConversationID) {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	delete(c.snapshotSources, string(convID))
}

func (c *Cluster) lookupSnapshotSource(convID ConversationID) *Substrate {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	return c.snapshotSources[string(convID)]
}

// UpdatePeerAddr updates the network address this Cluster uses
// to reach `replica`. Persisted to stable.Storage so the new
// address survives restart, and propagated to the gRPC routing
// table immediately (closing any cached connection to the old
// address). Useful after a peer has restarted on a new port
// (e.g. ":0" assignments in tests).
//
// No-op for non-gRPC transports (the in-memory test escape
// hatch), but persistence still updates.
func (c *Cluster) UpdatePeerAddr(replica ReplicaID, addr string) error {
	if len(replica) != idLen {
		return errors.New("comlink: UpdatePeerAddr: replica must be 16 bytes")
	}
	if err := c.members.Add(context.Background(), replica.toPB(), addr); err != nil {
		return err
	}
	if gn, ok := c.network.(*cgrpc.Network); ok {
		gn.AddPeer(replica.toPB(), addr)
	}
	return nil
}

// Members returns a snapshot of the current cluster ML.
func (c *Cluster) Members() []ReplicaID {
	pbMembers := c.sysMgr.Members()
	out := make([]ReplicaID, len(pbMembers))
	for i, m := range pbMembers {
		out[i] = replicaIDFromPB(m)
	}
	return out
}

// voteOutcomeLabel maps a VoteIn/Out result into a stable
// Prometheus label value. nil → "accepted".
func voteOutcomeLabel(err error) string {
	if err == nil {
		return "accepted"
	}
	if errors.Is(err, membership.ErrVoteInNacked) || errors.Is(err, membership.ErrVoteOutNacked) {
		return "nacked"
	}
	if errors.Is(err, membership.ErrVoteInTimeout) || errors.Is(err, membership.ErrVoteOutTimeout) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "error"
}

// VoteIn proposes adding `target` to the cluster (PLAN §2.13).
// The call blocks until the two-phase VoteIn completes locally
// (quorum gate + MemberAdd commit). On success, the persisted
// membership has been updated.
//
// addr is the network address peers should use to reach
// `target` after it is admitted. It is propagated through
// MemberAdd to every replica so they can persist routing
// information.
func (c *Cluster) VoteIn(ctx context.Context, target ReplicaID, addr string) error {
	if len(target) != idLen {
		return errors.New("comlink: VoteIn target must be 16 bytes")
	}
	err := c.sysMgr.VoteIn(ctx, target.toPB(), addr)
	metricMembershipVoteIn.WithLabelValues(voteOutcomeLabel(err)).Inc()
	return err
}

// VoteOut proposes removing `target` from the cluster
// (PLAN §2.13). Returns when the protocol completes (accepted,
// nacked, or timed out).
func (c *Cluster) VoteOut(ctx context.Context, target ReplicaID) error {
	if len(target) != idLen {
		return errors.New("comlink: VoteOut target must be 16 bytes")
	}
	err := c.sysMgr.VoteOut(ctx, target.toPB())
	metricMembershipVoteOut.WithLabelValues(voteOutcomeLabel(err)).Inc()
	return err
}

// onMembershipChange is the membership.Manager callback.
// Persists the change to stable.Storage so it survives restart,
// and updates gRPC routing (so VoteIn'd replicas become
// reachable via Send and VoteOut'd ones are dropped). Runs on a
// goroutine spawned by the Manager — must not call back into
// c.sysMgr.
func (c *Cluster) onMembershipChange(event membership.MembershipChange) {
	ctx := context.Background()
	var err error
	switch event.Kind {
	case membership.MembershipChangeAdded:
		err = c.members.Add(ctx, event.Replica, event.Addr)
		if gn, ok := c.network.(*cgrpc.Network); ok && event.Addr != "" {
			gn.AddPeer(event.Replica, event.Addr)
		}
		metricMembershipChange.WithLabelValues("added").Inc()
	case membership.MembershipChangeRemoved:
		err = c.members.Remove(ctx, event.Replica)
		if gn, ok := c.network.(*cgrpc.Network); ok {
			gn.RemovePeer(event.Replica)
		}
		metricMembershipChange.WithLabelValues("removed").Inc()
	}
	if err != nil {
		c.logger.Error("comlink: persist membership change",
			"kind", event.Kind, "replica", replicaIDFromPB(event.Replica).String(),
			"err", err)
	}
	// Refresh the cluster-members gauge so dashboards reflect the
	// new size. sysMgr.Members() is the live view.
	c.refreshMembersGauge()
}

// refreshMembersGauge updates the comlink_cluster_members gauge
// to the current ML size. Called on construction and after every
// membership change. Idempotent.
func (c *Cluster) refreshMembersGauge() {
	if c.sysMgr == nil {
		return
	}
	metricClusterMembers.WithLabelValues(c.clusterID.String()).
		Set(float64(len(c.sysMgr.Members())))
}

// Close shuts the Cluster down cleanly. Idempotent.
func (c *Cluster) Close() error {
	c.closeOnce.Do(func() {
		var firstErr error
		record := func(err error) {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		record(c.sysMgr.Close())
		record(c.sysConv.Close())
		record(c.sysLog.Close())
		record(c.mux.Close())
		if c.networkOwnedBy {
			record(c.network.Close())
		}
		record(c.storage.Close())
		if c.runCancel != nil {
			c.runCancel()
		}
		c.closeErr = firstErr
	})
	return c.closeErr
}
