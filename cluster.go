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

	network        transport.Network
	networkOwnedBy bool // true iff Cluster constructed the gRPC
	mux            *transport.Multiplex

	sysConv *psync.Conversation
	sysLog  clog.MessageLog
	sysMgr  *membership.Manager

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

	storage, err := stable.NewFile(filepath.Join(cfg.DataDir, "cluster_state"))
	if err != nil {
		return nil, fmt.Errorf("comlink: open stable.Storage: %w", err)
	}
	cleanup := []func(){func() { _ = storage.Close() }}
	rollback := func() {
		for i := len(cleanup) - 1; i >= 0; i-- {
			cleanup[i]()
		}
	}

	var bootstrap BootstrapConfig
	if cfg.Bootstrap != nil {
		bootstrap = *cfg.Bootstrap
	}
	clusterID, err := loadOrCreateClusterID(ctx, storage, bootstrap)
	if err != nil {
		rollback()
		return nil, err
	}
	sysConvID := SystemConversationID(clusterID)

	network, ownedNetwork, err := buildNetwork(cfg)
	if err != nil {
		rollback()
		return nil, err
	}
	if ownedNetwork {
		cleanup = append(cleanup, func() { _ = network.Close() })
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

	members := make([]*pb.ReplicaID, len(cfg.Members))
	for i, m := range cfg.Members {
		members[i] = m.toPB()
	}

	sysConv, err := psync.New(ctx, psync.Config{
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

	sysMgr, err := membership.New(membership.Config{
		Conversation:     sysConv,
		Self:             cfg.Self.toPB(),
		Members:          members,
		Log:              sysLog,
		Clock:            clk,
		Logger:           logger.With("mgr", "system"),
		InitialGroupSize: len(members),
	})
	if err != nil {
		rollback()
		return nil, fmt.Errorf("comlink: create system Manager: %w", err)
	}
	cleanup = append(cleanup, func() { _ = sysMgr.Close() })

	c := &Cluster{
		cfg:            cfg,
		clusterID:      clusterID,
		sysConvID:      sysConvID,
		logger:         logger,
		clk:            clk,
		storage:        storage,
		network:        network,
		networkOwnedBy: ownedNetwork,
		mux:            mux,
		sysConv:        sysConv,
		sysLog:         sysLog,
		sysMgr:         sysMgr,
	}
	logger.Info("comlink: cluster started",
		"cluster_id", clusterID.String(),
		"self", cfg.Self.String(),
		"members", len(cfg.Members))
	return c, nil
}

func validateClusterConfig(cfg ClusterConfig) error {
	if len(cfg.Self) != idLen {
		return errors.New("comlink: ClusterConfig.Self is required and must be 16 bytes")
	}
	if cfg.DataDir == "" {
		return errors.New("comlink: ClusterConfig.DataDir is required")
	}
	if len(cfg.Members) == 0 {
		return errors.New("comlink: ClusterConfig.Members must be non-empty (initial cluster membership)")
	}
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

// Members returns a snapshot of the current cluster ML.
func (c *Cluster) Members() []ReplicaID {
	pbMembers := c.sysMgr.Members()
	out := make([]ReplicaID, len(pbMembers))
	for i, m := range pbMembers {
		out[i] = replicaIDFromPB(m)
	}
	return out
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
		c.closeErr = firstErr
	})
	return c.closeErr
}
