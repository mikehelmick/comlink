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

// Package grpc provides the production transport.Network backed by
// gRPC over TCP. Each replica runs a gRPC server accepting Frames
// from peers and dials peers as a client when sending.
//
// Phase 0 keeps the wire pattern simple — Send is a unary RPC per
// message. Phase 5 may upgrade to a bidi stream if benchmarks
// warrant. Connections are cached per peer; failed sends do NOT
// retry — Psync's lost-message protocol is the layer that handles
// recovery (PLAN §2.4 footnote).
//
// No TLS in Phase 0; insecure credentials. Production deployments
// would replace credentials.NewTLS with a real config.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Peer is a routing-table entry mapping a ReplicaID to a network
// address.
type Peer struct {
	ID   *pb.ReplicaID
	Addr string
}

// Network is the gRPC implementation of transport.Network.
type Network struct {
	local    *pb.ReplicaID
	server   *grpc.Server
	listener net.Listener
	recv     chan transport.Inbound
	addr     string

	mu     sync.Mutex
	peers  map[string]string           // string(ReplicaID.Value) -> addr
	conns  map[string]*grpc.ClientConn // dialed connections
	closed bool
}

// Listen starts a gRPC server bound to listenAddr (use ":0" for an
// OS-assigned port). The Network returned is ready to Send and Recv;
// peers configures the routing table.
func Listen(local *pb.ReplicaID, listenAddr string, peers []Peer) (*Network, error) {
	if local == nil {
		return nil, errors.New("grpc: nil local replica id")
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("grpc: listen %s: %w", listenAddr, err)
	}
	n := &Network{
		local:    proto.Clone(local).(*pb.ReplicaID),
		listener: lis,
		recv:     make(chan transport.Inbound, 1024),
		addr:     lis.Addr().String(),
		peers:    make(map[string]string),
		conns:    make(map[string]*grpc.ClientConn),
	}
	for _, p := range peers {
		n.peers[string(p.ID.GetValue())] = p.Addr
	}

	srv := grpc.NewServer()
	pb.RegisterTransportServer(srv, &handler{recv: n.recv})
	n.server = srv

	go func() { _ = srv.Serve(lis) }()
	return n, nil
}

// Addr returns the actual listen address (useful with ":0").
func (n *Network) Addr() string { return n.addr }

// AddPeer adds (or updates) a routing-table entry. Safe to call at
// any time, including after Send has already cached a connection;
// re-pointing a peer to a new address invalidates and reopens the
// cached connection on the next Send.
func (n *Network) AddPeer(id *pb.ReplicaID, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	key := string(id.GetValue())
	if existing, ok := n.peers[key]; ok && existing != addr {
		if conn, ok := n.conns[key]; ok {
			_ = conn.Close()
			delete(n.conns, key)
		}
	}
	n.peers[key] = addr
}

// Local returns this network's replica id.
func (n *Network) Local() *pb.ReplicaID {
	return proto.Clone(n.local).(*pb.ReplicaID)
}

// Recv returns the inbound channel.
func (n *Network) Recv() <-chan transport.Inbound {
	return n.recv
}

// Send dials peer (lazily, cached) and invokes Transport/Send.
func (n *Network) Send(ctx context.Context, peer *pb.ReplicaID, payload []byte) error {
	if peer == nil {
		return transport.ErrUnknownPeer
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return transport.ErrClosed
	}
	addr, ok := n.peers[string(peer.GetValue())]
	if !ok {
		n.mu.Unlock()
		return transport.ErrUnknownPeer
	}
	conn := n.conns[string(peer.GetValue())]
	n.mu.Unlock()

	if conn == nil {
		newConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("grpc: dial %s: %w", addr, err)
		}
		n.mu.Lock()
		if existing, ok := n.conns[string(peer.GetValue())]; ok {
			n.mu.Unlock()
			_ = newConn.Close()
			conn = existing
		} else {
			n.conns[string(peer.GetValue())] = newConn
			conn = newConn
			n.mu.Unlock()
		}
	}

	client := pb.NewTransportClient(conn)
	_, err := client.Send(ctx, &pb.Frame{From: n.Local(), Payload: payload})
	return err
}

// Close stops the gRPC server, closes outgoing connections, and
// closes Recv.
func (n *Network) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	conns := n.conns
	n.conns = nil
	n.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
	if n.server != nil {
		n.server.GracefulStop()
	}
	close(n.recv)
	return nil
}

// handler implements pb.TransportServer. It is a separate type from
// Network so that Network.Send (transport.Network method) does not
// collide with the proto-named server handler method.
type handler struct {
	pb.UnimplementedTransportServer
	recv chan<- transport.Inbound
}

func (h *handler) Send(ctx context.Context, frame *pb.Frame) (*pb.SendAck, error) {
	select {
	case h.recv <- transport.Inbound{From: frame.GetFrom(), Payload: frame.GetPayload()}:
		return &pb.SendAck{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
