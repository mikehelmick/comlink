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
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ClusterIDMetadataKey is the gRPC metadata key carrying the
// sender's ClusterID on every outbound call. The server-side
// interceptor rejects calls whose metadata ClusterID doesn't
// match the server's own (preventing two distinct clusters with
// overlapping ConversationIDs from accidentally merging).
//
// Hex-encoded so the value is printable in logs / metadata
// inspection tools.
const ClusterIDMetadataKey = "x-comlink-cluster-id"

// ExemptHandshakeMethods are full gRPC method names that bypass
// the ClusterID handshake check. The sponsor Join RPC must
// bypass — a joiner uses Join to LEARN the ClusterID and can't
// possibly send it on the way in.
var ExemptHandshakeMethods = map[string]bool{
	"/comlink.v1.Cluster/Join": true,
}

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

	// clusterIDHex is the hex-encoded ClusterID stamped into
	// outgoing metadata and validated on incoming calls.
	// Empty before SetClusterID is called — in that state the
	// server is permissive (used during the brief window
	// between Listen and SetClusterID at startup).
	clusterIDHex string

	mu      sync.Mutex
	peers   map[string]string           // string(ReplicaID.Value) -> addr
	conns   map[string]*grpc.ClientConn // dialed connections
	started bool
	closed  bool
}

// Listen builds the gRPC Network and binds the listener, but does
// NOT start accepting connections. The caller can register
// additional services via RegisterService and then call Start to
// begin Serve. Splitting Listen and Start lets the Cluster
// register the Cluster/Join handler before traffic flows.
//
// listenAddr accepts ":0" for an OS-assigned port — read it back
// via Addr() after Listen returns.
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

	srv := grpc.NewServer(grpc.UnaryInterceptor(n.serverInterceptor))
	pb.RegisterTransportServer(srv, &handler{recv: n.recv})
	n.server = srv

	return n, nil
}

// Start begins accepting connections on the bound listener. Must
// be called exactly once after Listen (and any RegisterService
// calls). Safe to call from any goroutine.
func (n *Network) Start() {
	n.mu.Lock()
	if n.started || n.closed {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.mu.Unlock()
	go func() { _ = n.server.Serve(n.listener) }()
}

// RegisterService registers an extra gRPC service on the shared
// server. Must be called BEFORE Start (gRPC panics if a service
// is registered after Serve begins).
func (n *Network) RegisterService(desc *grpc.ServiceDesc, impl any) {
	n.server.RegisterService(desc, impl)
}

// SetClusterID stamps this Network with the local ClusterID so
// it can both attach it to outgoing calls and validate it on
// incoming calls. Safe to call once at Cluster construction
// time. Subsequent calls overwrite (used at sponsor-handshake
// completion when the joiner learns the ID).
func (n *Network) SetClusterID(id []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clusterIDHex = hex.EncodeToString(id)
}

// serverInterceptor enforces the ClusterID handshake on every
// inbound unary RPC except those in ExemptHandshakeMethods.
func (n *Network) serverInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if ExemptHandshakeMethods[info.FullMethod] {
		return handler(ctx, req)
	}
	n.mu.Lock()
	expected := n.clusterIDHex
	n.mu.Unlock()
	if expected == "" {
		// Cluster hasn't installed its ID yet — permissive (this
		// window is narrow; closes when Cluster calls
		// SetClusterID before Start).
		return handler(ctx, req)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get(ClusterIDMetadataKey)
	if len(vals) == 0 {
		return nil, status.Errorf(codes.PermissionDenied,
			"grpc: peer did not send %s metadata", ClusterIDMetadataKey)
	}
	if vals[0] != expected {
		return nil, status.Errorf(codes.PermissionDenied,
			"grpc: cluster id mismatch (peer=%s, server=%s)", vals[0], expected)
	}
	return handler(ctx, req)
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

// RemovePeer drops a routing-table entry and tears down any
// cached connection. Safe to call concurrently with Send (Send
// will see ErrUnknownPeer if RemovePeer wins the race).
func (n *Network) RemovePeer(id *pb.ReplicaID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	key := string(id.GetValue())
	delete(n.peers, key)
	if conn, ok := n.conns[key]; ok {
		_ = conn.Close()
		delete(n.conns, key)
	}
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
		newConn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(n.clientInterceptor),
		)
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

// clientInterceptor attaches the local ClusterID to every
// outbound unary call (skipping exempt methods, e.g. Join).
func (n *Network) clientInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if !ExemptHandshakeMethods[method] {
		n.mu.Lock()
		id := n.clusterIDHex
		n.mu.Unlock()
		if id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ClusterIDMetadataKey, id)
		}
	}
	return invoker(ctx, method, req, reply, cc, opts...)
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
