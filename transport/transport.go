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

// Package transport defines the abstract Network surface that comlink
// protocols (Psync and above) talk to, plus shared error types.
//
// The transport carries opaque byte payloads addressed by ReplicaID;
// it does not interpret message structure (Psync marshals Envelopes
// into bytes before Send, and unmarshals on Recv). Implementations
// live in subpackages: transport/memory for the deterministic
// in-process transport tests use, and transport/grpc for real
// network operation.
package transport

import (
	"context"
	"errors"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
)

// ErrUnknownPeer is returned by Send when peer is not reachable from
// this Network (not in the routing table, no transport handle, etc.).
var ErrUnknownPeer = errors.New("transport: unknown peer")

// ErrClosed is returned by Send when the Network has been closed.
var ErrClosed = errors.New("transport: closed")

// Inbound is a message received from a peer.
//
// Both fields share storage with the transport's internal buffer when
// allowed by the implementation; treat them as read-only after the
// channel send.
type Inbound struct {
	From    *pb.ReplicaID
	Payload []byte
}

// Network is the abstract per-replica view of the network.
//
// Implementations must be safe for concurrent use. Send returns when
// the payload has been handed off to the underlying transport — NOT
// when the peer has applied it. PLAN §2.4 footnote: callers must
// never assume "Send returned, therefore peer has it" for correctness;
// Psync's lost-message protocol provides the actual delivery
// guarantee.
type Network interface {
	// Local returns the ReplicaID this Network instance represents.
	Local() *pb.ReplicaID
	// Send delivers payload to peer.
	Send(ctx context.Context, peer *pb.ReplicaID, payload []byte) error
	// Recv returns a channel of incoming messages. The channel is
	// closed when the Network is Closed.
	Recv() <-chan Inbound
	// Close stops the Network and closes Recv.
	Close() error
}
