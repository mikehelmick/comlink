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

package transport

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Multiplex hosts multiple conversations on a single underlying
// Network. Phase 5(b): a Cluster owns one transport endpoint per
// node and uses Multiplex to give each conversation (the system
// conv plus N application convs) its own logical Network view.
//
// Wire format: Send wraps the caller's payload in a MultiplexFrame
// carrying the conversation_id. The receiving Multiplex decodes
// the frame and dispatches the inner payload to the matching
// per-conversation Recv channel.
//
// Multiplex itself implements no Send/Recv — call ForConversation
// to obtain a per-conversation Network view.
type Multiplex struct {
	underlying Network
	bufSize    int

	mu       sync.Mutex
	views    map[string]*multiplexView // string(convID.value) -> view
	pumpDone chan struct{}
	stopped  chan struct{}
	closed   bool
}

// NewMultiplex wraps an underlying Network (typically a memory
// or gRPC Network) and starts a pump goroutine that decodes
// MultiplexFrames and dispatches the inner payloads to the
// matching per-conversation view.
//
// bufSize is the per-view receive buffer; defaults to 256 if 0.
func NewMultiplex(underlying Network, bufSize int) *Multiplex {
	if bufSize <= 0 {
		bufSize = 256
	}
	m := &Multiplex{
		underlying: underlying,
		bufSize:    bufSize,
		views:      make(map[string]*multiplexView),
		pumpDone:   make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	go m.pump()
	return m
}

// ForConversation returns a Network view bound to convID. Sends
// through this view automatically wrap the payload with the
// conversation_id; received frames whose conversation_id matches
// arrive on its Recv channel.
//
// If a view for convID already exists, the existing view is
// returned (idempotent — safe to call multiple times).
func (m *Multiplex) ForConversation(convID *pb.ConversationID) Network {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := string(convID.GetValue())
	if v, ok := m.views[key]; ok {
		return v
	}
	v := &multiplexView{
		mp:     m,
		convID: proto.Clone(convID).(*pb.ConversationID),
		recv:   make(chan Inbound, m.bufSize),
	}
	m.views[key] = v
	return v
}

// Local returns the underlying Network's Local replica id.
func (m *Multiplex) Local() *pb.ReplicaID { return m.underlying.Local() }

// Close stops the pump and closes every view's Recv channel.
// The underlying Network is NOT closed; the caller owns it.
func (m *Multiplex) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	views := m.views
	m.views = nil
	close(m.stopped)
	m.mu.Unlock()
	<-m.pumpDone
	for _, v := range views {
		v.markClosed()
	}
	return nil
}

// pump reads from underlying.Recv, decodes MultiplexFrames, and
// dispatches inner payloads to the matching view.
func (m *Multiplex) pump() {
	defer close(m.pumpDone)
	for {
		select {
		case in, ok := <-m.underlying.Recv():
			if !ok {
				return
			}
			mf := &pb.MultiplexFrame{}
			if err := proto.Unmarshal(in.Payload, mf); err != nil {
				// Bad frame; drop silently. Real systems would log here.
				continue
			}
			m.mu.Lock()
			view := m.views[string(mf.GetConversationId().GetValue())]
			m.mu.Unlock()
			if view == nil {
				// No conversation registered for this convID; drop.
				continue
			}
			select {
			case view.recv <- Inbound{From: in.From, Payload: mf.GetPayload()}:
			default:
				// Per-view buffer full; drop. Same backpressure
				// approach as the in-memory transport.
			}
		case <-m.stopped:
			return
		}
	}
}

// multiplexView is a Network bound to one ConversationID.
type multiplexView struct {
	mp       *Multiplex
	convID   *pb.ConversationID
	recv     chan Inbound
	closeMu  sync.Mutex
	isClosed bool
}

// Local returns the underlying Network's Local replica id.
func (v *multiplexView) Local() *pb.ReplicaID { return v.mp.Local() }

// Send wraps payload in a MultiplexFrame and forwards to the
// underlying Network.
func (v *multiplexView) Send(ctx context.Context, peer *pb.ReplicaID, payload []byte) error {
	v.closeMu.Lock()
	if v.isClosed {
		v.closeMu.Unlock()
		return ErrClosed
	}
	v.closeMu.Unlock()
	wrapped, err := proto.Marshal(&pb.MultiplexFrame{
		ConversationId: v.convID,
		Payload:        payload,
	})
	if err != nil {
		return fmt.Errorf("multiplex: marshal frame: %w", err)
	}
	return v.mp.underlying.Send(ctx, peer, wrapped)
}

// Recv returns the per-conversation receive channel.
func (v *multiplexView) Recv() <-chan Inbound { return v.recv }

// Close detaches this view from the multiplex and closes Recv.
// Subsequent Sends return ErrClosed.
func (v *multiplexView) Close() error {
	v.mp.mu.Lock()
	delete(v.mp.views, string(v.convID.GetValue()))
	v.mp.mu.Unlock()
	v.markClosed()
	return nil
}

func (v *multiplexView) markClosed() {
	v.closeMu.Lock()
	defer v.closeMu.Unlock()
	if v.isClosed {
		return
	}
	v.isClosed = true
	close(v.recv)
}

// Compile-time guard: multiplexView implements Network.
var _ Network = (*multiplexView)(nil)
