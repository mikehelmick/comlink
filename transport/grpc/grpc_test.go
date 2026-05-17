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

package grpc_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	cgrpc "github.com/mikehelmick/comlink/transport/grpc"
)

func replica(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// twoNode boots two grpc Networks bound to OS-assigned ports and
// wires them together via AddPeer. Returned in (a, b) order.
func twoNode(t *testing.T) (*cgrpc.Network, *cgrpc.Network) {
	t.Helper()
	aID, bID := replica("a"), replica("b")

	a, err := cgrpc.Listen(aID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	a.Start()
	b, err := cgrpc.Listen(bID, "127.0.0.1:0", nil)
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	b.Start()
	a.AddPeer(bID, b.Addr())
	b.AddPeer(aID, a.Addr())
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return a, b
}

// TestRoundTrip is the Phase 0 exit-criterion smoke test on the
// real gRPC transport — two replicas exchange a payload.
func TestRoundTrip(t *testing.T) {
	a, b := twoNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.Send(ctx, b.Local(), []byte("hello b")); err != nil {
		t.Fatalf("a -> b Send: %v", err)
	}
	select {
	case in := <-b.Recv():
		if !bytes.Equal(in.Payload, []byte("hello b")) {
			t.Fatalf("b received %q, want %q", in.Payload, "hello b")
		}
		if !bytes.Equal(in.From.GetValue(), a.Local().GetValue()) {
			t.Fatalf("b received From = %x, want a's id %x", in.From.GetValue(), a.Local().GetValue())
		}
	case <-ctx.Done():
		t.Fatal("b did not receive within timeout")
	}

	if err := b.Send(ctx, a.Local(), []byte("reply")); err != nil {
		t.Fatalf("b -> a Send: %v", err)
	}
	select {
	case in := <-a.Recv():
		if !bytes.Equal(in.Payload, []byte("reply")) {
			t.Fatalf("a received %q, want %q", in.Payload, "reply")
		}
	case <-ctx.Done():
		t.Fatal("a did not receive within timeout")
	}
}

func TestSendToUnknownPeerErrors(t *testing.T) {
	a, _ := twoNode(t)
	ctx := context.Background()
	if err := a.Send(ctx, replica("ghost"), []byte("x")); !errors.Is(err, transport.ErrUnknownPeer) {
		t.Fatalf("Send to unknown peer: err = %v, want ErrUnknownPeer", err)
	}
}

func TestSendOnClosedNetworkErrors(t *testing.T) {
	a, b := twoNode(t)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(context.Background(), b.Local(), []byte("x")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Send on closed: err = %v, want ErrClosed", err)
	}
}
