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
	"context"
	"strings"
	"testing"
	"time"

	cgrpc "github.com/mikehelmick/comlink/transport/grpc"
)

// TestClusterIDHandshakeMismatchRejected: two Networks with
// different ClusterIDs cannot exchange Transport/Send. The
// server-side interceptor returns PermissionDenied; that
// surfaces back through Send as a non-nil error containing
// "cluster id mismatch".
func TestClusterIDHandshakeMismatchRejected(t *testing.T) {
	aID, bID := replica("a"), replica("b")

	a, err := cgrpc.Listen(aID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetClusterID(makeClusterID(0x01))
	a.Start()

	b, err := cgrpc.Listen(bID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// b advertises a DIFFERENT cluster ID — so a's interceptor
	// should reject b's calls.
	b.SetClusterID(makeClusterID(0x02))
	b.Start()

	a.AddPeer(bID, b.Addr())
	b.AddPeer(aID, a.Addr())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = b.Send(ctx, aID, []byte("x"))
	if err == nil {
		t.Fatal("Send across cluster boundary: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "cluster id mismatch") {
		t.Fatalf("Send error = %v, want one containing 'cluster id mismatch'", err)
	}
}

// TestClusterIDHandshakeMatchAccepted: a Send works when both
// sides agree on the ClusterID.
func TestClusterIDHandshakeMatchAccepted(t *testing.T) {
	aID, bID := replica("a"), replica("b")
	cid := makeClusterID(0xAA)

	a, err := cgrpc.Listen(aID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetClusterID(cid)
	a.Start()

	b, err := cgrpc.Listen(bID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.SetClusterID(cid)
	b.Start()

	a.AddPeer(bID, b.Addr())
	b.AddPeer(aID, a.Addr())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := a.Send(ctx, bID, []byte("x")); err != nil {
		t.Fatalf("Send with matching ClusterID: %v", err)
	}
	select {
	case <-b.Recv():
	case <-ctx.Done():
		t.Fatal("b did not receive")
	}
}

func makeClusterID(seed byte) []byte {
	out := make([]byte, 16)
	for i := range out {
		out[i] = seed
	}
	return out
}
