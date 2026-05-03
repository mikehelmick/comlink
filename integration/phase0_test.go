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

// Package integration contains end-to-end tests that wire together
// multiple comlink packages. These exercise the Phase 0 exit
// criterion from PLAN: a Hello round-trip between two replicas, with
// the message bearing a real MessageID and a ConversationID that's
// been persisted to and reloaded from stable.Storage, then appended
// to the receiver's MessageLog and recovered from disk.
package integration_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport"
	cgrpc "github.com/mikehelmick/comlink/transport/grpc"
	"github.com/mikehelmick/comlink/transport/memory"
	"google.golang.org/protobuf/proto"
)

func id16(tag string) []byte {
	b := make([]byte, 16)
	copy(b, tag)
	return b
}

type transportFactory func(t *testing.T, aID, bID *pb.ReplicaID) (a, b transport.Network, deliver func())

func memoryFactory(t *testing.T, aID, bID *pb.ReplicaID) (transport.Network, transport.Network, func()) {
	t.Helper()
	sched := memory.NewScheduler(1)
	t.Cleanup(func() { _ = sched.Close() })
	a, err := sched.Connect(aID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sched.Connect(bID)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, sched.RunAll
}

func grpcFactory(t *testing.T, aID, bID *pb.ReplicaID) (transport.Network, transport.Network, func()) {
	t.Helper()
	a, err := cgrpc.Listen(aID, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cgrpc.Listen(bID, "127.0.0.1:0", nil)
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	a.AddPeer(bID, b.Addr())
	b.AddPeer(aID, a.Addr())
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	// gRPC delivers asynchronously; nothing for the test to drive.
	return a, b, func() {}
}

// TestPhase0RoundTrip is the Phase 0 exit criterion. Runs against
// both transports.
func TestPhase0RoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		factory transportFactory
	}{
		{"memory", memoryFactory},
		{"grpc", grpcFactory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runRoundTrip(t, tc.factory)
		})
	}
}

func runRoundTrip(t *testing.T, factory transportFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a ConversationID and persist it via stable.Storage.
	convID := &pb.ConversationID{Value: id16("phase0-conv")}
	storageDir := t.TempDir()
	storage, err := stable.NewFile(storageDir)
	if err != nil {
		t.Fatalf("stable.NewFile: %v", err)
	}
	defer storage.Close()
	convBytes, err := proto.Marshal(convID)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.Put(ctx, "conversation_id", convBytes); err != nil {
		t.Fatalf("storage.Put: %v", err)
	}

	// 2. Reload it from stable.Storage to prove durability.
	loadedBytes, err := storage.Get(ctx, "conversation_id")
	if err != nil {
		t.Fatalf("storage.Get: %v", err)
	}
	loadedConvID := &pb.ConversationID{}
	if err := proto.Unmarshal(loadedBytes, loadedConvID); err != nil {
		t.Fatalf("unmarshal conv id: %v", err)
	}
	if !proto.Equal(convID, loadedConvID) {
		t.Fatalf("loaded ConvID = %v, want %v", loadedConvID, convID)
	}

	// 3. Open Alice's and Bob's MessageLogs, both bound to the
	//    reloaded ConversationID.
	aliceID := &pb.ReplicaID{Value: id16("alice")}
	bobID := &pb.ReplicaID{Value: id16("bob")}
	aliceLogDir := t.TempDir()
	bobLogDir := t.TempDir()
	aliceLog, err := clog.OpenFile(aliceLogDir, loadedConvID)
	if err != nil {
		t.Fatalf("alice OpenFile: %v", err)
	}
	defer aliceLog.Close()
	bobLog, err := clog.OpenFile(bobLogDir, loadedConvID)
	if err != nil {
		t.Fatalf("bob OpenFile: %v", err)
	}

	// 4. Build the Hello envelope on Alice's side. Alice's slot in
	//    the 2-replica vector is 0 (sorted by ReplicaID byte order:
	//    "alice" < "bob"), so vector_clock = [1, 0].
	helloBytes, err := proto.Marshal(&pb.Hello{Text: "hello bob, from alice"})
	if err != nil {
		t.Fatal(err)
	}
	envelope := &pb.Envelope{
		Id: &pb.MessageID{
			ConversationId: loadedConvID,
			Sender:         aliceID,
			VectorClock:    []uint64{1, 0},
		},
		Payload: helloBytes,
	}
	const senderSeq = uint64(1)

	// 5. Alice appends to her own log first (per PLAN §2.8: every
	//    accepted message is durably logged before being delivered
	//    upward / sent).
	if _, err := aliceLog.Append(ctx, envelope, senderSeq); err != nil {
		t.Fatalf("alice log Append: %v", err)
	}

	// 6. Wire transports and Send.
	aliceNet, bobNet, deliver := factory(t, aliceID, bobID)
	envBytes, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := aliceNet.Send(ctx, bobID, envBytes); err != nil {
		t.Fatalf("alice Send: %v", err)
	}
	deliver()

	// 7. Bob receives.
	var inbound transport.Inbound
	select {
	case in, ok := <-bobNet.Recv():
		if !ok {
			t.Fatal("bob's Recv was closed")
		}
		inbound = in
	case <-ctx.Done():
		t.Fatalf("bob did not receive: %v", ctx.Err())
	}

	// 8. From-field reflects the sender.
	if string(inbound.From.GetValue()) != string(aliceID.GetValue()) {
		t.Fatalf("inbound.From = %x, want %x", inbound.From.GetValue(), aliceID.GetValue())
	}

	// 9. Unmarshal envelope and verify identity round-tripped intact.
	receivedEnv := &pb.Envelope{}
	if err := proto.Unmarshal(inbound.Payload, receivedEnv); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if !proto.Equal(receivedEnv.GetId().GetConversationId(), convID) {
		t.Fatalf("ConversationID mutated on the wire: %v vs %v",
			receivedEnv.GetId().GetConversationId(), convID)
	}
	if !proto.Equal(receivedEnv.GetId().GetSender(), aliceID) {
		t.Fatalf("Sender mutated on the wire: %v vs %v",
			receivedEnv.GetId().GetSender(), aliceID)
	}
	if got, want := receivedEnv.GetId().GetVectorClock(), []uint64{1, 0}; !equalU64(got, want) {
		t.Fatalf("VectorClock mutated on the wire: %v vs %v", got, want)
	}

	// 10. Verify the Hello payload.
	receivedHello := &pb.Hello{}
	if err := proto.Unmarshal(receivedEnv.GetPayload(), receivedHello); err != nil {
		t.Fatalf("unmarshal Hello: %v", err)
	}
	if got := receivedHello.GetText(); got != "hello bob, from alice" {
		t.Fatalf("Hello.Text = %q, want %q", got, "hello bob, from alice")
	}

	// 11. Bob appends to his MessageLog.
	if _, err := bobLog.Append(ctx, receivedEnv, senderSeq); err != nil {
		t.Fatalf("bob log Append: %v", err)
	}
	if err := bobLog.Close(); err != nil {
		t.Fatal(err)
	}

	// 12. Bob reopens his log and recovers the message — closing the
	//     loop on the persistence chain.
	bobLog2, err := clog.OpenFile(bobLogDir, loadedConvID)
	if err != nil {
		t.Fatalf("bob log reopen: %v", err)
	}
	defer bobLog2.Close()

	entry, err := bobLog2.LookupBySender(ctx, aliceID.GetValue(), senderSeq)
	if err != nil {
		t.Fatalf("bob log Lookup after reopen: %v", err)
	}
	recoveredHello := &pb.Hello{}
	if err := proto.Unmarshal(entry.Envelope.GetPayload(), recoveredHello); err != nil {
		t.Fatalf("unmarshal Hello from log: %v", err)
	}
	if got := recoveredHello.GetText(); got != "hello bob, from alice" {
		t.Fatalf("recovered Hello.Text = %q, want %q", got, "hello bob, from alice")
	}

	// 13. And: opening bob's log with a different ConversationID is
	//     correctly rejected (PLAN §2.10 sanity check).
	otherConv := &pb.ConversationID{Value: id16("phase0-different-conv")}
	if _, err := clog.OpenFile(bobLogDir, otherConv); err == nil {
		t.Fatal("OpenFile with different ConversationID succeeded; want ErrConversationMismatch")
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
