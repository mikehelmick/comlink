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

package transport_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	"github.com/mikehelmick/comlink/transport/memory"
)

func convID(tag string) *pb.ConversationID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ConversationID{Value: b}
}

func mxReplica(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// TestMultiplexDispatchesByConvID exercises the core promise:
// two views on the same Multiplex receive only their own conv's
// payloads.
func TestMultiplexDispatchesByConvID(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	aliceUnderlying, _ := sched.Connect(mxReplica("alice"))
	bobUnderlying, _ := sched.Connect(mxReplica("bob"))

	aliceMx := transport.NewMultiplex(aliceUnderlying, 256)
	defer aliceMx.Close()
	bobMx := transport.NewMultiplex(bobUnderlying, 256)
	defer bobMx.Close()

	convA := convID("conv-A")
	convB := convID("conv-B")

	aliceA := aliceMx.ForConversation(convA)
	aliceB := aliceMx.ForConversation(convB)
	bobA := bobMx.ForConversation(convA)
	bobB := bobMx.ForConversation(convB)

	if err := aliceA.Send(ctx, mxReplica("bob"), []byte("for A")); err != nil {
		t.Fatal(err)
	}
	if err := aliceB.Send(ctx, mxReplica("bob"), []byte("for B")); err != nil {
		t.Fatal(err)
	}
	sched.RunAll()

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case in := <-bobA.Recv():
		if !bytes.Equal(in.Payload, []byte("for A")) {
			t.Fatalf("bobA got %q, want %q", in.Payload, "for A")
		}
	case <-timer.C:
		t.Fatal("bobA did not receive")
	}

	timer.Reset(time.Second)
	select {
	case in := <-bobB.Recv():
		if !bytes.Equal(in.Payload, []byte("for B")) {
			t.Fatalf("bobB got %q, want %q", in.Payload, "for B")
		}
	case <-timer.C:
		t.Fatal("bobB did not receive")
	}

	// Cross-channel sanity: bobA should never see B's payload.
	select {
	case in := <-bobA.Recv():
		t.Fatalf("bobA received cross-conv payload: %q", in.Payload)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestMultiplexUnknownConvIDDropsSilently: a payload arriving
// for a convID with no registered view is simply dropped (no
// error, no panic).
func TestMultiplexUnknownConvIDDropsSilently(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	aliceUnd, _ := sched.Connect(mxReplica("alice"))
	bobUnd, _ := sched.Connect(mxReplica("bob"))

	aliceMx := transport.NewMultiplex(aliceUnd, 256)
	defer aliceMx.Close()
	bobMx := transport.NewMultiplex(bobUnd, 256)
	defer bobMx.Close()

	// Alice has a view for convA; bob has views for convA AND convB.
	aliceA := aliceMx.ForConversation(convID("A"))
	bobA := bobMx.ForConversation(convID("A"))
	_ = bobMx.ForConversation(convID("B"))

	// Alice sends a stray frame on convC (which neither side has
	// a view for at alice's end... wait — alice would need to
	// have a convC view to call Send. Re-frame: alice DOES have
	// a convC view but bob does not.
	aliceC := aliceMx.ForConversation(convID("C"))
	_ = aliceA

	if err := aliceC.Send(ctx, mxReplica("bob"), []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	sched.RunAll()

	// Bob's existing views should NOT receive the convC payload.
	select {
	case in := <-bobA.Recv():
		t.Fatalf("bobA received unrelated convC payload: %q", in.Payload)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestMultiplexCloseClosesAllViews: closing the Multiplex closes
// every view's Recv channel.
func TestMultiplexCloseClosesAllViews(t *testing.T) {
	sched := memory.NewScheduler(1)
	defer sched.Close()
	und, _ := sched.Connect(mxReplica("alice"))
	mx := transport.NewMultiplex(und, 256)
	v1 := mx.ForConversation(convID("A"))
	v2 := mx.ForConversation(convID("B"))

	if err := mx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-v1.Recv(); ok {
		t.Fatal("v1.Recv was not closed")
	}
	if _, ok := <-v2.Recv(); ok {
		t.Fatal("v2.Recv was not closed")
	}
}

// TestMultiplexForConversationIdempotent returns the same view
// for the same convID.
func TestMultiplexForConversationIdempotent(t *testing.T) {
	sched := memory.NewScheduler(1)
	defer sched.Close()
	und, _ := sched.Connect(mxReplica("alice"))
	mx := transport.NewMultiplex(und, 256)
	defer mx.Close()
	v1 := mx.ForConversation(convID("A"))
	v2 := mx.ForConversation(convID("A"))
	if v1 != v2 {
		t.Fatal("ForConversation returned different views for same convID")
	}
}

// TestMultiplexViewSendAfterCloseFails
func TestMultiplexViewSendAfterCloseFails(t *testing.T) {
	sched := memory.NewScheduler(1)
	defer sched.Close()
	und, _ := sched.Connect(mxReplica("alice"))
	_, _ = sched.Connect(mxReplica("bob"))
	mx := transport.NewMultiplex(und, 256)
	defer mx.Close()
	v := mx.ForConversation(convID("A"))
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Send(context.Background(), mxReplica("bob"), []byte("x")); err == nil {
		t.Fatal("Send on closed view returned no error")
	}
}
