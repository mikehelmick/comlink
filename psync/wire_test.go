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

package psync_test

import (
	"errors"
	"slices"
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
	"google.golang.org/protobuf/proto"
)

func TestRoundtripEnvelope(t *testing.T) {
	env := &pb.Envelope{
		Id: &pb.MessageID{
			Sender:      r("alice"),
			VectorClock: []uint64{1, 0},
		},
		Payload: []byte("hello"),
	}
	bytes, err := psync.MarshalEnvelope(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got.LostMessageRequest != nil || got.RestartMessage != nil || got.RestartAck != nil {
		t.Fatalf("got non-envelope body: %+v", got)
	}
	if !proto.Equal(got.Envelope, env) {
		t.Fatalf("envelope round-trip differs: got %v want %v", got.Envelope, env)
	}
}

func TestRoundtripLostMessageRequest(t *testing.T) {
	bytes, err := psync.MarshalLostMessageRequest(r("alice"), 42)
	if err != nil {
		t.Fatal(err)
	}
	got, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got.Envelope != nil {
		t.Fatalf("got non-nil envelope: %v", got.Envelope)
	}
	if !slices.Equal(got.LostMessageRequest.GetMissingSender().GetValue(), r("alice").GetValue()) {
		t.Fatalf("MissingSender = %x, want alice", got.LostMessageRequest.GetMissingSender().GetValue())
	}
	if got.LostMessageRequest.GetMissingSeq() != 42 {
		t.Fatalf("MissingSeq = %d, want 42", got.LostMessageRequest.GetMissingSeq())
	}
}

func TestRoundtripRestartMessage(t *testing.T) {
	bytes, err := psync.MarshalRestartMessage(r("alice"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got.RestartMessage == nil {
		t.Fatalf("RestartMessage was nil")
	}
	if !slices.Equal(got.RestartMessage.GetRestarter().GetValue(), r("alice").GetValue()) {
		t.Fatalf("Restarter mismatch")
	}
}

func TestRoundtripRestartAck(t *testing.T) {
	leaves := []*pb.MessageID{
		{Sender: r("bob"), VectorClock: []uint64{1, 1}},
		{Sender: r("carol"), VectorClock: []uint64{0, 0, 1}},
	}
	bytes, err := psync.MarshalRestartAck(r("bob"), leaves)
	if err != nil {
		t.Fatal(err)
	}
	got, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if got.RestartAck == nil {
		t.Fatalf("RestartAck was nil")
	}
	if !slices.Equal(got.RestartAck.GetResponder().GetValue(), r("bob").GetValue()) {
		t.Fatalf("Responder mismatch")
	}
	if len(got.RestartAck.GetLeaves()) != 2 {
		t.Fatalf("Leaves count = %d, want 2", len(got.RestartAck.GetLeaves()))
	}
}

func TestUnmarshalGarbage(t *testing.T) {
	if _, err := psync.UnmarshalWire([]byte("not a proto")); err == nil {
		t.Fatal("UnmarshalWire(garbage) returned nil error")
	}
}

func TestUnmarshalEmptyPsyncMessage(t *testing.T) {
	bytes, err := proto.Marshal(&pb.PsyncMessage{}) // body unset
	if err != nil {
		t.Fatal(err)
	}
	if _, err := psync.UnmarshalWire(bytes); !errors.Is(err, psync.ErrEmptyPsyncMessage) {
		t.Fatalf("UnmarshalWire(empty) err = %v, want ErrEmptyPsyncMessage", err)
	}
}
