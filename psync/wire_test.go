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
	gotEnv, gotReq, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if gotReq != nil {
		t.Fatalf("got non-nil request: %v", gotReq)
	}
	if !proto.Equal(gotEnv, env) {
		t.Fatalf("envelope round-trip differs: got %v want %v", gotEnv, env)
	}
}

func TestRoundtripLostMessageRequest(t *testing.T) {
	bytes, err := psync.MarshalLostMessageRequest(r("alice"), 42)
	if err != nil {
		t.Fatal(err)
	}
	gotEnv, gotReq, err := psync.UnmarshalWire(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if gotEnv != nil {
		t.Fatalf("got non-nil envelope: %v", gotEnv)
	}
	if !slices.Equal(gotReq.GetMissingSender().GetValue(), r("alice").GetValue()) {
		t.Fatalf("MissingSender = %x, want alice", gotReq.GetMissingSender().GetValue())
	}
	if gotReq.GetMissingSeq() != 42 {
		t.Fatalf("MissingSeq = %d, want 42", gotReq.GetMissingSeq())
	}
}

func TestUnmarshalGarbage(t *testing.T) {
	if _, _, err := psync.UnmarshalWire([]byte("not a proto")); err == nil {
		t.Fatal("UnmarshalWire(garbage) returned nil error")
	}
}

func TestUnmarshalEmptyPsyncMessage(t *testing.T) {
	bytes, err := proto.Marshal(&pb.PsyncMessage{}) // body unset
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := psync.UnmarshalWire(bytes); !errors.Is(err, psync.ErrEmptyPsyncMessage) {
		t.Fatalf("UnmarshalWire(empty) err = %v, want ErrEmptyPsyncMessage", err)
	}
}
