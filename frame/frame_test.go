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

package frame_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/mikehelmick/comlink/frame"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

func r(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

func TestRoundtripApp(t *testing.T) {
	want := []byte("hello app")
	bs, err := frame.MarshalApp(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := frame.Unmarshal(bs)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.App, want) {
		t.Fatalf("App roundtrip = %q, want %q", got.App, want)
	}
	if got.Heartbeat || got.HasMembership() {
		t.Fatalf("non-app variant set: %+v", got)
	}
}

func TestRoundtripHeartbeat(t *testing.T) {
	bs, err := frame.MarshalHeartbeat()
	if err != nil {
		t.Fatal(err)
	}
	got, err := frame.Unmarshal(bs)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Heartbeat {
		t.Fatalf("Heartbeat not set: %+v", got)
	}
	if got.App != nil || got.HasMembership() {
		t.Fatalf("non-heartbeat variant set: %+v", got)
	}
}

func TestRoundtripMembershipEvents(t *testing.T) {
	cases := []struct {
		name    string
		marshal func() ([]byte, error)
		check   func(t *testing.T, got frame.Decoded)
	}{
		{
			"SuspectDown",
			func() ([]byte, error) { return frame.MarshalSuspectDown(r("alice")) },
			func(t *testing.T, got frame.Decoded) {
				if got.SuspectDown == nil {
					t.Fatal("SuspectDown not set")
				}
				if !bytes.Equal(got.SuspectDown.GetSuspect().GetValue(), r("alice").GetValue()) {
					t.Fatalf("SuspectDown.Suspect = %x", got.SuspectDown.GetSuspect().GetValue())
				}
			},
		},
		{
			"VoteOut",
			func() ([]byte, error) { return frame.MarshalVoteOut(r("bob")) },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteOut == nil {
					t.Fatal("VoteOut not set")
				}
				if !bytes.Equal(got.VoteOut.GetTarget().GetValue(), r("bob").GetValue()) {
					t.Fatalf("VoteOut.Target mismatch")
				}
			},
		},
		{
			"VoteOutAck",
			func() ([]byte, error) { return frame.MarshalVoteOutAck(r("bob")) },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteOutAck == nil {
					t.Fatal("VoteOutAck not set")
				}
			},
		},
		{
			"VoteOutNack",
			func() ([]byte, error) { return frame.MarshalVoteOutNack(r("bob")) },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteOutNack == nil {
					t.Fatal("VoteOutNack not set")
				}
			},
		},
		{
			"VoteIn",
			func() ([]byte, error) { return frame.MarshalVoteIn(r("dave"), "127.0.0.1:9000") },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteIn == nil {
					t.Fatal("VoteIn not set")
				}
				if got.VoteIn.GetAddr() != "127.0.0.1:9000" {
					t.Fatalf("VoteIn.Addr = %q", got.VoteIn.GetAddr())
				}
			},
		},
		{
			"VoteInAck",
			func() ([]byte, error) { return frame.MarshalVoteInAck(r("dave")) },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteInAck == nil {
					t.Fatal("VoteInAck not set")
				}
			},
		},
		{
			"VoteInNack",
			func() ([]byte, error) { return frame.MarshalVoteInNack(r("dave")) },
			func(t *testing.T, got frame.Decoded) {
				if got.VoteInNack == nil {
					t.Fatal("VoteInNack not set")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bs, err := tc.marshal()
			if err != nil {
				t.Fatal(err)
			}
			got, err := frame.Unmarshal(bs)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, got)
			if got.App != nil || got.Heartbeat {
				t.Fatalf("non-membership variant set: %+v", got)
			}
		})
	}
}

func TestUnmarshalGarbage(t *testing.T) {
	if _, err := frame.Unmarshal([]byte("not a proto")); err == nil {
		t.Fatal("Unmarshal(garbage) returned no error")
	}
}

func TestUnmarshalEmpty(t *testing.T) {
	bs, err := proto.Marshal(&pb.ConvFrame{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := frame.Unmarshal(bs); !errors.Is(err, frame.ErrEmptyFrame) {
		t.Fatalf("Unmarshal(empty) err = %v, want ErrEmptyFrame", err)
	}
}

// TestEmptyAppPayloadStillDistinguishable: an empty app payload
// should still decode as App, distinct from an unset body.
func TestEmptyAppPayloadStillDistinguishable(t *testing.T) {
	bs, err := frame.MarshalApp(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := frame.Unmarshal(bs)
	if err != nil {
		t.Fatal(err)
	}
	if got.App == nil {
		t.Fatal("App should be non-nil even for empty payload")
	}
	if len(got.App) != 0 {
		t.Fatalf("App length = %d, want 0", len(got.App))
	}
}
