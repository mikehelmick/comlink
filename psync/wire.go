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

package psync

import (
	"errors"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// ErrEmptyPsyncMessage is returned by UnmarshalWire when the wire
// payload decodes successfully but the body oneof is unset.
var ErrEmptyPsyncMessage = errors.New("psync: empty PsyncMessage on wire")

// MarshalEnvelope wraps env in a PsyncMessage and returns the
// marshaled bytes ready for transport.Send.
func MarshalEnvelope(env *pb.Envelope) ([]byte, error) {
	return proto.Marshal(&pb.PsyncMessage{
		Body: &pb.PsyncMessage_Envelope{Envelope: env},
	})
}

// MarshalLostMessageRequest wraps a LostMessageRequest in a
// PsyncMessage and returns the marshaled bytes.
func MarshalLostMessageRequest(missingSender *pb.ReplicaID, missingSeq uint64) ([]byte, error) {
	return proto.Marshal(&pb.PsyncMessage{
		Body: &pb.PsyncMessage_LostMessageRequest{
			LostMessageRequest: &pb.LostMessageRequest{
				MissingSender: missingSender,
				MissingSeq:    missingSeq,
			},
		},
	})
}

// UnmarshalWire decodes a transport payload into a PsyncMessage and
// returns the inner body via type-asserting accessors. Exactly one
// of the returned envelope / lostReq is non-nil on success.
func UnmarshalWire(data []byte) (env *pb.Envelope, lostReq *pb.LostMessageRequest, err error) {
	pm := &pb.PsyncMessage{}
	if err := proto.Unmarshal(data, pm); err != nil {
		return nil, nil, err
	}
	switch body := pm.GetBody().(type) {
	case *pb.PsyncMessage_Envelope:
		return body.Envelope, nil, nil
	case *pb.PsyncMessage_LostMessageRequest:
		return nil, body.LostMessageRequest, nil
	default:
		return nil, nil, ErrEmptyPsyncMessage
	}
}
