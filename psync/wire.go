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

// MarshalRestartMessage wraps a RestartMessage in a PsyncMessage.
func MarshalRestartMessage(restarter *pb.ReplicaID) ([]byte, error) {
	return proto.Marshal(&pb.PsyncMessage{
		Body: &pb.PsyncMessage_RestartMessage{
			RestartMessage: &pb.RestartMessage{Restarter: restarter},
		},
	})
}

// MarshalRestartAck wraps a RestartAck in a PsyncMessage.
func MarshalRestartAck(responder *pb.ReplicaID, leaves []*pb.MessageID) ([]byte, error) {
	return proto.Marshal(&pb.PsyncMessage{
		Body: &pb.PsyncMessage_RestartAck{
			RestartAck: &pb.RestartAck{
				Responder: responder,
				Leaves:    leaves,
			},
		},
	})
}

// Decoded is the union of all possible decoded PsyncMessage bodies.
// Exactly one field is non-nil on a successful UnmarshalWire.
type Decoded struct {
	Envelope           *pb.Envelope
	LostMessageRequest *pb.LostMessageRequest
	RestartMessage     *pb.RestartMessage
	RestartAck         *pb.RestartAck
}

// UnmarshalWire decodes a transport payload into a PsyncMessage and
// returns the inner body via the Decoded union. Exactly one field
// of the returned Decoded is non-nil on success.
func UnmarshalWire(data []byte) (Decoded, error) {
	pm := &pb.PsyncMessage{}
	if err := proto.Unmarshal(data, pm); err != nil {
		return Decoded{}, err
	}
	switch body := pm.GetBody().(type) {
	case *pb.PsyncMessage_Envelope:
		return Decoded{Envelope: body.Envelope}, nil
	case *pb.PsyncMessage_LostMessageRequest:
		return Decoded{LostMessageRequest: body.LostMessageRequest}, nil
	case *pb.PsyncMessage_RestartMessage:
		return Decoded{RestartMessage: body.RestartMessage}, nil
	case *pb.PsyncMessage_RestartAck:
		return Decoded{RestartAck: body.RestartAck}, nil
	default:
		return Decoded{}, ErrEmptyPsyncMessage
	}
}
