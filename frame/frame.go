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

// Package frame provides marshal/unmarshal helpers for the
// substrate-level ConvFrame wrapper that goes inside
// psync.Envelope.payload (see proto/comlink/v1/substrate.proto).
//
// Layering: applications using the membership.Manager send and
// receive raw bytes; Manager wraps those into ConvFrame.app
// transparently. FailureDetection emits ConvFrame.heartbeat;
// Membership emits ConvFrame.membership.<event>. All flow through
// the same psync conversation as ordinary Envelope sends.
package frame

import (
	"errors"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// ErrEmptyFrame is returned by Unmarshal when the frame decodes
// successfully but the body oneof is unset.
var ErrEmptyFrame = errors.New("frame: empty ConvFrame")

// MarshalApp wraps app payload bytes in a ConvFrame and returns
// the marshaled bytes.
func MarshalApp(app []byte) ([]byte, error) {
	return proto.Marshal(&pb.ConvFrame{Body: &pb.ConvFrame_App{App: app}})
}

// MarshalHeartbeat returns a marshaled ConvFrame carrying an empty
// heartbeat. Sent by FailureDetection when the conversation has
// been quiet beyond the failure-detection interval.
func MarshalHeartbeat() ([]byte, error) {
	return proto.Marshal(&pb.ConvFrame{Body: &pb.ConvFrame_Heartbeat{Heartbeat: &pb.Heartbeat{}}})
}

// MarshalSuspectDown wraps a SuspectDown event for `suspect`.
func MarshalSuspectDown(suspect *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_SuspectDown{
			SuspectDown: &pb.SuspectDown{Suspect: suspect},
		},
	})
}

// MarshalSuspectAck wraps an Ack-of-suspect event for `suspect`.
func MarshalSuspectAck(suspect *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_SuspectAck{
			SuspectAck: &pb.SuspectAck{Suspect: suspect},
		},
	})
}

// MarshalSuspectNack wraps a Nack-of-suspect event for `suspect`.
func MarshalSuspectNack(suspect *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_SuspectNack{
			SuspectNack: &pb.SuspectNack{Suspect: suspect},
		},
	})
}

// MarshalRecovering wraps a (p is up) event announcing `who` has
// restarted.
func MarshalRecovering(who *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_Recovering{
			Recovering: &pb.Recovering{Who: who},
		},
	})
}

// MarshalRecoveryAck wraps an (Ack, p is up) event acknowledging
// `who`'s incorporation.
func MarshalRecoveryAck(who *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_RecoveryAck{
			RecoveryAck: &pb.RecoveryAck{Who: who},
		},
	})
}

func marshalMembership(ev *pb.MembershipEvent) ([]byte, error) {
	return proto.Marshal(&pb.ConvFrame{
		Body: &pb.ConvFrame_Membership{Membership: ev},
	})
}

// Decoded is the union of all possible decoded ConvFrame bodies.
// Exactly one field group is populated on a successful Unmarshal:
// either App, Heartbeat, or one of the Suspect*/Recover* events.
type Decoded struct {
	App          []byte // populated for ConvFrame.app
	Heartbeat    bool   // true for ConvFrame.heartbeat
	SuspectDown  *pb.SuspectDown
	SuspectAck   *pb.SuspectAck
	SuspectNack  *pb.SuspectNack
	Recovering   *pb.Recovering
	RecoveryAck  *pb.RecoveryAck
}

// IsApp reports whether the decoded frame carries application data.
func (d Decoded) IsApp() bool { return d.App != nil || (!d.Heartbeat && !d.HasMembership()) }

// HasMembership reports whether any membership event variant is set.
func (d Decoded) HasMembership() bool {
	return d.SuspectDown != nil || d.SuspectAck != nil || d.SuspectNack != nil ||
		d.Recovering != nil || d.RecoveryAck != nil
}

// Unmarshal decodes a ConvFrame from data. The returned Decoded
// has exactly one variant populated.
func Unmarshal(data []byte) (Decoded, error) {
	cf := &pb.ConvFrame{}
	if err := proto.Unmarshal(data, cf); err != nil {
		return Decoded{}, err
	}
	switch body := cf.GetBody().(type) {
	case *pb.ConvFrame_App:
		// Distinguish "explicit app body, possibly empty" from
		// "no body set" by accepting any non-nil app slice. An
		// empty app payload becomes a non-nil zero-length slice.
		app := body.App
		if app == nil {
			app = []byte{}
		}
		return Decoded{App: app}, nil
	case *pb.ConvFrame_Heartbeat:
		return Decoded{Heartbeat: true}, nil
	case *pb.ConvFrame_Membership:
		ev := body.Membership
		switch e := ev.GetEvent().(type) {
		case *pb.MembershipEvent_SuspectDown:
			return Decoded{SuspectDown: e.SuspectDown}, nil
		case *pb.MembershipEvent_SuspectAck:
			return Decoded{SuspectAck: e.SuspectAck}, nil
		case *pb.MembershipEvent_SuspectNack:
			return Decoded{SuspectNack: e.SuspectNack}, nil
		case *pb.MembershipEvent_Recovering:
			return Decoded{Recovering: e.Recovering}, nil
		case *pb.MembershipEvent_RecoveryAck:
			return Decoded{RecoveryAck: e.RecoveryAck}, nil
		default:
			return Decoded{}, ErrEmptyFrame
		}
	default:
		return Decoded{}, ErrEmptyFrame
	}
}
