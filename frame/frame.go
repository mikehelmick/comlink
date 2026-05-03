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
//
// Membership-event split (PLAN §2.13): SuspectDown / Recovering
// are informational and have no Ack/Nack. VoteOut / VoteIn are
// the explicit ML-mutation mechanisms and have paired Ack/Nack
// responses.
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

// MarshalSuspectDown wraps a SuspectDown event for `suspect`. This
// is informational — no Ack/Nack response is expected.
func MarshalSuspectDown(suspect *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_SuspectDown{
			SuspectDown: &pb.SuspectDown{Suspect: suspect},
		},
	})
}

// MarshalRecovering wraps a Recovering event announcing `who` has
// restarted.
func MarshalRecovering(who *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_Recovering{
			Recovering: &pb.Recovering{Who: who},
		},
	})
}

// MarshalRecoveryAck wraps a RecoveryAck event acknowledging
// `who`'s incorporation.
func MarshalRecoveryAck(who *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_RecoveryAck{
			RecoveryAck: &pb.RecoveryAck{Who: who},
		},
	})
}

// MarshalVoteOut proposes permanent removal of `target` from ML.
func MarshalVoteOut(target *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteOut{
			VoteOut: &pb.VoteOut{Target: target},
		},
	})
}

// MarshalVoteOutAck votes "Ack" — yes, remove `target`.
func MarshalVoteOutAck(target *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteOutAck{
			VoteOutAck: &pb.VoteOutAck{Target: target},
		},
	})
}

// MarshalVoteOutNack votes "Nack" — no, do not remove `target`.
func MarshalVoteOutNack(target *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteOutNack{
			VoteOutNack: &pb.VoteOutNack{Target: target},
		},
	})
}

// MarshalVoteIn proposes adding `target` (reachable at `addr`) to
// ML.
func MarshalVoteIn(target *pb.ReplicaID, addr string) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteIn{
			VoteIn: &pb.VoteIn{Target: target, Addr: addr},
		},
	})
}

// MarshalVoteInAck votes "Ack" — yes, add `target`.
func MarshalVoteInAck(target *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteInAck{
			VoteInAck: &pb.VoteInAck{Target: target},
		},
	})
}

// MarshalVoteInNack votes "Nack" — no, do not add `target`.
func MarshalVoteInNack(target *pb.ReplicaID) ([]byte, error) {
	return marshalMembership(&pb.MembershipEvent{
		Event: &pb.MembershipEvent_VoteInNack{
			VoteInNack: &pb.VoteInNack{Target: target},
		},
	})
}

func marshalMembership(ev *pb.MembershipEvent) ([]byte, error) {
	return proto.Marshal(&pb.ConvFrame{
		Body: &pb.ConvFrame_Membership{Membership: ev},
	})
}

// Decoded is the union of all possible decoded ConvFrame bodies.
// Exactly one field group is populated on a successful Unmarshal.
type Decoded struct {
	App         []byte // populated for ConvFrame.app
	Heartbeat   bool   // true for ConvFrame.heartbeat
	SuspectDown *pb.SuspectDown
	Recovering  *pb.Recovering
	RecoveryAck *pb.RecoveryAck
	VoteOut     *pb.VoteOut
	VoteOutAck  *pb.VoteOutAck
	VoteOutNack *pb.VoteOutNack
	VoteIn      *pb.VoteIn
	VoteInAck   *pb.VoteInAck
	VoteInNack  *pb.VoteInNack
}

// HasMembership reports whether any membership event variant is set.
func (d Decoded) HasMembership() bool {
	return d.SuspectDown != nil || d.Recovering != nil || d.RecoveryAck != nil ||
		d.VoteOut != nil || d.VoteOutAck != nil || d.VoteOutNack != nil ||
		d.VoteIn != nil || d.VoteInAck != nil || d.VoteInNack != nil
}

// IsApp reports whether the decoded frame carries application data.
func (d Decoded) IsApp() bool { return d.App != nil || (!d.Heartbeat && !d.HasMembership()) }

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
		return decodeMembership(body.Membership), nil
	default:
		return Decoded{}, ErrEmptyFrame
	}
}

func decodeMembership(ev *pb.MembershipEvent) Decoded {
	switch e := ev.GetEvent().(type) {
	case *pb.MembershipEvent_SuspectDown:
		return Decoded{SuspectDown: e.SuspectDown}
	case *pb.MembershipEvent_Recovering:
		return Decoded{Recovering: e.Recovering}
	case *pb.MembershipEvent_RecoveryAck:
		return Decoded{RecoveryAck: e.RecoveryAck}
	case *pb.MembershipEvent_VoteOut:
		return Decoded{VoteOut: e.VoteOut}
	case *pb.MembershipEvent_VoteOutAck:
		return Decoded{VoteOutAck: e.VoteOutAck}
	case *pb.MembershipEvent_VoteOutNack:
		return Decoded{VoteOutNack: e.VoteOutNack}
	case *pb.MembershipEvent_VoteIn:
		return Decoded{VoteIn: e.VoteIn}
	case *pb.MembershipEvent_VoteInAck:
		return Decoded{VoteInAck: e.VoteInAck}
	case *pb.MembershipEvent_VoteInNack:
		return Decoded{VoteInNack: e.VoteInNack}
	default:
		return Decoded{}
	}
}
