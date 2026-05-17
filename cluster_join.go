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

package comlink

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// joinService is the gRPC server impl for the Cluster.Join RPC
// (PLAN §5(h)). Registered against the local gRPC server in
// NewCluster. Holds a back-pointer to its owning Cluster so it
// can perform VoteIn and inspect current state.
type joinService struct {
	pb.UnimplementedClusterServer
	owner *Cluster
}

// Join is the sponsor handshake handler. It runs VoteIn for the
// joiner; on success it returns the cluster's ClusterID and the
// post-admission ML so the joiner can install both locally.
func (s *joinService) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	joiner := req.GetJoiner()
	if joiner == nil || len(joiner.GetValue()) != idLen {
		return nil, status.Errorf(codes.InvalidArgument,
			"comlink: Join request missing or invalid joiner id")
	}
	addr := req.GetJoinerAddr()
	// Run VoteIn synchronously. Idempotent at the protocol level:
	// if the joiner is already a member, VoteIn returns
	// ErrVoteInTargetAlreadyMember — we treat that as success
	// (subsequent joiner re-bootstrap is legal).
	if err := s.owner.VoteIn(ctx, ReplicaID(joiner.GetValue()), addr); err != nil {
		if errors.Is(err, membership.ErrVoteInTargetAlreadyMember) {
			// Fall through — return current state.
		} else {
			return nil, status.Errorf(codes.FailedPrecondition,
				"comlink: VoteIn(%x) failed: %v", joiner.GetValue(), err)
		}
	}
	// Build the response from Manager.Members (synchronously
	// up-to-date) joined with the addr map from memStore. memStore
	// is updated via OnMembershipChange on a goroutine and may not
	// yet reflect the joiner — we explicitly inject the joiner's
	// addr from the request so the response is always complete.
	sysMembers := s.owner.sysMgr.Members()
	persisted := s.owner.members.Members()
	addrByID := make(map[string]string, len(persisted)+1)
	for _, m := range persisted {
		addrByID[string(m.GetId().GetValue())] = m.GetAddr()
	}
	addrByID[string(joiner.GetValue())] = addr

	members := make([]*pb.ClusterMember, 0, len(sysMembers))
	for _, id := range sysMembers {
		members = append(members, &pb.ClusterMember{
			Id:   id,
			Addr: addrByID[string(id.GetValue())],
		})
	}
	return &pb.JoinResponse{
		ClusterId: &pb.ClusterID{Value: s.owner.clusterID},
		Members:   members,
	}, nil
}

// dialSponsorJoin opens a one-shot gRPC client to sponsorAddr and
// calls Cluster.Join with (self, selfAddr). Used at joiner
// bootstrap when no ClusterID is persisted locally. The dial
// does NOT go through the long-lived Network — there's no
// Network yet and the Join RPC is intentionally exempt from
// the ClusterID handshake.
func dialSponsorJoin(ctx context.Context, sponsorAddr string, self ReplicaID, selfAddr string) (*pb.JoinResponse, error) {
	conn, err := grpc.NewClient(sponsorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("comlink: dial sponsor %s: %w", sponsorAddr, err)
	}
	defer conn.Close()
	client := pb.NewClusterClient(conn)
	resp, err := client.Join(ctx, &pb.JoinRequest{
		Joiner:     self.toPB(),
		JoinerAddr: selfAddr,
	})
	if err != nil {
		return nil, fmt.Errorf("comlink: Join(%s) failed: %w", sponsorAddr, err)
	}
	if len(resp.GetClusterId().GetValue()) != idLen {
		return nil, fmt.Errorf("comlink: sponsor returned invalid ClusterID")
	}
	return resp, nil
}
