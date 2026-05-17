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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/stable"
	"google.golang.org/protobuf/proto"
)

// stableKeyMembers is the stable.Storage key under which the
// Cluster persists its current PersistedMembership (PLAN §5).
const stableKeyMembers = "comlink.members"

// memberStore wraps stable.Storage with a small in-memory cache
// of the current cluster membership. All mutations serialize
// through the mutex so the on-disk format never sees a torn write
// and concurrent VoteIn/VoteOut callbacks are linearized.
type memberStore struct {
	storage stable.Storage

	mu      sync.Mutex
	members []*pb.ClusterMember
}

// loadMemberStore reads the persisted membership from storage.
// Returns an empty store (not an error) if no record exists yet —
// callers distinguish "fresh data dir" from "loaded zero members"
// via the len of the returned slice from Members().
func loadMemberStore(ctx context.Context, storage stable.Storage) (*memberStore, error) {
	s := &memberStore{storage: storage}
	bs, err := storage.Get(ctx, stableKeyMembers)
	if err != nil {
		if errors.Is(err, stable.ErrNotFound) {
			return s, nil
		}
		return nil, fmt.Errorf("comlink: read persisted members: %w", err)
	}
	var pm pb.PersistedMembership
	if err := proto.Unmarshal(bs, &pm); err != nil {
		return nil, fmt.Errorf("comlink: parse persisted members: %w", err)
	}
	s.members = pm.GetMembers()
	return s, nil
}

// Members returns a defensive copy of the persisted list.
func (s *memberStore) Members() []*pb.ClusterMember {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.ClusterMember, len(s.members))
	for i, m := range s.members {
		out[i] = proto.Clone(m).(*pb.ClusterMember)
	}
	return out
}

// Empty reports whether nothing has ever been persisted.
func (s *memberStore) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.members) == 0
}

// SetAll replaces the persisted set wholesale. Used during
// bootstrap to seed the initial cfg.Members list with a sentinel
// addr of "" (Cluster will overwrite Self's addr at gRPC start).
func (s *memberStore) SetAll(ctx context.Context, members []*pb.ClusterMember) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members = make([]*pb.ClusterMember, len(members))
	for i, m := range members {
		s.members[i] = proto.Clone(m).(*pb.ClusterMember)
	}
	return s.persistLocked(ctx)
}

// Add records a new (replica, addr) entry. No-op if replica is
// already present.
func (s *memberStore) Add(ctx context.Context, replica *pb.ReplicaID, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if bytes.Equal(m.GetId().GetValue(), replica.GetValue()) {
			// Update addr if it changed; otherwise no-op.
			if m.GetAddr() == addr {
				return nil
			}
			m.Addr = addr
			return s.persistLocked(ctx)
		}
	}
	s.members = append(s.members, &pb.ClusterMember{
		Id:   proto.Clone(replica).(*pb.ReplicaID),
		Addr: addr,
	})
	return s.persistLocked(ctx)
}

// Remove drops a replica. No-op if not present.
func (s *memberStore) Remove(ctx context.Context, replica *pb.ReplicaID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.members[:0]
	changed := false
	for _, m := range s.members {
		if bytes.Equal(m.GetId().GetValue(), replica.GetValue()) {
			changed = true
			continue
		}
		kept = append(kept, m)
	}
	if !changed {
		return nil
	}
	s.members = kept
	return s.persistLocked(ctx)
}

func (s *memberStore) persistLocked(ctx context.Context) error {
	bs, err := proto.Marshal(&pb.PersistedMembership{Members: s.members})
	if err != nil {
		return fmt.Errorf("comlink: marshal persisted members: %w", err)
	}
	if err := s.storage.Put(ctx, stableKeyMembers, bs); err != nil {
		return fmt.Errorf("comlink: persist members: %w", err)
	}
	return nil
}
