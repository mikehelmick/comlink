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

package comlink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mikehelmick/comlink"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport/memory"
)

// id16 generates a deterministic ReplicaID from a tag for tests.
func id16(tag string) comlink.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return comlink.ReplicaID(b)
}

// pbID is the internal pb.ReplicaID equivalent — needed only to
// register with the in-memory transport scheduler in tests.
func pbID(id comlink.ReplicaID) *pb.ReplicaID {
	return &pb.ReplicaID{Value: id}
}

// founderClusterConfig builds a ClusterConfig that bootstraps a
// fresh single-node cluster against a fresh in-memory transport.
func founderClusterConfig(t *testing.T, sched *memory.Scheduler, self string, members ...string) comlink.ClusterConfig {
	t.Helper()
	selfID := id16(self)
	mems := make([]comlink.ReplicaID, len(members))
	for i, m := range members {
		mems[i] = id16(m)
	}
	net, err := sched.Connect(pbID(selfID))
	if err != nil {
		t.Fatal(err)
	}
	return comlink.ClusterConfig{
		Self:      selfID,
		Members:   mems,
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{
			Network: net,
		},
	}
}

func TestNewClusterFounderMintsClusterID(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	c, err := comlink.NewCluster(ctx, founderClusterConfig(t, sched, "alice", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if id := c.ClusterID(); len(id) != 16 {
		t.Fatalf("ClusterID len = %d, want 16", len(id))
	}
	if !c.Self().Equal(id16("alice")) {
		t.Fatalf("Self = %x, want alice", c.Self())
	}
	if got := c.Members(); len(got) != 1 || !got[0].Equal(id16("alice")) {
		t.Fatalf("Members = %v, want [alice]", got)
	}
	// SystemConversationID is deterministic from ClusterID.
	if !c.SystemConversationID().Equal(comlink.SystemConversationID(c.ClusterID())) {
		t.Fatalf("SystemConversationID does not equal SystemConversationID(ClusterID())")
	}
}

func TestNewClusterRequiresSelfInMembers(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	cfg := founderClusterConfig(t, sched, "alice", "bob") // self alice, members [bob]
	_, err := comlink.NewCluster(ctx, cfg)
	if err == nil {
		t.Fatal("NewCluster with self not in members did not error")
	}
}

func TestNewClusterRequiresTransport(t *testing.T) {
	ctx := context.Background()
	cfg := comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		// no Network, no Listen
	}
	_, err := comlink.NewCluster(ctx, cfg)
	if err == nil {
		t.Fatal("NewCluster with no transport did not error")
	}
}

func TestNewClusterWithoutBootstrapAndNoStateErrors(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	cfg := founderClusterConfig(t, sched, "alice", "alice")
	cfg.Bootstrap = nil // no Force; no persisted state -> error

	_, err := comlink.NewCluster(ctx, cfg)
	if !errors.Is(err, comlink.ErrBootstrapRequired) {
		t.Fatalf("err = %v, want ErrBootstrapRequired", err)
	}
}

func TestNewClusterClusterIDPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()

	dataDir := t.TempDir()

	// First start: bootstrap.
	sched1 := memory.NewScheduler(1)
	net1, _ := sched1.Connect(pbID(id16("alice")))
	cfg := comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   dataDir,
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net1},
	}
	c1, err := comlink.NewCluster(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := c1.ClusterID()
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	_ = sched1.Close()

	// Second start: same DataDir, no Force -> loads persisted ID.
	sched2 := memory.NewScheduler(1)
	defer sched2.Close()
	net2, _ := sched2.Connect(pbID(id16("alice")))
	cfg.Bootstrap = nil
	cfg.Transport.Network = net2
	c2, err := comlink.NewCluster(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if !first.Equal(c2.ClusterID()) {
		t.Fatalf("ClusterID changed across restart: %v -> %v", first, c2.ClusterID())
	}
}
