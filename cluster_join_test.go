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
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
)

// TestSponsorHandshakeAdmitsJoiner exercises the full Phase 5(h)
// flow over the real gRPC transport:
//
//   1. Alice founds a cluster (Bootstrap.Force).
//   2. Bob starts with NO Force, NO persisted ClusterID, just a
//      Sponsors entry pointing at alice's gRPC addr.
//   3. Bob's NewCluster dials alice's Cluster.Join. Alice VoteIns
//      bob, persists the change, returns (ClusterID, post-add ML).
//   4. Bob installs both locally and finishes startup.
//   5. Verify bob's persisted ML contains both alice and bob,
//      bob's ClusterID matches alice's, and alice's ML reflects
//      bob.
func TestSponsorHandshakeAdmitsJoiner(t *testing.T) {
	ctx := context.Background()

	alice := id16("alice")
	bob := id16("bob")

	// Alice — single-node founder on a real gRPC listener.
	aliceCfg := comlink.ClusterConfig{
		Self:      alice,
		Members:   []comlink.ReplicaID{alice},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	}
	aliceCluster, err := comlink.NewCluster(ctx, aliceCfg)
	if err != nil {
		t.Fatalf("alice NewCluster: %v", err)
	}
	defer aliceCluster.Close()
	aliceAddr := aliceCluster.ListenAddr()
	if aliceAddr == "" {
		t.Fatal("alice ListenAddr is empty")
	}

	// Bob — joiner. No Force, no persisted state, sponsor = alice.
	bobCfg := comlink.ClusterConfig{
		Self:    bob,
		Members: nil, // joiner mode — sponsors supply ML
		DataDir: t.TempDir(),
		Transport: comlink.TransportConfig{
			Listen: "127.0.0.1:0",
			Sponsors: []comlink.Sponsor{
				{ID: alice, Addr: aliceAddr},
			},
		},
	}
	bobCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	bobCluster, err := comlink.NewCluster(bobCtx, bobCfg)
	if err != nil {
		t.Fatalf("bob NewCluster (sponsor handshake): %v", err)
	}
	defer bobCluster.Close()

	// Bob learned alice's ClusterID.
	if !bobCluster.ClusterID().Equal(aliceCluster.ClusterID()) {
		t.Fatalf("bob ClusterID = %s, want alice's %s",
			bobCluster.ClusterID(), aliceCluster.ClusterID())
	}

	// Bob's ML includes both. (Alice ran VoteIn for bob inside
	// the Join RPC, then returned the post-admission ML.)
	bobMembers := bobCluster.Members()
	if !containsMember(bobMembers, alice) || !containsMember(bobMembers, bob) {
		t.Fatalf("bob Members = %v, want both alice and bob", bobMembers)
	}

	// Alice's ML also reflects bob (the VoteIn applied locally
	// when she handled the Join RPC).
	if !containsMember(aliceCluster.Members(), bob) {
		t.Fatalf("alice Members = %v, missing bob after VoteIn",
			aliceCluster.Members())
	}
}

func containsMember(ms []comlink.ReplicaID, want comlink.ReplicaID) bool {
	for _, m := range ms {
		if m.Equal(want) {
			return true
		}
	}
	return false
}
