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
	"github.com/mikehelmick/comlink/transport/memory"
)

// startClusterNode is a helper that builds a Cluster on the
// supplied scheduler. clusterID is shared across all nodes
// participating in the same logical cluster — without that, each
// node would mint its own ID and derive a different system conv
// ID (so they couldn't talk).
func startClusterNode(t *testing.T, sched *memory.Scheduler, clusterID comlink.ClusterID, self comlink.ReplicaID, mems []comlink.ReplicaID) *comlink.Cluster {
	t.Helper()
	return startClusterNodeAt(t, sched, clusterID, self, mems, t.TempDir())
}

// startClusterNodeAt is like startClusterNode but uses a caller-
// supplied DataDir (so tests can close and reopen).
func startClusterNodeAt(t *testing.T, sched *memory.Scheduler, clusterID comlink.ClusterID, self comlink.ReplicaID, mems []comlink.ReplicaID, dataDir string) *comlink.Cluster {
	t.Helper()
	net, err := sched.Connect(pbID(self))
	if err != nil {
		t.Fatal(err)
	}
	c, err := comlink.NewCluster(context.Background(), comlink.ClusterConfig{
		Self:    self,
		Members: mems,
		DataDir: dataDir,
		Bootstrap: &comlink.BootstrapConfig{
			Force:     true,
			ClusterID: clusterID,
		},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// runScheduler drains the in-memory scheduler until stop closes.
func runScheduler(sched *memory.Scheduler, stop <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			sched.RunAll()
		}
	}
}

// TestClusterVoteInAddsMember runs a 2-node cluster founded with
// {alice}, then has alice VoteIn bob (who is already running with
// his initial ML = {alice, bob} because he's a peer in the same
// scheduler). After VoteIn, alice's Members() should include bob.
//
// Note: the joiner side here is started with its own bootstrap
// (Force=true); the realistic flow would have bob join via a
// sponsor handshake, which is Phase 5(i). For now we just verify
// the protocol & persistence on the founder side.
func TestClusterVoteInPersists(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	alice := id16("alice")
	bob := id16("bob")
	clusterID, err := comlink.NewClusterID()
	if err != nil {
		t.Fatal(err)
	}

	// Both nodes founded with ML = {alice, bob} and the SAME
	// ClusterID so they derive the same system conv ID and can
	// communicate. (Phase 5(i) will remove this hand-shaking —
	// bob will learn the ID via sponsor handshake.)
	mems := []comlink.ReplicaID{alice, bob}
	aliceCluster := startClusterNode(t, sched, clusterID, alice, mems)
	defer aliceCluster.Close()
	bobCluster := startClusterNode(t, sched, clusterID, bob, mems)
	defer bobCluster.Close()

	stop := make(chan struct{})
	go runScheduler(sched, stop)
	defer close(stop)

	// Add carol via VoteIn from alice. Carol is not running, but
	// the protocol only requires quorum among CURRENT members; alice
	// + bob will both Ack.
	carol := id16("carol")
	voteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := aliceCluster.VoteIn(voteCtx, carol, "carol.example:7000"); err != nil {
		t.Fatalf("alice VoteIn carol: %v", err)
	}

	// alice's ML should now include carol.
	found := false
	for _, m := range aliceCluster.Members() {
		if m.Equal(carol) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("alice.Members() = %v, missing carol", aliceCluster.Members())
	}

	// bob's ML should also reflect (eventually — the MemberAdd
	// commit message is on the system conv).
	deadline := time.Now().Add(3 * time.Second)
	for {
		hasCarol := false
		for _, m := range bobCluster.Members() {
			if m.Equal(carol) {
				hasCarol = true
				break
			}
		}
		if hasCarol {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob.Members() = %v, missing carol after deadline", bobCluster.Members())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestClusterVoteInSurvivesRestart: VoteIn carol, close alice,
// reopen alice (no Force) — the persisted membership should
// restore carol into the system conv ML.
func TestClusterVoteInSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	alice := id16("alice")
	bob := id16("bob")
	carol := id16("carol")
	clusterID, err := comlink.NewClusterID()
	if err != nil {
		t.Fatal(err)
	}

	sched := memory.NewScheduler(1)

	openAlice := func(bootstrap *comlink.BootstrapConfig) *comlink.Cluster {
		net, err := sched.Connect(pbID(alice))
		if err != nil {
			t.Fatal(err)
		}
		c, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
			Self:      alice,
			Members:   []comlink.ReplicaID{alice, bob},
			DataDir:   dataDir,
			Bootstrap: bootstrap,
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	aliceCluster := openAlice(&comlink.BootstrapConfig{Force: true, ClusterID: clusterID})
	bobCluster := startClusterNode(t, sched, clusterID, bob, []comlink.ReplicaID{alice, bob})
	defer bobCluster.Close()

	stop := make(chan struct{})
	go runScheduler(sched, stop)

	voteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := aliceCluster.VoteIn(voteCtx, carol, "carol.example:7000"); err != nil {
		cancel()
		t.Fatalf("alice VoteIn carol: %v", err)
	}
	cancel()

	// Wait briefly so the OnMembershipChange goroutine's Put
	// completes before we close storage.
	time.Sleep(100 * time.Millisecond)

	close(stop)
	if err := aliceCluster.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bobCluster.Close(); err != nil {
		t.Fatal(err)
	}
	_ = sched.Close()

	// Restart alice — no Force. Persisted ML should reload.
	sched2 := memory.NewScheduler(1)
	defer sched2.Close()
	sched = sched2

	reopened := openAlice(nil)
	defer reopened.Close()

	mems := reopened.Members()
	gotCarol := false
	for _, m := range mems {
		if m.Equal(carol) {
			gotCarol = true
			break
		}
	}
	if !gotCarol {
		t.Fatalf("after restart Members = %v, missing carol — persistence broken", mems)
	}
}
