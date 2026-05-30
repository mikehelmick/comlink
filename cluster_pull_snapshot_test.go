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
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
)

// TestPullSnapshotEndToEnd (Phase 10(c)): a real gRPC sponsor
// has a substrate with a Snapshotter SM populated with some
// state. A joiner (separate process / cluster) calls
// PullSnapshot against the sponsor and reassembles the
// snapshot to a file in DataDir, then constructs a Substrate
// with InitialSnapshot = the pulled file. Restore fires;
// the SM state matches the sponsor's.
func TestPullSnapshotEndToEnd(t *testing.T) {
	ctx := context.Background()

	alice := id16("alice")
	bob := id16("bob")

	// Founder (sponsor) on real gRPC.
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

	convID, _ := comlink.NewConversationID()

	// Alice's snapshot-capable substrate. Use Partial ordering
	// so a single-replica substrate can Submit without waiting
	// for peer heartbeats / wave gates.
	aliceSM := newPullableSM()
	aliceSub, err := aliceCluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{alice},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   aliceSM,
	})
	if err != nil {
		t.Fatalf("alice NewSubstrate: %v", err)
	}
	defer aliceSub.Close()

	// Populate state.
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		want[k] = v
		op, _ := json.Marshal(pullableOp{K: k, V: v})
		if _, err := aliceSub.Submit(ctx, op); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	// Bob is a "joiner" Cluster — separate DataDir, different
	// kind cluster than alice. Doesn't need to actually join
	// alice's system conv to pull a snapshot; PullSnapshot is
	// independent of system membership.
	bobCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      bob,
		Members:   []comlink.ReplicaID{bob},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatalf("bob NewCluster: %v", err)
	}
	defer bobCluster.Close()

	pullCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	snap, err := bobCluster.PullSnapshot(pullCtx, aliceCluster.ListenAddr(), convID)
	if err != nil {
		t.Fatalf("PullSnapshot: %v", err)
	}
	if snap.Reader == nil {
		t.Fatal("PullSnapshot returned nil Reader")
	}
	if snap.ThroughOffset == 0 {
		// alice's substrate ran 50 Apply's; through_offset should
		// be the offset of the LAST one (non-zero).
		t.Errorf("through_offset = 0, want non-zero")
	}

	// Build bob's substrate with the pulled snapshot.
	bobSM := newPullableSM()
	bobSub, err := bobCluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID:  convID,
		Members:         []comlink.ReplicaID{bob},
		Ordering:        comlink.OrderingPartial,
		StateMachine:    bobSM,
		InitialSnapshot: snap,
	})
	if err != nil {
		t.Fatalf("bob NewSubstrate w/ snapshot: %v", err)
	}
	defer bobSub.Close()

	// Close the staged-file reader now that Restore has consumed
	// it. (Some readers implement io.Closer; ours does.)
	if c, ok := snap.Reader.(io.Closer); ok {
		_ = c.Close()
	}

	// Bob's SM should match alice's exactly.
	gotData, restored := bobSM.snapshot()
	if !restored {
		t.Fatal("bob's SM Restore was not called")
	}
	if len(gotData) != len(want) {
		t.Fatalf("bob len(state) = %d, want %d", len(gotData), len(want))
	}
	for k, v := range want {
		if got := gotData[k]; got != v {
			t.Errorf("bob[%q] = %q, want %q", k, got, v)
		}
	}
}

// TestAutoBootstrapFromSponsor (Phase 10(d)): a substrate
// constructed with AutoBootstrapFromSponsor=true on a joiner
// Cluster (sponsors set) auto-pulls a snapshot during
// NewSubstrate and Restores the SM — no manual PullSnapshot
// call from the app code.
func TestAutoBootstrapFromSponsor(t *testing.T) {
	ctx := context.Background()
	alice := id16("alice")
	bob := id16("bob")
	clusterID, _ := comlink.NewClusterID()

	// Alice founds with a known ClusterID so bob can join.
	aliceCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    alice,
		Members: []comlink.ReplicaID{alice, bob}, // pre-known so VoteIn isn't needed
		DataDir: t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{
			Force:     true,
			ClusterID: clusterID,
		},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer aliceCluster.Close()

	convID, _ := comlink.NewConversationID()
	aliceSM := newPullableSM()
	aliceSub, err := aliceCluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{alice, bob},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   aliceSM,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer aliceSub.Close()

	// Populate alice's state.
	want := map[string]string{}
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		want[k] = v
		op, _ := json.Marshal(pullableOp{K: k, V: v})
		if _, err := aliceSub.Submit(ctx, op); err != nil {
			t.Fatalf("alice Submit %d: %v", i, err)
		}
	}

	// Bob — joiner Cluster with alice as sponsor + same ClusterID.
	bobCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:    bob,
		Members: []comlink.ReplicaID{alice, bob},
		DataDir: t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{
			Force:     true,
			ClusterID: clusterID,
		},
		Transport: comlink.TransportConfig{
			Listen: "127.0.0.1:0",
			Sponsors: []comlink.Sponsor{
				{ID: alice, Addr: aliceCluster.ListenAddr()},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bobCluster.Close()

	// Build bob's substrate with AutoBootstrapFromSponsor — no
	// manual PullSnapshot call. The substrate should pull,
	// Restore, and come up with alice's state already installed.
	bobSM := newPullableSM()
	bobSub, err := bobCluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID:           convID,
		Members:                  []comlink.ReplicaID{alice, bob},
		Ordering:                 comlink.OrderingPartial,
		StateMachine:             bobSM,
		AutoBootstrapFromSponsor: true,
	})
	if err != nil {
		t.Fatalf("bob NewSubstrate w/ AutoBootstrapFromSponsor: %v", err)
	}
	defer bobSub.Close()

	got, restored := bobSM.snapshot()
	if !restored {
		t.Fatal("AutoBootstrapFromSponsor: SM.Restore was not called")
	}
	if len(got) != len(want) {
		t.Fatalf("AutoBootstrapFromSponsor: len(state) = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if g := got[k]; g != v {
			t.Errorf("bob[%q] = %q, want %q", k, g, v)
		}
	}
}

// TestAutoBootstrapFromSponsorFounderIsNoOp: AutoBootstrapFromSponsor
// on a founder (no sponsors configured) is a silent no-op — the
// substrate comes up empty as expected for the founder role.
func TestAutoBootstrapFromSponsorFounderIsNoOp(t *testing.T) {
	ctx := context.Background()
	alice := id16("alice")
	aliceCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      alice,
		Members:   []comlink.ReplicaID{alice},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer aliceCluster.Close()

	convID, _ := comlink.NewConversationID()
	sm := newPullableSM()
	sub, err := aliceCluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID:           convID,
		Members:                  []comlink.ReplicaID{alice},
		Ordering:                 comlink.OrderingPartial,
		StateMachine:             sm,
		AutoBootstrapFromSponsor: true,
	})
	if err != nil {
		t.Fatalf("founder NewSubstrate w/ AutoBootstrap: %v", err)
	}
	defer sub.Close()

	_, restored := sm.snapshot()
	if restored {
		t.Error("founder: Restore was called but there's no sponsor to pull from")
	}
}

// TestPullSnapshotMissingSource: pulling for an unknown conv
// returns a gRPC NotFound from the server.
func TestPullSnapshotMissingSource(t *testing.T) {
	ctx := context.Background()
	alice := id16("alice")
	aliceCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      alice,
		Members:   []comlink.ReplicaID{alice},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer aliceCluster.Close()

	bobCluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("bob"),
		Members:   []comlink.ReplicaID{id16("bob")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Listen: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bobCluster.Close()

	unknownConv, _ := comlink.NewConversationID()
	pullCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = bobCluster.PullSnapshot(pullCtx, aliceCluster.ListenAddr(), unknownConv)
	if err == nil {
		t.Fatal("PullSnapshot for unknown conv: want error, got nil")
	}
}

// ─── helpers ────────────────────────────────────────────────────

type pullableOp struct {
	K, V string
}

type pullableSM struct {
	data     map[string]string
	maxOff   uint64
	restored bool
}

func newPullableSM() *pullableSM {
	return &pullableSM{data: map[string]string{}}
}

func (s *pullableSM) Apply(_ context.Context, msg *comlink.Message) {
	var op pullableOp
	if err := json.Unmarshal(msg.Payload, &op); err != nil {
		return
	}
	s.data[op.K] = op.V
	if msg.Offset > s.maxOff {
		s.maxOff = msg.Offset
	}
}

type pullablePayload struct {
	Data   map[string]string `json:"data"`
	MaxOff uint64            `json:"max_off"`
}

func (s *pullableSM) Snapshot() ([]byte, uint64, error) {
	bs, err := json.Marshal(pullablePayload{Data: s.data, MaxOff: s.maxOff})
	return bs, s.maxOff, err
}

func (s *pullableSM) Restore(r io.Reader) error {
	bs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var p pullablePayload
	if err := json.Unmarshal(bs, &p); err != nil {
		return err
	}
	s.data = p.Data
	s.maxOff = p.MaxOff
	s.restored = true
	return nil
}

func (s *pullableSM) snapshot() (map[string]string, bool) {
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out, s.restored
}
