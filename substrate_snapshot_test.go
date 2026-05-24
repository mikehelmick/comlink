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
	"sync"
	"testing"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/transport/memory"
)

// snapshotterSM is a minimal kvstore-like SM that also implements
// comlink.Snapshotter. State = map[string]string; snapshot is
// JSON of the map + the last-applied offset.
type snapshotterSM struct {
	mu       sync.Mutex
	data     map[string]string
	maxOff   uint64
	restored bool
}

func newSnapshotterSM() *snapshotterSM {
	return &snapshotterSM{data: map[string]string{}}
}

type snapshotterOp struct {
	K, V string
}

type snapshotPayload struct {
	Data     map[string]string `json:"data"`
	MaxOff   uint64            `json:"max_off"`
	Restored bool              `json:"restored"`
}

func (s *snapshotterSM) Apply(_ context.Context, msg *comlink.Message) {
	var op snapshotterOp
	if err := json.Unmarshal(msg.Payload, &op); err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[op.K] = op.V
	if msg.Offset > s.maxOff {
		s.maxOff = msg.Offset
	}
}

func (s *snapshotterSM) Snapshot() ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copied[k] = v
	}
	bs, err := json.Marshal(snapshotPayload{Data: copied, MaxOff: s.maxOff})
	return bs, s.maxOff, err
}

func (s *snapshotterSM) Restore(bytes []byte) error {
	var p snapshotPayload
	if err := json.Unmarshal(bytes, &p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = p.Data
	s.maxOff = p.MaxOff
	s.restored = true
	return nil
}

func (s *snapshotterSM) Snapshot4Test() (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out, s.restored
}

// TestSubstrateRestoreFromInitialSnapshot (Phase 10(b)): a SM
// that implements Snapshotter is fed an InitialSnapshot at
// substrate construction. Restore is called BEFORE the apply
// pump; on Restore returning the SM's state matches the snapshot.
//
// A subsequent Submit succeeds and is Apply'd normally (only
// messages BEYOND the snapshot's throughOffset reach Apply).
func TestSubstrateRestoreFromInitialSnapshot(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	net, err := sched.Connect(pbID(id16("alice")))
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	convID, _ := comlink.NewConversationID()
	sm := newSnapshotterSM()

	// Build an artificial snapshot: {a:1, b:2} at throughOffset=42.
	snapPayload := snapshotPayload{
		Data:   map[string]string{"a": "1", "b": "2"},
		MaxOff: 42,
	}
	bs, err := json.Marshal(snapPayload)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   sm,
		InitialSnapshot: &comlink.Snapshot{
			Bytes:         bs,
			ThroughOffset: 42,
		},
	})
	if err != nil {
		t.Fatalf("NewSubstrate: %v", err)
	}
	defer sub.Close()

	// Restore must have fired before the substrate returned.
	got, restored := sm.Snapshot4Test()
	if !restored {
		t.Fatal("Restore was never called on the SM")
	}
	if got["a"] != "1" || got["b"] != "2" || len(got) != 2 {
		t.Fatalf("post-Restore state = %v, want {a:1, b:2}", got)
	}

	// Watermark should reflect the snapshot.
	if got, want := sub.SnapshotWatermark(), uint64(42); got != want {
		t.Errorf("SnapshotWatermark = %d, want %d", got, want)
	}

	// Subsequent Submit lands and is Apply'd (the new message's
	// log offset is naturally > 42 — psync writes to a fresh log
	// here, but that doesn't matter for the suppression test:
	// what matters is that BUT a Submit succeeds end-to-end).
	op, _ := json.Marshal(snapshotterOp{K: "c", V: "3"})
	if err := sub.Submit(ctx, op); err != nil {
		t.Fatalf("post-restore Submit: %v", err)
	}
	got, _ = sm.Snapshot4Test()
	if got["c"] != "3" {
		t.Errorf("post-restore Submit not applied: state=%v", got)
	}
}

// TestSubstrateInitialSnapshotRequiresSnapshotter: configuring
// InitialSnapshot on a SM that doesn't implement Snapshotter is
// a config error.
func TestSubstrateInitialSnapshotRequiresSnapshotter(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	net, _ := sched.Connect(pbID(id16("alice")))
	cluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	convID, _ := comlink.NewConversationID()
	// counterSM does NOT implement Snapshotter.
	_, err = cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   &counterSM{},
		InitialSnapshot: &comlink.Snapshot{
			Bytes:         []byte("garbage"),
			ThroughOffset: 1,
		},
	})
	if err == nil {
		t.Fatal("NewSubstrate accepted InitialSnapshot on non-Snapshotter SM")
	}
}

// TestSubstrateSnapshotWatermarkMonotonic: AdvanceSnapshotWatermark
// is monotonic — going backwards is silently ignored.
func TestSubstrateSnapshotWatermarkMonotonic(t *testing.T) {
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	net, _ := sched.Connect(pbID(id16("alice")))
	cluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
		Self:      id16("alice"),
		Members:   []comlink.ReplicaID{id16("alice")},
		DataDir:   t.TempDir(),
		Bootstrap: &comlink.BootstrapConfig{Force: true},
		Transport: comlink.TransportConfig{Network: net},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	convID, _ := comlink.NewConversationID()
	sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
		ConversationID: convID,
		Members:        []comlink.ReplicaID{id16("alice")},
		Ordering:       comlink.OrderingPartial,
		StateMachine:   newSnapshotterSM(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if got := sub.SnapshotWatermark(); got != 0 {
		t.Fatalf("initial watermark = %d, want 0", got)
	}
	sub.AdvanceSnapshotWatermark(10)
	if got := sub.SnapshotWatermark(); got != 10 {
		t.Errorf("after advance(10): %d, want 10", got)
	}
	sub.AdvanceSnapshotWatermark(5) // backwards — ignored
	if got := sub.SnapshotWatermark(); got != 10 {
		t.Errorf("after backwards advance(5): %d, want still 10", got)
	}
	sub.AdvanceSnapshotWatermark(20)
	if got := sub.SnapshotWatermark(); got != 20 {
		t.Errorf("after advance(20): %d, want 20", got)
	}
}
