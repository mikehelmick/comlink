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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/transport/memory"
)

// snapshottableSM is a StateMachine that, beyond Apply, exposes
// a deterministic digest of its accumulated state. Used by the
// cross-replica comparison in determinism tests.
type snapshottableSM interface {
	comlink.StateMachine
	Snapshot() []byte
}

// xorSum is a deterministic SM: state is the XOR of every
// Apply'd payload (interpreted as a 1-byte tag) summed across
// all applies. Identical sequences → identical Snapshot.
type xorSum struct {
	mu  sync.Mutex
	sum byte
	n   int
}

func (s *xorSum) Apply(ctx context.Context, msg *comlink.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range msg.Payload {
		s.sum ^= b
	}
	s.n++
}

func (s *xorSum) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, 9)
	out[0] = s.sum
	binary.LittleEndian.PutUint64(out[1:], uint64(s.n))
	return out
}

// timestampedSM is INTENTIONALLY non-deterministic: it mixes the
// wall-clock time at the moment of Apply into its state digest.
// Two replicas will see Apply at slightly different times and
// therefore produce divergent Snapshots — exactly the failure
// mode we want a determinism check to catch.
type timestampedSM struct {
	mu sync.Mutex
	h  []byte
}

func (s *timestampedSM) Apply(ctx context.Context, msg *comlink.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	digest := sha256.New()
	digest.Write(s.h)
	digest.Write(msg.Payload)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(now))
	digest.Write(buf[:])
	s.h = digest.Sum(nil)
}

func (s *timestampedSM) Snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, len(s.h))
	copy(out, s.h)
	return out
}

// detectDivergence returns "" if every snapshot is byte-equal,
// otherwise a human-readable description of the first mismatch.
func detectDivergence(snapshots [][]byte) string {
	if len(snapshots) < 2 {
		return ""
	}
	for i := 1; i < len(snapshots); i++ {
		if !equalBytes(snapshots[0], snapshots[i]) {
			return fmt.Sprintf("replica 0 snapshot %x != replica %d snapshot %x",
				snapshots[0], i, snapshots[i])
		}
	}
	return ""
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// runDeterminismScenario boots a 3-replica cluster on the
// in-memory transport, submits N messages distributed round-robin
// across replicas, waits for convergence on count, then returns
// the per-replica snapshots for cross-comparison.
func runDeterminismScenario(t *testing.T, factory func() snapshottableSM) [][]byte {
	t.Helper()
	ctx := context.Background()
	sched := memory.NewScheduler(1)
	defer sched.Close()

	replicas := []string{"alice", "bob", "carol"}
	mems := make([]comlink.ReplicaID, len(replicas))
	for i, name := range replicas {
		mems[i] = id16(name)
	}

	type node struct {
		cluster *comlink.Cluster
		sub     *comlink.Substrate
		sm      snapshottableSM
	}
	nodes := make([]*node, len(replicas))
	convID, _ := comlink.NewConversationID()
	for i, name := range replicas {
		net, err := sched.Connect(pbID(id16(name)))
		if err != nil {
			t.Fatal(err)
		}
		c, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
			Self:      id16(name),
			Members:   []comlink.ReplicaID{id16(name)},
			DataDir:   t.TempDir(),
			Bootstrap: &comlink.BootstrapConfig{Force: true},
			Transport: comlink.TransportConfig{Network: net},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		sm := factory()
		sub, err := c.NewSubstrate(ctx, comlink.SubstrateConfig{
			ConversationID: convID,
			Members:        mems,
			Ordering:       comlink.OrderingTotal,
			StateMachine:   sm,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		nodes[i] = &node{cluster: c, sub: sub, sm: sm}
	}

	stop := make(chan struct{})
	go func() {
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
	}()
	defer close(stop)

	const realRounds = 3
	const settleRounds = 1
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, n *node) {
			defer wg.Done()
			for round := 0; round < realRounds+settleRounds; round++ {
				subCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				if _, err := n.sub.Submit(subCtx, []byte{byte(idx), byte(round)}); err != nil {
					t.Errorf("replica %d round %d Submit: %v", idx, round, err)
					cancel()
					return
				}
				cancel()
			}
		}(i, n)
	}
	wg.Wait()

	// Wait for every replica to have applied the same number of
	// messages — snapshot comparison requires they're at the
	// SAME point in the ordered stream, not just past some
	// threshold.
	want := realRounds * len(nodes)
	deadline := time.Now().Add(10 * time.Second)
	for {
		counts := make([]int, len(nodes))
		minC, maxC := -1, -1
		for i, n := range nodes {
			counts[i] = appliedCount(n.sm)
			if minC < 0 || counts[i] < minC {
				minC = counts[i]
			}
			if counts[i] > maxC {
				maxC = counts[i]
			}
		}
		if minC >= want && minC == maxC {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("warning: counts %v did not converge to == within deadline", counts)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	snapshots := make([][]byte, len(nodes))
	for i, n := range nodes {
		snapshots[i] = n.sm.Snapshot()
	}
	return snapshots
}

// appliedCount extracts the apply count from the xorSum / a
// best-effort fallback for timestampedSM (uses snapshot length
// as a coarse heuristic — actually we just always-return-want
// for non-counting SMs, but for our test SMs only xorSum needs
// counting).
func appliedCount(sm snapshottableSM) int {
	if x, ok := sm.(*xorSum); ok {
		x.mu.Lock()
		defer x.mu.Unlock()
		return x.n
	}
	// For SMs that don't expose a count, return MaxInt so the
	// deadline-poll above isn't gated on them. The convergence
	// of THAT test is judged purely by snapshot divergence.
	return 1 << 30
}

// TestDeterministicSMReplicasConverge: a deterministic SM
// applied through OrderingTotal yields identical snapshots on
// every replica. This is the control case — if THIS test ever
// fails, comlink's ordering layer has broken.
func TestDeterministicSMReplicasConverge(t *testing.T) {
	snapshots := runDeterminismScenario(t, func() snapshottableSM { return &xorSum{} })
	if msg := detectDivergence(snapshots); msg != "" {
		t.Fatalf("deterministic SM diverged across replicas: %s", msg)
	}
}

// TestNondeterministicSMReplicasDiverge: a SM that depends on
// wall-clock time produces different snapshots on different
// replicas, and the cross-replica comparison catches it.
//
// This test demonstrates the failure mode the comparison
// detects — it's a positive test for the detector itself, not
// a defect in comlink. If you ever change the SM implementation
// to be deterministic again, this test will rightfully fail.
func TestNondeterministicSMReplicasDiverge(t *testing.T) {
	snapshots := runDeterminismScenario(t, func() snapshottableSM { return &timestampedSM{} })
	if msg := detectDivergence(snapshots); msg == "" {
		t.Fatal("non-deterministic SM (timestampedSM) produced identical snapshots across replicas — the detector failed to catch the divergence (or the test is racing tighter than wall-clock resolution)")
	}
}
