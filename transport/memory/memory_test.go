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

package memory_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/transport"
	"github.com/mikehelmick/comlink/transport/memory"
)

func replica(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// drainAll returns every payload received on net's recv channel.
// Reads until n payloads have arrived; fails the test if fewer arrive
// (no timeout — tests should call sched.RunAll first so all expected
// messages are already delivered).
func drainAll(t *testing.T, net transport.Network, n int) [][]byte {
	t.Helper()
	got := make([][]byte, 0, n)
	for range n {
		select {
		case in, ok := <-net.Recv():
			if !ok {
				t.Fatalf("Recv closed before %d messages drained", n)
			}
			got = append(got, in.Payload)
		default:
			t.Fatalf("Recv had only %d/%d messages", len(got), n)
		}
	}
	return got
}

func TestPointToPointDelivery(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(1)
	defer s.Close()

	a, _ := s.Connect(replica("a"))
	b, _ := s.Connect(replica("b"))

	if err := a.Send(ctx, replica("b"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := s.Pending(); got != 1 {
		t.Fatalf("Pending after Send = %d, want 1", got)
	}
	s.RunAll()
	got := drainAll(t, b, 1)
	if !bytes.Equal(got[0], []byte("hello")) {
		t.Fatalf("got %q, want %q", got[0], "hello")
	}
}

func TestSendToUnknownPeerErrors(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(1)
	defer s.Close()
	a, _ := s.Connect(replica("a"))
	if err := a.Send(ctx, replica("ghost"), []byte("x")); !errors.Is(err, transport.ErrUnknownPeer) {
		t.Fatalf("Send to unknown peer: err = %v, want ErrUnknownPeer", err)
	}
}

func TestConnectTwiceErrors(t *testing.T) {
	s := memory.NewScheduler(1)
	defer s.Close()
	if _, err := s.Connect(replica("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Connect(replica("a")); err == nil {
		t.Fatal("second Connect of same replica returned no error")
	}
}

// TestDeterministicReorder is the Phase 0 exit-criterion check that
// the in-memory transport reproduces a fixed message ordering across
// runs given the same seed.
func TestDeterministicReorder(t *testing.T) {
	ctx := context.Background()
	const N = 30

	run := func() []string {
		s := memory.NewScheduler(42)
		defer s.Close()
		s.SetReorder(true)

		a, _ := s.Connect(replica("a"))
		b, _ := s.Connect(replica("b"))
		for i := range N {
			if err := a.Send(ctx, replica("b"), fmt.Appendf(nil, "%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		s.RunAll()
		got := drainAll(t, b, N)
		out := make([]string, 0, N)
		for _, p := range got {
			out = append(out, string(p))
		}
		return out
	}

	first := run()
	for trial := range 5 {
		again := run()
		if !slices.Equal(first, again) {
			t.Fatalf("trial %d: same seed produced different ordering\nfirst: %v\nagain: %v", trial, first, again)
		}
	}
	// Sanity: with reorder enabled, the order should not be the
	// trivial 0..N-1 FIFO.
	fifo := make([]string, N)
	for i := range N {
		fifo[i] = fmt.Sprintf("%d", i)
	}
	if slices.Equal(first, fifo) {
		t.Fatalf("reorder produced FIFO order; expected scrambling. got: %v", first)
	}
}

func TestFIFOByDefault(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(1)
	defer s.Close()
	a, _ := s.Connect(replica("a"))
	b, _ := s.Connect(replica("b"))
	const N = 10
	for i := range N {
		if err := a.Send(ctx, replica("b"), fmt.Appendf(nil, "%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	s.RunAll()
	got := drainAll(t, b, N)
	for i, p := range got {
		if string(p) != fmt.Sprintf("%d", i) {
			t.Fatalf("FIFO violation at idx %d: got %q, want %q", i, p, fmt.Sprintf("%d", i))
		}
	}
}

func TestDropProbabilityHalves(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(7)
	defer s.Close()
	s.SetDropProb(0.5)

	a, _ := s.Connect(replica("a"))
	b, _ := s.Connect(replica("b"))
	const N = 1000
	for i := range N {
		if err := a.Send(ctx, replica("b"), fmt.Appendf(nil, "%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	s.RunAll()

	// Drain everything that did arrive.
	delivered := 0
loop:
	for {
		select {
		case _, ok := <-b.Recv():
			if !ok {
				break loop
			}
			delivered++
		default:
			break loop
		}
	}
	// Expected ~500; allow a wide tolerance because this test exists
	// to confirm the knob *works*, not to check statistical accuracy.
	if delivered < N/4 || delivered > 3*N/4 {
		t.Fatalf("with dropProb=0.5, delivered=%d/%d (expected roughly N/2)", delivered, N)
	}
}

func TestPartitionBlocksSpecificPair(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(1)
	defer s.Close()

	aID, bID, cID := replica("a"), replica("b"), replica("c")
	a, _ := s.Connect(aID)
	b, _ := s.Connect(bID)
	c, _ := s.Connect(cID)

	// Block a -> b only. a -> c and b -> a still flow.
	s.AddPartition(func(from, to *pb.ReplicaID) bool {
		return bytes.Equal(from.GetValue(), aID.GetValue()) && bytes.Equal(to.GetValue(), bID.GetValue())
	})

	if err := a.Send(ctx, bID, []byte("a->b")); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(ctx, cID, []byte("a->c")); err != nil {
		t.Fatal(err)
	}
	if err := b.Send(ctx, aID, []byte("b->a")); err != nil {
		t.Fatal(err)
	}
	s.RunAll()

	// b should have nothing.
	select {
	case in := <-b.Recv():
		t.Fatalf("b received %q despite partition", in.Payload)
	default:
	}
	// c should have one.
	got := drainAll(t, c, 1)
	if !bytes.Equal(got[0], []byte("a->c")) {
		t.Fatalf("c got %q, want %q", got[0], "a->c")
	}
	// a should have one.
	got = drainAll(t, a, 1)
	if !bytes.Equal(got[0], []byte("b->a")) {
		t.Fatalf("a got %q, want %q", got[0], "b->a")
	}

	// Heal and try again.
	s.ClearPartitions()
	if err := a.Send(ctx, bID, []byte("a->b after heal")); err != nil {
		t.Fatal(err)
	}
	s.RunAll()
	got = drainAll(t, b, 1)
	if !bytes.Equal(got[0], []byte("a->b after heal")) {
		t.Fatalf("after heal b got %q, want %q", got[0], "a->b after heal")
	}
}

func TestCloseClosesRecvChannels(t *testing.T) {
	s := memory.NewScheduler(1)
	a, _ := s.Connect(replica("a"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-a.Recv(); ok {
		t.Fatal("Recv channel was not closed by scheduler Close")
	}
}

func TestSendOnClosedNetworkErrors(t *testing.T) {
	ctx := context.Background()
	s := memory.NewScheduler(1)
	defer s.Close()
	a, _ := s.Connect(replica("a"))
	_, _ = s.Connect(replica("b"))
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(ctx, replica("b"), []byte("x")); !errors.Is(err, transport.ErrClosed) {
		t.Fatalf("Send on closed: err = %v, want ErrClosed", err)
	}
}
