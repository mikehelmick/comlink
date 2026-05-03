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

package failure_test

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/failure"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
)

func r(tag string) *pb.ReplicaID {
	b := make([]byte, 16)
	copy(b, tag)
	return &pb.ReplicaID{Value: b}
}

// These tests use real time with short intervals. The Detector
// runs a tick goroutine that processes timer fires asynchronously,
// which makes the manual-clock + Advance pattern racy (the
// goroutine has to Reset the timer after each tick, and a fast
// test thread can issue a second Advance before that Reset has
// happened, missing the next deadline).
//
// Tests stay tight (<200ms each typically). If they ever flake on
// CI we'll switch to a synchronous-tick-injection pattern.

// waitFor polls cond until it returns true or deadline elapses.
func waitFor(deadline time.Duration, cond func() bool) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestHeartbeatFiresWhenQuiet(t *testing.T) {
	var hbCount atomic.Int32
	d := failure.New(failure.Config{
		Self:              r("alice"),
		Members:           []*pb.ReplicaID{r("alice"), r("bob")},
		Clock:             clock.NewSystem(),
		QuietInterval:     50 * time.Millisecond,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      10 * time.Millisecond,
		SendHeartbeat:     func() { hbCount.Add(1) },
		OnSuspect:         func(*pb.ReplicaID) {},
	})
	defer d.Close()

	if !waitFor(time.Second, func() bool { return hbCount.Load() >= 1 }) {
		t.Fatalf("heartbeat did not fire within QuietInterval; count=%d", hbCount.Load())
	}
}

func TestHeartbeatSuppressedByActivity(t *testing.T) {
	var hbCount atomic.Int32
	d := failure.New(failure.Config{
		Self:              r("alice"),
		Members:           []*pb.ReplicaID{r("alice"), r("bob")},
		Clock:             clock.NewSystem(),
		QuietInterval:     150 * time.Millisecond,
		SuspicionInterval: 5 * time.Second,
		TickInterval:      10 * time.Millisecond,
		SendHeartbeat:     func() { hbCount.Add(1) },
		OnSuspect:         func(*pb.ReplicaID) {},
	})
	defer d.Close()

	// Hammer NoteSent for a bit; the quiet timer should keep
	// resetting and no heartbeat should fire.
	end := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(end) {
		d.NoteSent()
		time.Sleep(20 * time.Millisecond)
	}
	if got := hbCount.Load(); got != 0 {
		t.Fatalf("heartbeat fired despite continuous NoteSent: count=%d", got)
	}

	// Stop activity; heartbeat should fire after QuietInterval.
	if !waitFor(500*time.Millisecond, func() bool { return hbCount.Load() >= 1 }) {
		t.Fatalf("heartbeat did not fire after activity stopped; count=%d", hbCount.Load())
	}
}

func TestSuspicionFiresAfterTimeout(t *testing.T) {
	var suspectMu sync.Mutex
	var suspected []*pb.ReplicaID
	d := failure.New(failure.Config{
		Self:              r("alice"),
		Members:           []*pb.ReplicaID{r("alice"), r("bob")},
		Clock:             clock.NewSystem(),
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 50 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
		SendHeartbeat:     func() {},
		OnSuspect: func(rep *pb.ReplicaID) {
			suspectMu.Lock()
			defer suspectMu.Unlock()
			suspected = append(suspected, rep)
		},
	})
	defer d.Close()

	if !waitFor(time.Second, func() bool {
		suspectMu.Lock()
		defer suspectMu.Unlock()
		return len(suspected) >= 1
	}) {
		t.Fatalf("OnSuspect did not fire for bob within suspicion interval")
	}
	suspectMu.Lock()
	got := suspected[0]
	suspectMu.Unlock()
	if !bytes.Equal(got.GetValue(), r("bob").GetValue()) {
		t.Fatalf("suspected = %x, want bob", got.GetValue())
	}
	if !d.Suspected(r("bob")) {
		t.Errorf("d.Suspected(bob) = false, want true")
	}
}

func TestNoteReceivedClearsSuspicion(t *testing.T) {
	var suspectCount atomic.Int32
	d := failure.New(failure.Config{
		Self:              r("alice"),
		Members:           []*pb.ReplicaID{r("alice"), r("bob")},
		Clock:             clock.NewSystem(),
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 50 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
		SendHeartbeat:     func() {},
		OnSuspect:         func(*pb.ReplicaID) { suspectCount.Add(1) },
	})
	defer d.Close()

	if !waitFor(time.Second, func() bool { return suspectCount.Load() >= 1 }) {
		t.Fatalf("first suspicion did not fire")
	}
	if !d.Suspected(r("bob")) {
		t.Fatal("bob not marked suspected")
	}

	d.NoteReceived(r("bob"))
	if d.Suspected(r("bob")) {
		t.Fatal("bob still suspected after NoteReceived")
	}

	if !waitFor(time.Second, func() bool { return suspectCount.Load() >= 2 }) {
		t.Fatalf("second suspicion did not fire after recovery; count=%d", suspectCount.Load())
	}
}

// TestNoSelfSuspicion: the Detector should never suspect itself.
func TestNoSelfSuspicion(t *testing.T) {
	var selfSuspected atomic.Bool
	d := failure.New(failure.Config{
		Self:              r("alice"),
		Members:           []*pb.ReplicaID{r("alice"), r("bob")},
		Clock:             clock.NewSystem(),
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 30 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
		SendHeartbeat:     func() {},
		OnSuspect: func(rep *pb.ReplicaID) {
			if bytes.Equal(rep.GetValue(), r("alice").GetValue()) {
				selfSuspected.Store(true)
			}
		},
	})
	defer d.Close()

	time.Sleep(100 * time.Millisecond)
	if selfSuspected.Load() {
		t.Fatal("Detector suspected self")
	}
}
