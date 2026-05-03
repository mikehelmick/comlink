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

package membership_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	clog "github.com/mikehelmick/comlink/log"
	"github.com/mikehelmick/comlink/membership"
	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
	"github.com/mikehelmick/comlink/transport/memory"
)

// trimFixture is like the standard membership fixture but each
// replica has its own MessageLog instance, exposed for trim
// assertions.
type trimFixture struct {
	t       *testing.T
	sched   *memory.Scheduler
	convs   []*psync.Conversation
	mgrs    []*membership.Manager
	logs    []clog.MessageLog
	members []*pb.ReplicaID
	convID  *pb.ConversationID
}

func setupTrim(t *testing.T, replicas []string, seed uint64, mgrCfg membership.Config) *trimFixture {
	t.Helper()
	convID := &pb.ConversationID{Value: id16("trim-test")}
	members := make([]*pb.ReplicaID, len(replicas))
	for i, name := range replicas {
		members[i] = r(name)
	}
	sched := memory.NewScheduler(seed)
	t.Cleanup(func() { _ = sched.Close() })

	f := &trimFixture{
		t: t, sched: sched, members: members, convID: convID,
	}
	for _, m := range members {
		net, err := sched.Connect(m)
		if err != nil {
			t.Fatal(err)
		}
		lg := clog.NewMemory(convID)
		c, err := psync.New(context.Background(), psync.Config{
			ConversationID: convID, Self: m, Members: members,
			Network: net, Log: lg, Storage: stable.NewMemory(),
			DeliveryBufSize: 1024,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.convs = append(f.convs, c)
		f.logs = append(f.logs, lg)

		cfg := mgrCfg
		cfg.Conversation = c
		cfg.Self = m
		cfg.Members = members
		cfg.Log = lg
		mgr, err := membership.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		f.mgrs = append(f.mgrs, mgr)
	}
	t.Cleanup(func() {
		for _, c := range f.convs {
			_ = c.Close()
		}
		for _, mgr := range f.mgrs {
			_ = mgr.Close()
		}
	})
	return f
}

func (f *trimFixture) drive(rounds int) {
	for range rounds {
		f.sched.RunAll()
		time.Sleep(2 * time.Millisecond)
	}
}

// TestTrimAdvancesWhenAllReplicasCheckpoint: a 3-replica
// conversation accumulates messages; each replica calls
// SetWatermark; the safe frontier (min) advances and every
// replica's local log is truncated.
func TestTrimAdvancesWhenAllReplicasCheckpoint(t *testing.T) {
	f := setupTrim(t, []string{"alice", "bob", "carol"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})

	// Each replica sends a few messages so the logs grow.
	for round := 0; round < 5; round++ {
		for i, mgr := range f.mgrs {
			if _, err := mgr.SendApp([]byte{byte(i), byte(round)}); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}

	// Each replica's log should have ~5*3=15 entries (a bit more
	// from heartbeats etc.). Verify NextOffset has advanced.
	for i, lg := range f.logs {
		if got := lg.NextOffset(); got < 5 {
			t.Fatalf("replica %d log NextOffset = %d, want at least 5", i, got)
		}
	}

	// Each replica advertises a safe-trim watermark of 3.
	for _, mgr := range f.mgrs {
		mgr.SetWatermark(3)
	}
	// Drive scheduler so the Watermark frames propagate.
	f.drive(20)

	// Every replica's local log FirstOffset should now be 3 (the
	// safe-trim frontier — min of all watermarks).
	if !waitFor(2*time.Second, func() bool {
		for _, lg := range f.logs {
			if lg.FirstOffset() < 3 {
				return false
			}
		}
		return true
	}) {
		offsets := make([]clog.Offset, len(f.logs))
		for i, lg := range f.logs {
			offsets[i] = lg.FirstOffset()
		}
		t.Fatalf("not all logs trimmed to 3; FirstOffsets = %v", offsets)
	}
}

// TestTrimWaitsForLaggingReplica: if one replica never advertises
// a watermark, the others' SetWatermark calls don't actually
// truncate anything (safe-trim frontier = 0 because lagging
// replica has no watermark).
func TestTrimWaitsForLaggingReplica(t *testing.T) {
	f := setupTrim(t, []string{"alice", "bob", "carol"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})

	// Send some messages so logs have content.
	for i, mgr := range f.mgrs {
		if _, err := mgr.SendApp([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	f.drive(10)

	// Alice and bob advertise watermarks; carol stays silent.
	f.mgrs[0].SetWatermark(2)
	f.mgrs[1].SetWatermark(2)
	f.drive(20)

	// Alice's log should not have been trimmed past 0 because
	// carol has no watermark yet.
	if got := f.logs[0].FirstOffset(); got != 0 {
		t.Fatalf("alice log trimmed to %d despite carol having no watermark; should still be 0", got)
	}
}

// TestTrimAdvancesAfterLaggingReplicaCatchesUp: once the lagging
// replica also advertises, trim resumes.
func TestTrimAdvancesAfterLaggingReplicaCatchesUp(t *testing.T) {
	f := setupTrim(t, []string{"alice", "bob", "carol"}, 1, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 10 * time.Second,
		TickInterval:      50 * time.Millisecond,
	})
	for round := 0; round < 5; round++ {
		for i, mgr := range f.mgrs {
			if _, err := mgr.SendApp([]byte{byte(i), byte(round)}); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}

	f.mgrs[0].SetWatermark(2)
	f.mgrs[1].SetWatermark(2)
	f.drive(15)
	if got := f.logs[0].FirstOffset(); got != 0 {
		t.Fatalf("pre-carol-watermark FirstOffset = %d, want 0", got)
	}

	// Carol catches up.
	f.mgrs[2].SetWatermark(2)
	f.drive(20)

	if !waitFor(2*time.Second, func() bool {
		return f.logs[0].FirstOffset() >= 2
	}) {
		t.Fatalf("alice log did not advance after carol's watermark; FirstOffset = %d",
			f.logs[0].FirstOffset())
	}
}

// TestTrimAdvancesAfterVoteOut: a voted-out replica's stale
// watermark is dropped from the min computation, so trim resumes.
func TestTrimAdvancesAfterVoteOut(t *testing.T) {
	f := setupTrim(t, []string{"alice", "bob", "carol"}, 7, membership.Config{
		QuietInterval:     5 * time.Second,
		SuspicionInterval: 80 * time.Millisecond,
		TickInterval:      10 * time.Millisecond,
	})

	for round := 0; round < 5; round++ {
		for i, mgr := range f.mgrs {
			if _, err := mgr.SendApp([]byte{byte(i), byte(round)}); err != nil {
				t.Fatal(err)
			}
		}
		f.drive(8)
	}

	// Take carol offline so alice and bob's FDs fire.
	_ = f.convs[2].Close()
	_ = f.mgrs[2].Close()

	// Drive long enough for soft suspicion to land at alice and bob.
	f.drive(50)
	stop := runScheduler(f.t, &fixture{
		t: f.t, sched: f.sched, members: f.members, convID: f.convID,
		convs: f.convs, mgrs: f.mgrs,
	})
	defer stop()

	if !waitFor(2*time.Second, func() bool {
		return f.mgrs[0].IsSuspected(r("carol")) && f.mgrs[1].IsSuspected(r("carol"))
	}) {
		t.Fatalf("alice and bob did not both suspect carol")
	}

	// Vote carol out.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.mgrs[0].VoteOut(ctx, r("carol")); err != nil {
		t.Fatalf("VoteOut(carol): %v", err)
	}

	// Now alice and bob both advertise watermark 2; trim should
	// advance because carol is no longer in the active list.
	f.mgrs[0].SetWatermark(2)
	f.mgrs[1].SetWatermark(2)

	if !waitFor(2*time.Second, func() bool {
		return f.logs[0].FirstOffset() >= 2 && f.logs[1].FirstOffset() >= 2
	}) {
		t.Fatalf("after VoteOut, trim still pinned by carol's stale watermark; "+
			"alice FirstOffset=%d bob FirstOffset=%d",
			f.logs[0].FirstOffset(), f.logs[1].FirstOffset())
	}
}
