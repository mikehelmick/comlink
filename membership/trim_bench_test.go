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

// BenchmarkCheckpointOverhead measures how much wall-clock time
// the trim path adds to a steady-state SendApp + checkpoint cycle
// — comparable to paper §6.4 / Table 4. Per iteration: app sends
// one message, then SetWatermark advances by one. Higher numbers
// = more checkpointing overhead.
func BenchmarkCheckpointOverhead(b *testing.B) {
	convID := &pb.ConversationID{Value: id16("trim-bench")}
	members := []*pb.ReplicaID{r("alice"), r("bob")}
	sched := memory.NewScheduler(1)
	defer sched.Close()
	aliceNet, _ := sched.Connect(r("alice"))
	bobNet, _ := sched.Connect(r("bob"))

	aliceLog := clog.NewMemory(convID)
	bobLog := clog.NewMemory(convID)

	alice, err := psync.New(context.Background(), psync.Config{
		ConversationID: convID, Self: r("alice"), Members: members,
		Network: aliceNet, Log: aliceLog, Storage: stable.NewMemory(),
		DeliveryBufSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer alice.Close()
	bob, err := psync.New(context.Background(), psync.Config{
		ConversationID: convID, Self: r("bob"), Members: members,
		Network: bobNet, Log: bobLog, Storage: stable.NewMemory(),
		DeliveryBufSize: 4096,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer bob.Close()

	aliceMgr, err := membership.New(membership.Config{
		Conversation: alice, Self: r("alice"), Members: members,
		Log: aliceLog, QuietInterval: 5 * time.Second, SuspicionInterval: 5 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer aliceMgr.Close()
	bobMgr, err := membership.New(membership.Config{
		Conversation: bob, Self: r("bob"), Members: members,
		Log: bobLog, QuietInterval: 5 * time.Second, SuspicionInterval: 5 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer bobMgr.Close()

	// Background scheduler pump.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				sched.RunAll()
			}
		}
	}()
	defer close(stop)

	// Drain bob's app channel in the background.
	bobDrain := make(chan struct{})
	go func() {
		defer close(bobDrain)
		for range bobMgr.Recv() {
		}
	}()
	defer func() { <-bobDrain }()

	// Bob also advances its watermark in the background so trim
	// can actually progress; otherwise bob's stale watermark
	// blocks trim and we'd be measuring something else.
	bobWMStop := make(chan struct{})
	go func() {
		defer close(bobWMStop)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		var off uint64 = 0
		for {
			select {
			case <-bobWMStop:
				return
			case <-ticker.C:
				off++
				bobMgr.SetWatermark(off)
			}
		}
	}()
	defer func() { <-bobWMStop }()

	b.ResetTimer()
	var wm uint64
	for b.Loop() {
		if _, err := aliceMgr.SendApp([]byte("x")); err != nil {
			b.Fatal(err)
		}
		// Drain alice's own self-delivery so the channel doesn't
		// fill.
		<-aliceMgr.Recv()
		wm++
		aliceMgr.SetWatermark(wm)
	}
}
