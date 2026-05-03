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

package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/mikehelmick/comlink/clock"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestManualNow(t *testing.T) {
	c := clock.NewManual(epoch)
	if got := c.Now(); !got.Equal(epoch) {
		t.Fatalf("initial Now() = %v, want %v", got, epoch)
	}
	c.Advance(5 * time.Second)
	if got := c.Now(); !got.Equal(epoch.Add(5 * time.Second)) {
		t.Fatalf("after advance Now() = %v, want %v", got, epoch.Add(5*time.Second))
	}
}

func TestManualTimerFiresOnAdvance(t *testing.T) {
	c := clock.NewManual(epoch)
	timer := c.NewTimer(100 * time.Millisecond)

	select {
	case <-timer.C():
		t.Fatal("timer fired before Advance")
	default:
	}

	c.Advance(50 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("timer fired before deadline")
	default:
	}

	c.Advance(50 * time.Millisecond)
	select {
	case got := <-timer.C():
		if !got.Equal(epoch.Add(100 * time.Millisecond)) {
			t.Fatalf("timer fired with t=%v, want %v", got, epoch.Add(100*time.Millisecond))
		}
	default:
		t.Fatal("timer did not fire after Advance to deadline")
	}
}

func TestManualTimerExactBoundary(t *testing.T) {
	c := clock.NewManual(epoch)
	timer := c.NewTimer(100 * time.Millisecond)
	c.Advance(100 * time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("timer did not fire at exact deadline")
	}
}

func TestManualMultipleTimersInOrder(t *testing.T) {
	c := clock.NewManual(epoch)
	t1 := c.NewTimer(300 * time.Millisecond)
	t2 := c.NewTimer(100 * time.Millisecond)
	t3 := c.NewTimer(200 * time.Millisecond)

	c.Advance(time.Second)

	for _, ti := range []clock.Timer{t1, t2, t3} {
		select {
		case <-ti.C():
		case <-time.After(time.Second):
			t.Fatal("timer did not fire after sweeping advance")
		}
	}
}

func TestManualStopBeforeFire(t *testing.T) {
	c := clock.NewManual(epoch)
	timer := c.NewTimer(100 * time.Millisecond)
	if !timer.Stop() {
		t.Fatal("Stop returned false on active timer")
	}
	c.Advance(time.Second)
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if timer.Stop() {
		t.Fatal("second Stop returned true")
	}
}

func TestManualReset(t *testing.T) {
	c := clock.NewManual(epoch)
	timer := c.NewTimer(100 * time.Millisecond)
	if !timer.Reset(50 * time.Millisecond) {
		t.Fatal("Reset on active timer returned false")
	}
	c.Advance(50 * time.Millisecond)
	select {
	case <-timer.C():
	default:
		t.Fatal("reset timer did not fire at new deadline")
	}
}

// TestManualConcurrentSafe verifies that concurrent NewTimer calls
// are race-free and that a single Advance fires every registered
// timer. The synchronization barrier ensures all timers are registered
// before Advance runs, so deadlines all fall in the swept interval.
func TestManualConcurrentSafe(t *testing.T) {
	c := clock.NewManual(epoch)
	var wg sync.WaitGroup
	const N = 50
	ready := make(chan struct{}, N)
	for range N {
		wg.Go(func() {
			tm := c.NewTimer(10 * time.Millisecond)
			ready <- struct{}{}
			<-tm.C()
		})
	}
	for range N {
		<-ready
	}
	c.Advance(20 * time.Millisecond)
	wg.Wait()
}

func TestSystemNowProgresses(t *testing.T) {
	c := clock.NewSystem()
	t1 := c.Now()
	time.Sleep(2 * time.Millisecond)
	t2 := c.Now()
	if !t2.After(t1) {
		t.Fatalf("System clock did not advance: %v -> %v", t1, t2)
	}
}

func TestSystemTimerFires(t *testing.T) {
	c := clock.NewSystem()
	timer := c.NewTimer(5 * time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("system timer did not fire")
	}
}

func TestSystemAfter(t *testing.T) {
	c := clock.NewSystem()
	select {
	case <-c.After(5 * time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("system After channel did not fire")
	}
}
