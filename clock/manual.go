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

package clock

import (
	"sort"
	"sync"
	"time"
)

// Manual is a [Clock] whose flow of time is controlled by the test via
// [Manual.Advance] or [Manual.Set]. Real wall-clock time is ignored.
//
// All methods are safe for concurrent use.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

// NewManual returns a Manual clock starting at start.
func NewManual(start time.Time) *Manual {
	return &Manual{now: start}
}

// Now returns the current logical time.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// NewTimer creates a timer that fires when the clock has advanced by d
// from "now."
func (m *Manual) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := &manualTimer{
		clock:  m,
		fireAt: m.now.Add(d),
		ch:     make(chan time.Time, 1),
		active: true,
	}
	m.timers = append(m.timers, t)
	return t
}

// After is a convenience for NewTimer(d).C().
func (m *Manual) After(d time.Duration) <-chan time.Time {
	return m.NewTimer(d).C()
}

// Advance moves the clock forward by d, firing any timers whose
// deadlines fall in the new interval. Timers fire in deadline order.
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	target := m.now.Add(d)
	due := m.collectDueLocked(target)
	m.now = target
	m.mu.Unlock()
	for _, t := range due {
		t.fire(target)
	}
}

// Set jumps the clock to t. Like Advance, fires timers with deadlines
// in (oldNow, t]. Setting the clock backward is allowed but does not
// rewind any already-fired timers.
func (m *Manual) Set(t time.Time) {
	m.mu.Lock()
	if !t.After(m.now) {
		m.now = t
		m.mu.Unlock()
		return
	}
	due := m.collectDueLocked(t)
	m.now = t
	m.mu.Unlock()
	for _, ti := range due {
		ti.fire(t)
	}
}

// collectDueLocked returns the timers that should fire at or before
// target, ordered by deadline. Caller must hold m.mu.
func (m *Manual) collectDueLocked(target time.Time) []*manualTimer {
	var due []*manualTimer
	kept := m.timers[:0]
	for _, t := range m.timers {
		if t.active && !t.fireAt.After(target) {
			t.active = false
			due = append(due, t)
		} else {
			kept = append(kept, t)
		}
	}
	m.timers = kept
	sort.Slice(due, func(i, j int) bool { return due[i].fireAt.Before(due[j].fireAt) })
	return due
}

// removeLocked drops a stopped timer from the pending list. Caller
// must hold m.mu.
func (m *Manual) removeLocked(target *manualTimer) {
	kept := m.timers[:0]
	for _, t := range m.timers {
		if t != target {
			kept = append(kept, t)
		}
	}
	m.timers = kept
}

type manualTimer struct {
	clock  *Manual
	fireAt time.Time
	ch     chan time.Time
	active bool // protected by clock.mu
}

func (mt *manualTimer) C() <-chan time.Time { return mt.ch }

func (mt *manualTimer) Stop() bool {
	mt.clock.mu.Lock()
	defer mt.clock.mu.Unlock()
	if !mt.active {
		return false
	}
	mt.active = false
	mt.clock.removeLocked(mt)
	return true
}

func (mt *manualTimer) Reset(d time.Duration) bool {
	mt.clock.mu.Lock()
	wasActive := mt.active
	if mt.active {
		mt.clock.removeLocked(mt)
	}
	mt.fireAt = mt.clock.now.Add(d)
	mt.active = true
	mt.clock.timers = append(mt.clock.timers, mt)
	mt.clock.mu.Unlock()
	return wasActive
}

// fire delivers the firing time on the timer channel. Non-blocking:
// if the receiver hasn't drained the previous fire (only possible
// after Reset on a timer whose previous firing wasn't consumed), the
// new firing is dropped to match [time.Timer] semantics.
func (mt *manualTimer) fire(t time.Time) {
	select {
	case mt.ch <- t:
	default:
	}
}
