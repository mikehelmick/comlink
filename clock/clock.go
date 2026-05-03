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

// Package clock abstracts time so that comlink protocols can be tested
// deterministically. Production code uses [System]; tests use [Manual]
// and step time forward via [Manual.Advance].
//
// Callers must take a [Clock] as a dependency and never call
// [time.Now], [time.NewTimer], or [time.After] directly. This is the
// rule that makes the algorithms in PLAN.md testable in the in-memory
// transport's deterministic scheduler.
package clock

import "time"

// Clock is the abstract source of time and timers used throughout
// comlink.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer creates a single-shot timer that fires once after d.
	NewTimer(d time.Duration) Timer
	// After is a convenience for NewTimer(d).C().
	After(d time.Duration) <-chan time.Time
}

// Timer is a single-shot timer. The semantics mirror [time.Timer]
// closely enough that switching from real time to [Manual] should not
// require behavioral changes in callers.
type Timer interface {
	// C returns the channel on which the firing time is delivered.
	// Buffered with capacity 1.
	C() <-chan time.Time
	// Stop prevents the timer from firing. Returns true if the call
	// stops the timer; false if it has already fired or been stopped.
	Stop() bool
	// Reset reschedules the timer to fire after d. Follows the same
	// best-effort semantics as [time.Timer.Reset]: callers should
	// drain C before calling Reset on a timer that may have fired.
	Reset(d time.Duration) bool
}
