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

import "time"

// System is the production [Clock] backed by the standard library
// [time] package.
type System struct{}

// NewSystem returns a [System] clock.
func NewSystem() *System { return &System{} }

// Now returns [time.Now].
func (s *System) Now() time.Time { return time.Now() }

// NewTimer wraps [time.NewTimer].
func (s *System) NewTimer(d time.Duration) Timer { return &systemTimer{t: time.NewTimer(d)} }

// After delegates to [time.After].
func (s *System) After(d time.Duration) <-chan time.Time { return time.After(d) }

type systemTimer struct{ t *time.Timer }

func (st *systemTimer) C() <-chan time.Time     { return st.t.C }
func (st *systemTimer) Stop() bool              { return st.t.Stop() }
func (st *systemTimer) Reset(d time.Duration) bool { return st.t.Reset(d) }
