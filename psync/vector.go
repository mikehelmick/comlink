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

package psync

import (
	"fmt"
	"strings"
)

// Vector is a fixed-length vector clock indexed by sorted-membership
// slot. Slot semantics: vector[i] is the highest sequence number
// from the i-th participant (sorted by ReplicaID byte order) that
// the bearer message causally depends on; the slot belonging to the
// sender carries the sender's own monotonic seq.
//
// All Vector helpers in this file require operands of the same
// length; mismatched lengths indicate a membership-shape disagreement
// that the caller must handle separately (PLAN §2.10.1: a receiver
// with a shorter view defers and catches up via the lost-message
// protocol). The helpers panic on length mismatch so the bug surfaces
// loudly rather than producing silently-wrong causality answers.
type Vector []uint64

// String renders the vector compactly for logs and errors.
func (v Vector) String() string {
	if len(v) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d", x)
	}
	b.WriteByte(']')
	return b.String()
}

// Equal reports whether a and b are component-wise equal.
func Equal(a, b Vector) bool {
	mustSameLen(a, b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Dominates reports whether a strictly dominates b: a[i] >= b[i]
// for all i AND there is at least one i where a[i] > b[i].
//
// Causal interpretation: Dominates(a, b) means the message bearing
// a strictly causally follows the one bearing b — every message b
// depends on is also a dependency of a, plus at least one more.
func Dominates(a, b Vector) bool {
	mustSameLen(a, b)
	any := false
	for i := range a {
		if a[i] < b[i] {
			return false
		}
		if a[i] > b[i] {
			any = true
		}
	}
	return any
}

// HappensBefore reports whether a happens-before b in the causal
// order: equivalent to Dominates(b, a).
func HappensBefore(a, b Vector) bool {
	return Dominates(b, a)
}

// Concurrent reports whether a and b are causally independent:
// neither dominates the other. Equivalent to neither happens-before
// the other.
func Concurrent(a, b Vector) bool {
	mustSameLen(a, b)
	return !Dominates(a, b) && !Dominates(b, a) && !Equal(a, b)
}

// Max returns a freshly-allocated vector whose slots are the
// component-wise maximum of a and b. Used by the receiver to advance
// its own causal view after accepting an incoming message.
func Max(a, b Vector) Vector {
	mustSameLen(a, b)
	out := make(Vector, len(a))
	for i := range a {
		out[i] = max(a[i], b[i])
	}
	return out
}

// Increment returns a new vector that is a copy of v with slot
// incremented by 1. Used by the sender to advance its own slot
// when emitting a new message.
func Increment(v Vector, slot int) Vector {
	if slot < 0 || slot >= len(v) {
		panic(fmt.Sprintf("psync: Increment slot %d out of range [0,%d)", slot, len(v)))
	}
	out := make(Vector, len(v))
	copy(out, v)
	out[slot]++
	return out
}

// Clone returns an independent copy of v.
func Clone(v Vector) Vector {
	out := make(Vector, len(v))
	copy(out, v)
	return out
}

func mustSameLen(a, b Vector) {
	if len(a) != len(b) {
		panic(fmt.Sprintf("psync: vector length mismatch %d vs %d — caller must reconcile membership shape first (PLAN §2.10.1)", len(a), len(b)))
	}
}
