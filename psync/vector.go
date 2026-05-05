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

// Vector is a vector clock indexed by insertion-order membership
// slot (PLAN §2.10.1). Slot semantics: vector[i] is the highest
// sequence number from the i-th participant (in the order they
// were added to the conversation) that the bearer message
// causally depends on; the slot belonging to the sender carries
// the sender's own monotonic seq.
//
// Length tolerance: comparison helpers in this file accept
// vectors of different lengths and treat the shorter one as if
// padded with zeros at the end (lazy padding). This is correct
// because new slots always append: a shorter vector is just a
// prefix of the new shape (an "old-era" message that predates a
// MemberAdd). The conversation layer is still responsible for
// detecting that an INCOMING longer vector is from a future era
// and catching up via the lost-message protocol before applying.
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

// at returns v[i] if i is in range, else 0 (lazy padding).
func at(v Vector, i int) uint64 {
	if i < len(v) {
		return v[i]
	}
	return 0
}

// Equal reports whether a and b are component-wise equal under
// lazy zero-padding (a shorter vector is treated as if extended
// with zeros).
func Equal(a, b Vector) bool {
	n := max(len(a), len(b))
	for i := 0; i < n; i++ {
		if at(a, i) != at(b, i) {
			return false
		}
	}
	return true
}

// Dominates reports whether a strictly dominates b: a[i] >= b[i]
// for all i AND there is at least one i where a[i] > b[i] (lazy
// padding applies to both sides).
//
// Causal interpretation: Dominates(a, b) means the message bearing
// a strictly causally follows the one bearing b — every message b
// depends on is also a dependency of a, plus at least one more.
func Dominates(a, b Vector) bool {
	n := max(len(a), len(b))
	any := false
	for i := 0; i < n; i++ {
		ai, bi := at(a, i), at(b, i)
		if ai < bi {
			return false
		}
		if ai > bi {
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
// neither dominates the other and they are not equal.
func Concurrent(a, b Vector) bool {
	return !Dominates(a, b) && !Dominates(b, a) && !Equal(a, b)
}

// Max returns a freshly-allocated vector whose slots are the
// component-wise maximum of a and b (lazy padding). The result
// has length max(len(a), len(b)).
func Max(a, b Vector) Vector {
	n := max(len(a), len(b))
	out := make(Vector, n)
	for i := 0; i < n; i++ {
		out[i] = max(at(a, i), at(b, i))
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
