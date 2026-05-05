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

package psync_test

import (
	"slices"
	"testing"

	"github.com/mikehelmick/comlink/psync"
)

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b psync.Vector
		want bool
	}{
		{psync.Vector{0, 0, 0}, psync.Vector{0, 0, 0}, true},
		{psync.Vector{1, 2, 3}, psync.Vector{1, 2, 3}, true},
		{psync.Vector{1, 2, 3}, psync.Vector{1, 2, 4}, false},
		{psync.Vector{}, psync.Vector{}, true},
	}
	for _, tc := range cases {
		if got := psync.Equal(tc.a, tc.b); got != tc.want {
			t.Errorf("Equal(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestDominates(t *testing.T) {
	cases := []struct {
		name string
		a, b psync.Vector
		want bool
	}{
		{"equal does not dominate", psync.Vector{1, 1}, psync.Vector{1, 1}, false},
		{"strictly greater", psync.Vector{2, 2}, psync.Vector{1, 1}, true},
		{"a >= b with one strict", psync.Vector{1, 2}, psync.Vector{1, 1}, true},
		{"a < b in one slot", psync.Vector{1, 0}, psync.Vector{0, 1}, false},
		{"a >= in some, < in others", psync.Vector{2, 0, 3}, psync.Vector{1, 1, 2}, false},
		{"zero vs non-zero", psync.Vector{0, 0}, psync.Vector{1, 1}, false},
	}
	for _, tc := range cases {
		if got := psync.Dominates(tc.a, tc.b); got != tc.want {
			t.Errorf("[%s] Dominates(%v, %v) = %v, want %v", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}

func TestHappensBefore(t *testing.T) {
	a := psync.Vector{1, 2}
	b := psync.Vector{2, 3}
	if !psync.HappensBefore(a, b) {
		t.Errorf("HappensBefore(%v, %v) = false, want true", a, b)
	}
	if psync.HappensBefore(b, a) {
		t.Errorf("HappensBefore(%v, %v) = true, want false", b, a)
	}
}

func TestConcurrent(t *testing.T) {
	cases := []struct {
		name string
		a, b psync.Vector
		want bool
	}{
		{"obviously concurrent", psync.Vector{1, 0}, psync.Vector{0, 1}, true},
		{"one dominates", psync.Vector{2, 2}, psync.Vector{1, 1}, false},
		{"equal is not concurrent (same message)", psync.Vector{1, 1}, psync.Vector{1, 1}, false},
		{"partial concurrent across 3 slots", psync.Vector{2, 1, 0}, psync.Vector{1, 0, 2}, true},
	}
	for _, tc := range cases {
		if got := psync.Concurrent(tc.a, tc.b); got != tc.want {
			t.Errorf("[%s] Concurrent(%v, %v) = %v, want %v", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}

func TestMax(t *testing.T) {
	got := psync.Max(psync.Vector{3, 1, 5}, psync.Vector{2, 4, 5})
	want := psync.Vector{3, 4, 5}
	if !slices.Equal(got, want) {
		t.Errorf("Max = %v, want %v", got, want)
	}
}

func TestIncrement(t *testing.T) {
	v := psync.Vector{0, 0, 0}
	got := psync.Increment(v, 1)
	want := psync.Vector{0, 1, 0}
	if !slices.Equal(got, want) {
		t.Errorf("Increment(%v, 1) = %v, want %v", v, got, want)
	}
	// Original unmodified.
	if !slices.Equal(v, psync.Vector{0, 0, 0}) {
		t.Errorf("Increment mutated original: %v", v)
	}
}

func TestIncrementOutOfRangePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Increment with out-of-range slot did not panic")
		}
	}()
	psync.Increment(psync.Vector{0, 0}, 5)
}

func TestClone(t *testing.T) {
	v := psync.Vector{1, 2, 3}
	c := psync.Clone(v)
	c[0] = 99
	if v[0] != 1 {
		t.Fatalf("Clone shares storage with original: %v", v)
	}
}

// TestVectorOpsAcceptDifferentLengths exercises PLAN §2.10.1
// lazy zero-padding: comparison helpers accept vectors of
// different lengths and treat the shorter as if extended with
// zeros at the end (an "old-era" message that predates a
// MemberAdd). Old behavior was to panic; new behavior matches
// insertion-order's append-only invariant.
func TestVectorOpsAcceptDifferentLengths(t *testing.T) {
	cases := []struct {
		name      string
		shorter   psync.Vector
		longer    psync.Vector
		equal     bool
		shortDoms bool // Dominates(shorter, longer)?
		longDoms  bool // Dominates(longer, shorter)?
		conc      bool
	}{
		{
			"shorter zero-padded equals longer",
			psync.Vector{1, 2},
			psync.Vector{1, 2, 0},
			true, false, false, false,
		},
		{
			"longer dominates with later-slot value",
			psync.Vector{1, 2},
			psync.Vector{1, 2, 5},
			false, false, true, false,
		},
		{
			"longer happens-before-or-equal in seen slots, shorter has higher in early slot",
			psync.Vector{2, 3},
			psync.Vector{1, 3, 0},
			false, true, false, false,
		},
		{
			"different in seen slot AND new slot used",
			psync.Vector{2, 3},
			psync.Vector{1, 3, 5},
			false, false, false, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := psync.Equal(tc.shorter, tc.longer); got != tc.equal {
				t.Errorf("Equal = %v, want %v", got, tc.equal)
			}
			if got := psync.Dominates(tc.shorter, tc.longer); got != tc.shortDoms {
				t.Errorf("Dominates(shorter, longer) = %v, want %v", got, tc.shortDoms)
			}
			if got := psync.Dominates(tc.longer, tc.shorter); got != tc.longDoms {
				t.Errorf("Dominates(longer, shorter) = %v, want %v", got, tc.longDoms)
			}
			if got := psync.Concurrent(tc.shorter, tc.longer); got != tc.conc {
				t.Errorf("Concurrent = %v, want %v", got, tc.conc)
			}
		})
	}
}

// TestVectorMaxAcceptsDifferentLengths confirms Max returns a
// vector of the longer length, lazy-padding the shorter side.
func TestVectorMaxAcceptsDifferentLengths(t *testing.T) {
	got := psync.Max(psync.Vector{3, 1}, psync.Vector{2, 4, 5})
	want := psync.Vector{3, 4, 5}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Max[%d] = %d, want %d", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("Max length = %d, want %d", len(got), len(want))
	}
}
