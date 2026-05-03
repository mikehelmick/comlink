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

func TestVectorOpsLengthMismatchPanics(t *testing.T) {
	tests := []struct {
		name string
		op   func()
	}{
		{"Equal", func() { psync.Equal(psync.Vector{1}, psync.Vector{1, 2}) }},
		{"Dominates", func() { psync.Dominates(psync.Vector{1}, psync.Vector{1, 2}) }},
		{"Concurrent", func() { psync.Concurrent(psync.Vector{1}, psync.Vector{1, 2}) }},
		{"Max", func() { psync.Max(psync.Vector{1}, psync.Vector{1, 2}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic on length mismatch")
				}
			}()
			tt.op()
		})
	}
}
