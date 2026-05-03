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
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/mikehelmick/comlink/psync"
	"github.com/mikehelmick/comlink/stable"
)

func TestMaskEmptyOnFirstLoad(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	m, err := psync.LoadMask(ctx, s, psync.MaskStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.MaskedReplicas(); len(got) != 0 {
		t.Errorf("freshly-loaded mask = %v, want empty", got)
	}
}

func TestMaskoutThenIsMasked(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	m, _ := psync.LoadMask(ctx, s, psync.MaskStorageKey)
	bob := r("bob")
	if m.IsMasked(bob) {
		t.Fatal("bob unexpectedly masked before Maskout")
	}
	if err := m.Maskout(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if !m.IsMasked(bob) {
		t.Fatal("bob not masked after Maskout")
	}
}

func TestMaskinReverses(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	m, _ := psync.LoadMask(ctx, s, psync.MaskStorageKey)
	bob := r("bob")
	_ = m.Maskout(ctx, bob)
	if err := m.Maskin(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if m.IsMasked(bob) {
		t.Fatal("bob still masked after Maskin")
	}
}

func TestMaskoutIdempotent(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	m, _ := psync.LoadMask(ctx, s, psync.MaskStorageKey)
	bob := r("bob")
	if err := m.Maskout(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if err := m.Maskout(ctx, bob); err != nil {
		t.Fatal("second Maskout returned err: " + err.Error())
	}
}

func TestMaskPersistsAcrossReload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s1, err := stable.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, err := psync.LoadMask(ctx, s1, psync.MaskStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Maskout(ctx, r("bob")); err != nil {
		t.Fatal(err)
	}
	if err := m.Maskout(ctx, r("carol")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate process restart.
	s2, err := stable.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m2, err := psync.LoadMask(ctx, s2, psync.MaskStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	if !m2.IsMasked(r("bob")) {
		t.Errorf("bob's mask did not persist across restart")
	}
	if !m2.IsMasked(r("carol")) {
		t.Errorf("carol's mask did not persist across restart")
	}
	if m2.IsMasked(r("alice")) {
		t.Errorf("alice unexpectedly masked after restart")
	}

	// MaskedReplicas should be sorted.
	got := m2.MaskedReplicas()
	if !slices.IsSortedFunc(got, bytes.Compare) {
		t.Errorf("MaskedReplicas not sorted: %v", got)
	}
}
