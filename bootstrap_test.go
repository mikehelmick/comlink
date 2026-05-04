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

package comlink

import (
	"context"
	"errors"
	"testing"

	"github.com/mikehelmick/comlink/stable"
)

// These tests live in the internal `comlink` package (not _test)
// because loadOrCreateClusterID is unexported.

func TestLoadOrCreate_FreshNoForce_Errors(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	_, err := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: false})
	if !errors.Is(err, ErrBootstrapRequired) {
		t.Fatalf("err = %v, want ErrBootstrapRequired", err)
	}
}

func TestLoadOrCreate_FreshWithForce_Mints(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	id, err := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != idLen {
		t.Fatalf("minted ClusterID len = %d, want %d", len(id), idLen)
	}
	// Persisted: subsequent load (without Force) should return the same id.
	again, err := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: false})
	if err != nil {
		t.Fatal(err)
	}
	if !id.Equal(again) {
		t.Fatalf("persisted ClusterID = %v, want %v", again, id)
	}
}

func TestLoadOrCreate_ExistingForceWithoutOverride_Errors(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	first, _ := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: true})
	_ = first

	_, err := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: true, AllowOverride: false})
	if !errors.Is(err, ErrBootstrapWouldOverride) {
		t.Fatalf("err = %v, want ErrBootstrapWouldOverride", err)
	}
}

func TestLoadOrCreate_ExistingForceAllowOverride_Mints(t *testing.T) {
	ctx := context.Background()
	s := stable.NewMemory()
	defer s.Close()
	first, _ := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: true})

	second, err := loadOrCreateClusterID(ctx, s, BootstrapConfig{Force: true, AllowOverride: true})
	if err != nil {
		t.Fatal(err)
	}
	if first.Equal(second) {
		t.Fatal("AllowOverride did not mint a fresh ClusterID")
	}
}

func TestLoadOrCreate_PersistsAcrossReload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s1, err := stable.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := loadOrCreateClusterID(ctx, s1, BootstrapConfig{Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := stable.NewFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	second, err := loadOrCreateClusterID(ctx, s2, BootstrapConfig{Force: false})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Equal(second) {
		t.Fatalf("ClusterID did not survive restart: %v -> %v", first, second)
	}
}
