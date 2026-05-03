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

package stable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mikehelmick/comlink/stable"
)

// runConformance exercises the Storage contract. Both Memory and
// File must pass this suite.
func runConformance(t *testing.T, factory func(t *testing.T) stable.Storage) {
	t.Helper()
	ctx := context.Background()

	t.Run("missing_key_returns_not_found", func(t *testing.T) {
		s := factory(t)
		_, err := s.Get(ctx, "absent")
		if !errors.Is(err, stable.ErrNotFound) {
			t.Fatalf("Get on missing key: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("put_then_get_roundtrip", func(t *testing.T) {
		s := factory(t)
		want := []byte("hello-stable")
		if err := s.Put(ctx, "k1", want); err != nil {
			t.Fatalf("Put: %v", err)
		}
		got, err := s.Get(ctx, "k1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get returned %q, want %q", got, want)
		}
	})

	t.Run("put_overwrites", func(t *testing.T) {
		s := factory(t)
		if err := s.Put(ctx, "k", []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(ctx, "k", []byte("v2")); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte("v2")) {
			t.Fatalf("after overwrite Get = %q, want %q", got, "v2")
		}
	})

	t.Run("delete_removes_key", func(t *testing.T) {
		s := factory(t)
		if err := s.Put(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err := s.Get(ctx, "k")
		if !errors.Is(err, stable.ErrNotFound) {
			t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("delete_missing_returns_not_found", func(t *testing.T) {
		s := factory(t)
		err := s.Delete(ctx, "absent")
		if !errors.Is(err, stable.ErrNotFound) {
			t.Fatalf("Delete on missing key: err = %v, want ErrNotFound", err)
		}
	})

	t.Run("invalid_key_rejected", func(t *testing.T) {
		s := factory(t)
		bad := []string{"", "has/slash", "has space", "a:b", string(make([]byte, 300))}
		for _, k := range bad {
			if err := s.Put(ctx, k, []byte("v")); !errors.Is(err, stable.ErrInvalidKey) {
				t.Errorf("Put(%q) err = %v, want ErrInvalidKey", k, err)
			}
			if _, err := s.Get(ctx, k); !errors.Is(err, stable.ErrInvalidKey) {
				t.Errorf("Get(%q) err = %v, want ErrInvalidKey", k, err)
			}
			if err := s.Delete(ctx, k); !errors.Is(err, stable.ErrInvalidKey) {
				t.Errorf("Delete(%q) err = %v, want ErrInvalidKey", k, err)
			}
		}
	})

	t.Run("concurrent_writers_safe", func(t *testing.T) {
		s := factory(t)
		var wg sync.WaitGroup
		const N = 25
		for i := range N {
			wg.Go(func() {
				key := fmt.Sprintf("k%d", i)
				val := fmt.Appendf(nil, "v%d", i)
				if err := s.Put(ctx, key, val); err != nil {
					t.Errorf("Put(%s): %v", key, err)
				}
			})
		}
		wg.Wait()
		for i := range N {
			key := fmt.Sprintf("k%d", i)
			want := fmt.Appendf(nil, "v%d", i)
			got, err := s.Get(ctx, key)
			if err != nil {
				t.Errorf("Get(%s): %v", key, err)
				continue
			}
			if !bytes.Equal(got, want) {
				t.Errorf("Get(%s) = %q, want %q", key, got, want)
			}
		}
	})

	t.Run("returned_value_isolated_from_internal_state", func(t *testing.T) {
		// Mutating the bytes returned from Get must not affect the
		// stored value, and mutating the bytes passed to Put must
		// not either.
		s := factory(t)
		val := []byte("immutable")
		if err := s.Put(ctx, "k", val); err != nil {
			t.Fatal(err)
		}
		val[0] = 'X' // try to corrupt the stored copy via the input slice
		got, err := s.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte("immutable")) {
			t.Fatalf("Put did not isolate input: stored %q, want %q", got, "immutable")
		}
		got[0] = 'Y' // try to corrupt via the output slice
		got2, err := s.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got2, []byte("immutable")) {
			t.Fatalf("Get did not isolate output: stored %q, want %q", got2, "immutable")
		}
	})
}

func TestMemoryConformance(t *testing.T) {
	runConformance(t, func(t *testing.T) stable.Storage {
		s := stable.NewMemory()
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestFileConformance(t *testing.T) {
	runConformance(t, func(t *testing.T) stable.Storage {
		dir := t.TempDir()
		s, err := stable.NewFile(dir)
		if err != nil {
			t.Fatalf("NewFile: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

func TestFilePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s1, err := stable.NewFile(dir)
	if err != nil {
		t.Fatalf("NewFile: %v", err)
	}
	if err := s1.Put(ctx, "persistent", []byte("survives")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := stable.NewFile(dir)
	if err != nil {
		t.Fatalf("reopen NewFile: %v", err)
	}
	defer s2.Close()
	got, err := s2.Get(ctx, "persistent")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("survives")) {
		t.Fatalf("Get after reopen = %q, want %q", got, "survives")
	}
}
