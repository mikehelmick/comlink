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

package stable

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// File is a Storage backed by one file per key in a directory. Put
// uses write-temp + fsync + rename + dir-fsync for crash-safe atomic
// updates.
type File struct {
	mu     sync.Mutex
	dir    string
	closed bool
}

// NewFile opens (creating if necessary) a File storage rooted at dir.
func NewFile(dir string) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &File{dir: dir}, nil
}

func (f *File) path(key string) string {
	return filepath.Join(f.dir, key)
}

// Get reads the file for key.
func (f *File) Get(_ context.Context, key string) ([]byte, error) {
	if !ValidKey(key) {
		return nil, ErrInvalidKey
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, errClosed
	}
	data, err := os.ReadFile(f.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

// Put atomically and durably stores value under key.
func (f *File) Put(_ context.Context, key string, value []byte) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errClosed
	}

	final := f.path(key)
	tmp, err := os.CreateTemp(f.dir, key+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(value); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return err
	}
	// fsync the directory so the rename is durable.
	dir, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Delete removes the file for key.
func (f *File) Delete(_ context.Context, key string) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errClosed
	}
	err := os.Remove(f.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	// fsync the directory so the unlink is durable.
	dir, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// Close marks the storage closed. Files remain on disk; reopen with
// NewFile on the same directory to access them again.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}
