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
	"fmt"

	"github.com/mikehelmick/comlink/stable"
)

// stableKeyClusterID is the stable.Storage key under which the
// local ClusterID is persisted.
const stableKeyClusterID = "comlink.cluster_id"

// BootstrapConfig controls how a Cluster handles its ClusterID
// at construction. PLAN §5: minting a new ClusterID is an
// explicit, opt-in action. Default behavior assumes "join an
// existing cluster" and reads the ClusterID from stable.Storage
// (or, in the future, learns it from sponsors during the
// gRPC handshake).
type BootstrapConfig struct {
	// Force, when true, mints a fresh ClusterID and persists it.
	// This is REQUIRED to create a new cluster — without it, a
	// node with no persisted ClusterID will refuse to start.
	// Operator-error guard against accidental cluster
	// fragmentation.
	//
	// If Force is true AND a ClusterID is already persisted, the
	// behavior depends on AllowOverride: by default we refuse
	// (preventing accidental cluster reset).
	Force bool `env:"FORCE"`

	// AllowOverride, when true, allows Force to overwrite an
	// existing persisted ClusterID. Use with extreme caution —
	// this destroys cluster identity and any subsequent peer
	// communication will fail the ClusterID handshake.
	AllowOverride bool `env:"ALLOW_OVERRIDE"`

	// ClusterID, when set together with Force, installs that
	// specific ClusterID rather than minting a fresh one. Used
	// by joiners that have learned the cluster's ID via a
	// sponsor handshake (Phase 5(i)), and by tests that need
	// multiple Cluster instances on the same logical cluster.
	// Must be exactly 16 bytes if non-nil.
	ClusterID ClusterID `env:"CLUSTER_ID"`
}

// Errors returned by bootstrap operations.
var (
	// ErrBootstrapRequired is returned when a Cluster is
	// constructed against an empty stable.Storage without
	// Bootstrap.Force=true. The operator must explicitly opt in
	// to creating a fresh cluster.
	ErrBootstrapRequired = errors.New("comlink: no persisted ClusterID and Bootstrap.Force is not set")

	// ErrBootstrapWouldOverride is returned when Bootstrap.Force
	// is true but a ClusterID is already persisted and
	// AllowOverride is false. Prevents accidental cluster
	// reset.
	ErrBootstrapWouldOverride = errors.New("comlink: refusing to override existing persisted ClusterID; set Bootstrap.AllowOverride to allow")
)

// loadOrCreateClusterID applies the bootstrap discipline against
// storage:
//
//   - Persisted ClusterID present + !Force: return persisted.
//   - Persisted ClusterID present + Force + !AllowOverride: error.
//   - Persisted ClusterID present + Force + AllowOverride:
//     mint fresh, persist (overrides existing).
//   - No persisted ClusterID + Force: mint fresh, persist.
//   - No persisted ClusterID + !Force: error.
func loadOrCreateClusterID(ctx context.Context, storage stable.Storage, b BootstrapConfig) (ClusterID, error) {
	existing, err := loadClusterID(ctx, storage)
	if err != nil && !errors.Is(err, stable.ErrNotFound) {
		return nil, fmt.Errorf("comlink: load persisted ClusterID: %w", err)
	}
	hasExisting := !errors.Is(err, stable.ErrNotFound)

	if b.ClusterID != nil && len(b.ClusterID) != idLen {
		return nil, fmt.Errorf("%w: BootstrapConfig.ClusterID wrong length %d", ErrInvalidID, len(b.ClusterID))
	}

	switch {
	case hasExisting && !b.Force:
		return existing, nil

	case hasExisting && b.Force && !b.AllowOverride:
		return nil, ErrBootstrapWouldOverride

	case hasExisting && b.Force && b.AllowOverride:
		fresh, err := chooseClusterID(b)
		if err != nil {
			return nil, err
		}
		if err := persistClusterID(ctx, storage, fresh); err != nil {
			return nil, fmt.Errorf("comlink: persist ClusterID: %w", err)
		}
		return fresh, nil

	case !hasExisting && b.Force:
		fresh, err := chooseClusterID(b)
		if err != nil {
			return nil, err
		}
		if err := persistClusterID(ctx, storage, fresh); err != nil {
			return nil, fmt.Errorf("comlink: persist ClusterID: %w", err)
		}
		return fresh, nil

	default: // !hasExisting && !b.Force
		return nil, ErrBootstrapRequired
	}
}

// chooseClusterID returns BootstrapConfig.ClusterID if set
// (joiner / test path) or mints a fresh one (founder path).
func chooseClusterID(b BootstrapConfig) (ClusterID, error) {
	if b.ClusterID != nil {
		out := make(ClusterID, idLen)
		copy(out, b.ClusterID)
		return out, nil
	}
	fresh, err := NewClusterID()
	if err != nil {
		return nil, fmt.Errorf("comlink: generate ClusterID: %w", err)
	}
	return fresh, nil
}

func loadClusterID(ctx context.Context, storage stable.Storage) (ClusterID, error) {
	bs, err := storage.Get(ctx, stableKeyClusterID)
	if err != nil {
		return nil, err
	}
	if len(bs) != idLen {
		return nil, fmt.Errorf("%w: persisted ClusterID has wrong length %d", ErrInvalidID, len(bs))
	}
	return ClusterID(bs), nil
}

func persistClusterID(ctx context.Context, storage stable.Storage, id ClusterID) error {
	return storage.Put(ctx, stableKeyClusterID, []byte(id))
}
