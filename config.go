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
	"log/slog"

	"github.com/mikehelmick/comlink/clock"
	"github.com/mikehelmick/comlink/transport"
)

// ClusterConfig configures a Cluster (PLAN §5). All fields except
// Logger and Clock are required for production use; tests with
// the in-process transport can omit Listen+Sponsors and pass a
// pre-built TransportConfig.Network instead.
type ClusterConfig struct {
	// Self is this replica's stable identity within the cluster.
	// Generated once at first run; persisted by the application
	// and re-supplied on subsequent starts.
	Self ReplicaID

	// Members is the initial cluster membership at first
	// (Bootstrap.Force=true) startup. Subsequent startups load
	// the persisted ML and ignore this field — it's the
	// scenario-X founder's input.
	//
	// Self must appear in Members.
	Members []ReplicaID

	// DataDir is the filesystem root for stable.Storage and
	// log.MessageLog files. Layout:
	//   <DataDir>/cluster_state/      stable.Storage (KV)
	//   <DataDir>/conversations/<id>/ log.MessageLog per conv
	DataDir string `env:"COMLINK_DATA_DIR"`

	// Bootstrap controls ClusterID minting (PLAN §5). Pass
	// non-nil with Force=true on the founder node to create a
	// fresh cluster; nil otherwise to join an existing cluster
	// (the persisted ClusterID is loaded).
	Bootstrap *BootstrapConfig

	// Transport configures how this Cluster communicates with
	// peers. See TransportConfig.
	Transport TransportConfig

	// Logger; defaults to slog.Default().
	Logger *slog.Logger

	// Clock; defaults to clock.NewSystem().
	Clock clock.Clock
}

// TransportConfig configures the network transport for a
// Cluster. Either Network OR Listen must be set; if both are,
// Network wins (the Listen+Sponsors path is ignored).
type TransportConfig struct {
	// Listen is the bind address for this replica's gRPC server,
	// e.g. ":8001" or "0.0.0.0:8001". When set (and Network is
	// nil), the Cluster constructs an insecure gRPC transport.
	// PLAN §5: TLS is intentionally not configured here — we
	// rely on the deployment-layer service mesh (Kubernetes +
	// proxyless gRPC) for transport security.
	Listen string `env:"COMLINK_TRANSPORT_LISTEN"`

	// Sponsors is the bootstrap routing table — a small set of
	// (ReplicaID, addr) entries sufficient to make first contact
	// with the cluster. After bootstrap, full peer routing
	// learned from MemberAdd events is persisted to
	// stable.Storage; subsequent startups can rely on the
	// persisted table and need Sponsors only as a fallback.
	//
	// Wire-form for env: comma-separated "<replica_hex>@host:port"
	// pairs.
	Sponsors []Sponsor `env:"COMLINK_TRANSPORT_SPONSORS"`

	// Network is an escape hatch for tests / custom transports.
	// If non-nil, Listen and Sponsors are ignored.
	Network transport.Network
}

// Sponsor is one entry in TransportConfig.Sponsors — a known
// peer's ID and address used to make first contact with the
// cluster during bootstrap.
type Sponsor struct {
	ID   ReplicaID
	Addr string
}
