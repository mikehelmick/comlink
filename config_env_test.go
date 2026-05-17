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

package comlink_test

import (
	"context"
	"testing"

	"github.com/mikehelmick/comlink"
)

// TestLoadConfigFromEnvAll exercises every documented env var
// path through LoadConfigFromEnv. Uses t.Setenv so the test
// stays hermetic.
func TestLoadConfigFromEnvAll(t *testing.T) {
	aliceHex := id16("alice").String()
	bobHex := id16("bob").String()
	clusterID := "0102030405060708090a0b0c0d0e0f10"

	t.Setenv("COMLINK_SELF", aliceHex)
	t.Setenv("COMLINK_MEMBERS", aliceHex+","+bobHex)
	t.Setenv("COMLINK_DATA_DIR", "/var/lib/comlink/alice")
	t.Setenv("COMLINK_BOOTSTRAP_FORCE", "true")
	t.Setenv("COMLINK_BOOTSTRAP_CLUSTER_ID", clusterID)
	t.Setenv("COMLINK_TRANSPORT_LISTEN", "0.0.0.0:7000")
	t.Setenv("COMLINK_TRANSPORT_SPONSORS", bobHex+"@bob.example:7000")

	cfg, err := comlink.LoadConfigFromEnv(context.Background())
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}

	if !cfg.Self.Equal(id16("alice")) {
		t.Errorf("Self = %s, want alice", cfg.Self)
	}
	if len(cfg.Members) != 2 ||
		!cfg.Members[0].Equal(id16("alice")) ||
		!cfg.Members[1].Equal(id16("bob")) {
		t.Errorf("Members = %v, want [alice, bob]", cfg.Members)
	}
	if cfg.DataDir != "/var/lib/comlink/alice" {
		t.Errorf("DataDir = %q", cfg.DataDir)
	}
	if cfg.Bootstrap == nil {
		t.Fatal("Bootstrap is nil; expected allocated by env vars")
	}
	if !cfg.Bootstrap.Force {
		t.Errorf("Bootstrap.Force = false, want true")
	}
	if cfg.Bootstrap.ClusterID.String() != clusterID {
		t.Errorf("Bootstrap.ClusterID = %s, want %s",
			cfg.Bootstrap.ClusterID, clusterID)
	}
	if cfg.Transport.Listen != "0.0.0.0:7000" {
		t.Errorf("Transport.Listen = %q", cfg.Transport.Listen)
	}
	if len(cfg.Transport.Sponsors) != 1 {
		t.Fatalf("Transport.Sponsors len = %d, want 1", len(cfg.Transport.Sponsors))
	}
	sp := cfg.Transport.Sponsors[0]
	if !sp.ID.Equal(id16("bob")) {
		t.Errorf("Sponsor.ID = %s, want bob", sp.ID)
	}
	if sp.Addr != "bob.example:7000" {
		t.Errorf("Sponsor.Addr = %q, want bob.example:7000", sp.Addr)
	}
}

// TestLoadConfigFromEnvEmpty: empty env yields a zero config
// without error — apps can layer overrides on top. envconfig
// may allocate the Bootstrap pointer to a zero-value struct
// regardless; cluster.go treats that as equivalent to nil
// (Force defaults to false).
func TestLoadConfigFromEnvEmpty(t *testing.T) {
	cfg, err := comlink.LoadConfigFromEnv(context.Background())
	if err != nil {
		t.Fatalf("LoadConfigFromEnv: %v", err)
	}
	if len(cfg.Self) != 0 {
		t.Errorf("Self = %v, want empty", cfg.Self)
	}
	if cfg.Bootstrap != nil && cfg.Bootstrap.Force {
		t.Errorf("Bootstrap.Force = true, want false in empty env")
	}
	if cfg.DataDir != "" {
		t.Errorf("DataDir = %q, want empty", cfg.DataDir)
	}
	if cfg.Transport.Listen != "" {
		t.Errorf("Transport.Listen = %q, want empty", cfg.Transport.Listen)
	}
}

// TestSponsorEnvDecodeRejectsBadFormat: malformed Sponsor strings
// surface a meaningful error rather than silently producing
// garbage entries.
func TestSponsorEnvDecodeRejectsBadFormat(t *testing.T) {
	bad := []string{
		"no-at-sign",
		"@only-addr",
		"abcd@",
		"not-hex@host:1",
	}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			var sp comlink.Sponsor
			if err := sp.EnvDecode(s); err == nil {
				t.Fatalf("Sponsor.EnvDecode(%q) = nil error, want failure", s)
			}
		})
	}
}
