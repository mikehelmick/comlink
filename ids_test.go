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
	"errors"
	"strings"
	"testing"

	"github.com/mikehelmick/comlink"
)

func TestNewClusterIDIsRandom(t *testing.T) {
	a, err := comlink.NewClusterID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := comlink.NewClusterID()
	if err != nil {
		t.Fatal(err)
	}
	if a.Equal(b) {
		t.Fatal("two NewClusterID calls returned the same id")
	}
	if len(a) != 16 || len(b) != 16 {
		t.Fatalf("NewClusterID lengths = %d, %d; want 16", len(a), len(b))
	}
}

func TestNewReplicaIDAndConversationIDRandom(t *testing.T) {
	r1, _ := comlink.NewReplicaID()
	r2, _ := comlink.NewReplicaID()
	if r1.Equal(r2) {
		t.Fatal("ReplicaID collision")
	}
	c1, _ := comlink.NewConversationID()
	c2, _ := comlink.NewConversationID()
	if c1.Equal(c2) {
		t.Fatal("ConversationID collision")
	}
}

func TestSystemConversationIDIsDeterministic(t *testing.T) {
	cid, _ := comlink.NewClusterID()
	a := comlink.SystemConversationID(cid)
	b := comlink.SystemConversationID(cid)
	if !a.Equal(b) {
		t.Fatal("SystemConversationID not deterministic for same ClusterID")
	}
	// Different ClusterIDs should derive different ConversationIDs.
	other, _ := comlink.NewClusterID()
	c := comlink.SystemConversationID(other)
	if a.Equal(c) {
		t.Fatal("SystemConversationID collision across different ClusterIDs")
	}
}

func TestParseRoundtrip(t *testing.T) {
	cid, _ := comlink.NewClusterID()
	parsed, err := comlink.ParseClusterID(cid.String())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Equal(cid) {
		t.Fatalf("Parse(String) round-trip failed: %v -> %v", cid, parsed)
	}

	rid, _ := comlink.NewReplicaID()
	rparsed, err := comlink.ParseReplicaID(rid.String())
	if err != nil {
		t.Fatal(err)
	}
	if !rparsed.Equal(rid) {
		t.Fatalf("ReplicaID parse round-trip failed")
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	bad := []string{
		"",        // empty
		"xyz",     // not hex
		"00",      // too short
		strings.Repeat("00", 32), // too long
		"00112233445566778899aabbccddeegg", // invalid hex chars
	}
	for _, s := range bad {
		if _, err := comlink.ParseClusterID(s); err == nil {
			t.Errorf("ParseClusterID(%q) returned no error", s)
		}
		if _, err := comlink.ParseReplicaID(s); err == nil {
			t.Errorf("ParseReplicaID(%q) returned no error", s)
		}
		if _, err := comlink.ParseConversationID(s); err == nil {
			t.Errorf("ParseConversationID(%q) returned no error", s)
		}
	}
}

func TestParseErrorsAreInvalidID(t *testing.T) {
	_, err := comlink.ParseClusterID("xyz")
	if !errors.Is(err, comlink.ErrInvalidID) {
		t.Fatalf("err = %v, want wrapping ErrInvalidID", err)
	}
}

func TestEnvDecode(t *testing.T) {
	cid, _ := comlink.NewClusterID()
	var got comlink.ClusterID
	if err := got.EnvDecode(cid.String()); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(cid) {
		t.Fatalf("EnvDecode result %v, want %v", got, cid)
	}

	var rid comlink.ReplicaID
	src, _ := comlink.NewReplicaID()
	if err := rid.EnvDecode(src.String()); err != nil {
		t.Fatal(err)
	}
	if !rid.Equal(src) {
		t.Fatal("ReplicaID EnvDecode failed")
	}
}
