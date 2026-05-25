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
	"time"

	"github.com/mikehelmick/comlink"
)

// TestRegistryRegisterAndGet: a Register on a single-replica
// cluster shows up in Get + List after the pump processes it.
func TestRegistryRegisterAndGet(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()

	reg := comlink.NewMetadataRegistry(cluster)
	defer reg.Close()

	convID, _ := comlink.NewConversationID()
	want := comlink.ConvInfo{
		Name:    "alpha",
		Conv:    convID,
		Members: []comlink.ReplicaID{id16("alice")},
		Properties: map[string]string{
			"tenant": "acme",
		},
	}
	if err := reg.Register(ctx, want); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Poll for local apply.
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, ok := reg.Get("alpha")
		if ok && got.Conv.Equal(convID) {
			if got.Properties["tenant"] != "acme" {
				t.Errorf("tenant property = %q, want acme", got.Properties["tenant"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Register never applied locally; entries = %v", reg.List())
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := reg.List(); len(got) != 1 {
		t.Fatalf("List len = %d, want 1", len(got))
	}
}

// TestRegistryUnregister: Register then Unregister; Get returns
// (zero, false) afterward.
func TestRegistryUnregister(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()

	reg := comlink.NewMetadataRegistry(cluster)
	defer reg.Close()

	convID, _ := comlink.NewConversationID()
	if err := reg.Register(ctx, comlink.ConvInfo{
		Name: "alpha", Conv: convID,
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for the register to apply.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Get("alpha"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Register never applied")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := reg.Unregister(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	// Wait for the unregister to apply.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Get("alpha"); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Unregister never applied; entries = %v", reg.List())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRegistryWatch: a Watch channel receives both Register and
// Unregister events.
func TestRegistryWatch(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()

	reg := comlink.NewMetadataRegistry(cluster)
	defer reg.Close()

	ch, cancel := reg.Watch()
	defer cancel()

	convID, _ := comlink.NewConversationID()
	go func() {
		_ = reg.Register(ctx, comlink.ConvInfo{Name: "beta", Conv: convID})
		time.Sleep(50 * time.Millisecond)
		_ = reg.Unregister(ctx, "beta")
	}()

	wantKinds := []comlink.RegistryEventKind{
		comlink.RegistryEventRegistered,
		comlink.RegistryEventUnregistered,
	}
	for i, want := range wantKinds {
		select {
		case e := <-ch:
			if e.Kind != want {
				t.Errorf("event %d kind = %v, want %v", i, e.Kind, want)
			}
			if e.Name != "beta" {
				t.Errorf("event %d name = %q, want beta", i, e.Name)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("watch event %d never arrived", i)
		}
	}
}

// TestRegistryCrossReplicaPropagation: a 2-replica cluster,
// alice Registers, bob's registry eventually sees the entry.
func TestRegistryCrossReplicaPropagation(t *testing.T) {
	ctx := context.Background()
	alice := id16("alice")
	bob := id16("bob")
	clusterID, _ := comlink.NewClusterID()

	a, b := startTwoNodePeers(t, clusterID, alice, bob)

	regA := comlink.NewMetadataRegistry(a)
	defer regA.Close()
	regB := comlink.NewMetadataRegistry(b)
	defer regB.Close()

	convID, _ := comlink.NewConversationID()
	if err := regA.Register(ctx, comlink.ConvInfo{
		Name:    "shared",
		Conv:    convID,
		Members: []comlink.ReplicaID{alice, bob},
	}); err != nil {
		t.Fatal(err)
	}

	// Bob's registry should see it within a few hundred ms.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got, ok := regB.Get("shared"); ok && got.Conv.Equal(convID) {
			if len(got.Members) != 2 {
				t.Errorf("Members on bob = %v, want both alice and bob", got.Members)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bob never saw 'shared'; entries = %v", regB.List())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRegistryIgnoresNonRegistryMetadata: another app's metadata
// payload (not a registry op) is silently ignored.
func TestRegistryIgnoresNonRegistryMetadata(t *testing.T) {
	ctx := context.Background()
	cluster := mustNewSingleNodeCluster(t, "alice")
	defer cluster.Close()

	reg := comlink.NewMetadataRegistry(cluster)
	defer reg.Close()

	// Send a non-JSON payload.
	if err := cluster.SubmitMetadata(ctx, []byte("not-a-registry-op")); err != nil {
		t.Fatal(err)
	}
	// Send valid JSON that isn't a registry op.
	if err := cluster.SubmitMetadata(ctx, []byte(`{"hello":"world"}`)); err != nil {
		t.Fatal(err)
	}
	// Send a valid registry op so we have something to wait on.
	convID, _ := comlink.NewConversationID()
	if err := reg.Register(ctx, comlink.ConvInfo{Name: "gamma", Conv: convID}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := reg.Get("gamma"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Register never applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(reg.List()); got != 1 {
		t.Errorf("List len = %d, want 1 (non-registry payloads ignored)", got)
	}
}
