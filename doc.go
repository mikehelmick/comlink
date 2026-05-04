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

// Package comlink is the public API for building distributed
// state machines on top of the comlink communication substrate.
//
// Architecture (PLAN §5):
//
//   - A Cluster is one node's handle to a deployed group. Owns
//     the system conversation (well-known ConversationID derived
//     from ClusterID) whose membership IS the cluster
//     membership. Created with Bootstrap.Force=true on the
//     founder node; otherwise joins an existing cluster via
//     Sponsors.
//   - A Substrate is one node's handle to a specific
//     application's state machine. Substrates are created via
//     Cluster.NewSubstrate; each runs on its own ConversationID
//     with a member set that is a subset of the cluster's. One
//     node hosts many Substrates over one shared transport.
//
// Quickstart for an existing cluster (joiner):
//
//	cluster, err := comlink.NewCluster(ctx, comlink.ClusterConfig{
//	    Self:    aliceID,
//	    DataDir: "/var/comlink/alice",
//	    Transport: comlink.TransportConfig{
//	        Listen:   ":8001",
//	        Sponsors: map[comlink.ReplicaID]string{bobID: "bob:8002"},
//	    },
//	})
//	defer cluster.Close()
//
//	sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
//	    ConversationID: appConvID,
//	    Members:        []comlink.ReplicaID{aliceID, bobID, carolID},
//	    Ordering:       comlink.Total,
//	    StateMachine:   myKVStore,
//	})
//	defer sub.Close()
//	sub.Submit(ctx, []byte("set foo bar"))
//
// To bootstrap a fresh cluster, the founder passes
// Bootstrap.Force=true; this generates a new ClusterID and
// persists it. Operator error preventing accidental cluster
// fragmentation: without Force=true, a node with no persisted
// state will refuse to start.
package comlink
