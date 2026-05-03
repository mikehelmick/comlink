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

// Package psync implements the Psync protocol from §2.3 of the
// Consul paper — the causal-order multicast layer at the heart of
// the substrate.
//
// Conversations exchange messages whose identity is a vector clock
// (PLAN §2.10). Each replica maintains an in-memory context graph
// whose edges encode the partial order; messages are delivered to
// the application in an order consistent with that partial order
// (i.e. never before any of their causal predecessors). When a
// message arrives whose predecessors are not yet present locally —
// because the predecessors are in flight from another sender, or
// were lost — Psync defers delivery and requests the missing
// messages from the sender via the lost-message protocol.
//
// Public surface (filled out across commits in this phase):
//
//   - Conversation: a per-replica GenServer wrapping the context
//     graph, mask, and delivery state machine.
//   - Send / Recv: the application-facing send and delivery channels.
//   - Maskin / Maskout: per-replica exclusion/inclusion (durably
//     persisted via stable.Storage; survives restart).
//   - Restart: rebuild the context graph after a process restart by
//     replaying the local MessageLog and unioning with peer state.
//
// All algorithms in this package operate on the vector-clock
// encoding; there are no explicit predecessor lists on the wire.
// Direct DAG parents are derived from each incoming message's vector
// clock relative to the local membership view (see vector.go and
// membership.go).
package psync
