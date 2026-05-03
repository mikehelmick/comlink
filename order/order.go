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

// Package order layers ordering policies on top of psync's partial-
// order delivery stream (paper §3, PLAN Phase 2).
//
// Three strategies ship in Phase 2:
//
//   - PartialOrder: passthrough. Application sees psync deliveries
//     in their causal-order arrival sequence.
//   - Total: every replica sees the same total order. Achieved by
//     buffering until a wave is wave-complete (paper §2.3) then
//     sorting that wave's messages by sender ReplicaID and emitting.
//   - SemOrder: §3 semantic-dependent ordering with op-groups
//     (Phase 2(b)). Commutative operations within an op-group can
//     be applied in different orders at different replicas; non-
//     commutative op-groups are totally ordered.
//
// All Orders consume a *psync.Conversation, expose an Apply()
// channel of applied envelopes, and a Close() to stop. Closing the
// Order does NOT close the underlying Conversation.
package order

import "github.com/mikehelmick/comlink/psync"

// Order is the public interface for an ordering layer.
type Order interface {
	// Apply returns the channel of applied envelopes in this
	// order's chosen sequence. Closed when Close is called or the
	// underlying Conversation's delivery channel closes.
	Apply() <-chan Applied
	// Close stops the Order. The underlying Conversation is NOT
	// closed; the caller owns it.
	Close() error
}

// Applied is one envelope handed to the application by an Order
// after it has been positioned in the order's chosen sequence.
//
// Delivery is the original psync delivery (envelope + node), so
// callers that want to inspect graph context (parents, wave) don't
// need a separate query.
type Applied struct {
	psync.Delivery
}
