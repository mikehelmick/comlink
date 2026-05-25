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

// Cluster metadata API (Phase 11(a)).
//
// Apps can ride on the cluster's system conversation for small,
// cluster-wide metadata: a conversation registry, tenant
// directory, feature flags, anything that benefits from being
// replicated to every cluster member without spinning up a
// separate application substrate.
//
// The system conv is already in place — every Cluster has one,
// every member is in it, the library's membership protocol
// (VoteIn / VoteOut / SuspectDown / trim Watermark) rides on
// it too. Application metadata messages are tagged ConvFrame.app
// inside that same conv and are routed through to the
// MetadataMessages channel for the application to consume.
//
// Semantics:
//   - SubmitMetadata is fire-and-forget — it returns the
//     assigned MessageID but does NOT wait for local apply.
//     Apps that need at-least-locally-delivered semantics
//     should correlate the returned ID with what shows up on
//     MetadataMessages.
//   - MetadataMessages delivers messages from EVERY member,
//     INCLUDING self. The receiving order is the system conv's
//     causal order (which is what's needed for a deterministic
//     registry SM).
//   - Apps are responsible for handling out-of-order delivery
//     within their own ordering policy. The system conv is
//     OrderingPartial by default — apps that need total order
//     for the metadata SM should either tolerate it (the
//     pattern usually is "register-once-then-immutable" which
//     is order-insensitive) or layer their own ordering on top.
//   - The system conv's app channel buffer is large (~256)
//     but a slow consumer will eventually back-pressure the
//     receiving membership pump. Don't block in the consumer.

import (
	"context"
	"errors"

	"github.com/mikehelmick/comlink/membership"
)

// MetadataMessage is one app-level payload received on the
// system conversation. Mirrors membership.AppMessage but
// promoted to the public API.
type MetadataMessage struct {
	// From is the ReplicaID of the originating replica.
	From ReplicaID
	// Payload is the opaque application bytes. Apps choose
	// their own encoding (protobuf, JSON, etc).
	Payload []byte
}

// SubmitMetadata sends payload on the cluster's system
// conversation. Every member (including self) will receive it
// via MetadataMessages. Fire-and-forget: no waiter for local
// apply. Use a consumer goroutine reading MetadataMessages to
// observe state changes.
//
// Returns an error only if the underlying send fails (e.g., the
// cluster is closed or the genserver is unhealthy); the message
// being lost in flight is detected by the consumer (it never
// arrives) rather than reported here.
//
// Concurrent-safe.
func (c *Cluster) SubmitMetadata(ctx context.Context, payload []byte) error {
	_ = ctx // sysMgr.SendApp is currently synchronous; ctx reserved for future cancellation
	if c.sysMgr == nil {
		return errors.New("comlink: SubmitMetadata: cluster has no system manager (closed?)")
	}
	if len(payload) == 0 {
		return errors.New("comlink: SubmitMetadata: empty payload")
	}
	_, err := c.sysMgr.SendApp(payload)
	return err
}

// MetadataMessages returns the read-only channel for inbound
// system-conv app messages. Apps consume from this in a
// goroutine — typically feeding an in-memory replicated SM
// (e.g., a conversation registry).
//
// The channel is closed when the Cluster is Closed. Consumers
// should range over it and exit cleanly when it closes.
//
// Lifetime: there is exactly one channel per Cluster; multiple
// callers reading concurrently would each get a subset of
// messages (each delivered once). Apps that need fan-out
// should build it on top with their own subscriber list.
func (c *Cluster) MetadataMessages() <-chan MetadataMessage {
	if c.metadataCh == nil {
		// Lazy-initialize the public-facing channel + start the
		// fan-in goroutine that adapts membership.AppMessage to
		// our public type. This isn't strictly necessary today
		// since membership.AppMessage and MetadataMessage are
		// structurally similar, but the indirection lets us
		// evolve the public surface without breaking
		// SubmitMetadata callers.
		c.metadataCh = make(chan MetadataMessage, metadataChanBuffer)
		go c.adaptMetadataChannel()
	}
	return c.metadataCh
}

const metadataChanBuffer = 256

// adaptMetadataChannel pumps from sysMgr.Recv() into the
// public-facing metadataCh, translating AppMessage →
// MetadataMessage. Exits when sysMgr's channel closes (which
// happens at Manager.Close, which happens at Cluster.Close).
func (c *Cluster) adaptMetadataChannel() {
	defer close(c.metadataCh)
	for m := range c.sysMgr.Recv() {
		// Strip the *pb.ReplicaID + *pb.Envelope wrapping;
		// hand the app just (from, payload) — which is all
		// the contract promises.
		_ = membership.AppMessage{} // import-pinning placeholder
		select {
		case c.metadataCh <- MetadataMessage{
			From:    ReplicaID(m.From.GetValue()),
			Payload: m.Payload,
		}:
		case <-c.runCtx.Done():
			return
		}
	}
}
