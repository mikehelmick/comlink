# Comlink Developer Guide

This guide is for application developers building distributed
state on top of `github.com/mikehelmick/comlink`. It assumes
you've read [`README.md`](../README.md) and want to understand
the mental model + the APIs in enough depth to design a
multi-tenant application correctly.

## Mental model

Four concepts. Internalize these and the API stops surprising you.

```
   ┌────────────────────────────────────────────────────────────┐
   │                         Cluster                            │
   │                                                            │
   │   ┌─────────────────┐    ┌─────────────────┐               │
   │   │  System Conv    │    │   Application   │               │
   │   │  (built-in)     │    │   Substrate(s)  │   (one per    │
   │   │                 │    │                 │    conv ID)   │
   │   │  Members,       │    │  Per-app SM,    │               │
   │   │  VoteIn/Out,    │    │  Per-app data,  │               │
   │   │  app metadata   │    │  Per-app order  │               │
   │   └─────────────────┘    └─────────────────┘               │
   │             │                       │                      │
   │             └────────┬──────────────┘                      │
   │                      │                                     │
   │              Shared transport (gRPC),                      │
   │              shared ClusterID gate                         │
   └────────────────────────────────────────────────────────────┘
```

### Cluster

One per process. Owns shared infrastructure: the gRPC transport,
the ClusterID handshake, persistent member routing, and one
**system conversation** that the library uses for its own
membership protocol (`VoteIn` / `VoteOut`).

A Cluster is constructed once at startup with
`comlink.NewCluster(ctx, cfg)` and torn down with `Cluster.Close()`.

### Conversation

A *conversation* is a logical channel — a `ConversationID`
shared by every replica participating in it. Conversations
have:
- An ordered, durable message log (per replica).
- A vector-clock causal-multicast protocol (psync) on top of it.
- A membership list, frozen at substrate construction today
  (per-conversation dynamic membership is on the roadmap — see
  PLAN.md Phase 11+).

Two kinds of conversations exist:
- **System conversation** — exactly one per Cluster. Its
  `ConversationID` is derived deterministically from the
  `ClusterID` via `comlink.SystemConversationID(clusterID)`,
  so every node in the cluster agrees on it without
  coordination. Used internally by the library for membership
  protocol traffic; **also available** to applications for
  cluster-wide metadata.
- **Application conversations** — any number, created by the
  app via `Cluster.NewSubstrate(...)`. Each carries its own
  state machine, log, snapshot, ordering policy.

### Substrate

The application-facing handle to a conversation. Owns the
psync, the Order layer (Partial / Total / SemOrder), the apply
pump that dispatches to your `StateMachine`, the heartbeat-only
failure detector, and (optionally) the snapshot recovery
machinery.

Created via `Cluster.NewSubstrate(ctx, SubstrateConfig{...})`.
Closed via `Substrate.Close()`.

### StateMachine

The application code that processes messages. Implements:

```go
type StateMachine interface {
    Apply(ctx context.Context, msg *Message)
}
```

Optionally implements `Snapshotter` for snapshot-based recovery
(see [Snapshot recovery](#snapshot-recovery) below).

**MUST be deterministic.** Apply runs on every replica; if it
calls `time.Now()`, `rand`, or reads I/O, replicas diverge.
The integration test suite includes a divergence detector
that catches this (`determinism_test.go`).

## Getting started

The shortest possible app: a 1-replica cluster with one
substrate.

```go
package main

import (
    "context"
    "github.com/mikehelmick/comlink"
)

type counterSM struct{ n int }

func (s *counterSM) Apply(_ context.Context, _ *comlink.Message) {
    s.n++
}

func main() {
    ctx := context.Background()
    self, _ := comlink.NewReplicaID()

    cluster, _ := comlink.NewCluster(ctx, comlink.ClusterConfig{
        Self:      self,
        Members:   []comlink.ReplicaID{self},
        DataDir:   "/var/lib/comlink",
        Bootstrap: &comlink.BootstrapConfig{Force: true},
        Transport: comlink.TransportConfig{Listen: "0.0.0.0:7000"},
    })
    defer cluster.Close()

    convID, _ := comlink.NewConversationID()
    sub, _ := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
        ConversationID: convID,
        Members:        []comlink.ReplicaID{self},
        Ordering:       comlink.OrderingTotal,
        StateMachine:   &counterSM{},
    })
    defer sub.Close()

    sub.Submit(ctx, []byte("increment"))
}
```

For multi-replica recipes, see [`README.md` Quickstart](../README.md#quickstart---a-replicated-kv-store).

## Cluster membership: enumerating peers

```go
members := cluster.Members()  // []ReplicaID
self := cluster.Self()         // your own ReplicaID
```

`Cluster.Members()` returns a snapshot of the current cluster
ML as known to *this replica*. The list reflects every accepted
`VoteIn` and `VoteOut` that has propagated through the system
conversation.

For dynamically-changing clusters, re-read `Members()` whenever
you need a fresh view. There is no events-channel for membership
changes today — if you want one, layer it on top via the system
conversation (see next section).

## Cluster-scoped metadata: using the system conversation

The library's system conversation is a fully functional
replicated state machine; applications are encouraged to *also*
use it for low-volume cluster-wide metadata:
- **Conversation registry** — record `(convID, name, intended members)`
  triples so peers can discover what application conversations
  exist and who should be in them.
- **Membership-assignment policy** — decide which subset of
  cluster members each conversation should land on. The app
  consumes `Cluster.Members()` plus its own policy and writes
  the resulting assignment to the system conv.
- **Tenant directory** — for multi-tenant apps, a list of
  tenants and the conversations associated with each.
- **Feature flags / coordinator state** — anything small + global.

**Why ride on the system conv instead of a separate substrate?**
Because the system conv is already there, already replicated,
already snapshot-able (once your metadata SM implements
`Snapshotter`), and already has a working membership protocol
covering every cluster member. Spinning up a separate
"metadata conversation" duplicates all of that.

**API today**: the Cluster doesn't yet expose Submit/Recv for
the system conv directly. That surface is Phase 11 work. In
the meantime, applications that need cluster-wide metadata
can either:
- Build it inside their *application* substrate(s) (works for
  single-conversation apps), or
- Wait for Phase 11's `Cluster.SubmitMetadata` /
  `Cluster.MetadataMessages()` (or the equivalent
  `Cluster.MetadataSubstrate()` Substrate-like handle —
  shape still being designed).

**What apps should NEVER do**: directly create a Substrate
bound to the system conversation's `ConversationID`. That ID
is exposed for diagnostic purposes only (`Cluster.SystemConversationID()`).
The system conv's psync is already wrapped by the library's
internal `membership.Manager`; a second consumer would
conflict with its protocol traffic.

## Application conversations: per-substrate membership

Application substrates are the unit of replicated state for
the app's actual workload. Each substrate:
- Has its own `ConversationID` (mint with `NewConversationID()`).
- Has its own member list (subset of cluster members; chosen
  by the app).
- Has its own state machine.
- Has its own ordering policy (`OrderingPartial` /
  `OrderingTotal` / `OrderingSemOrder`).
- Has its own snapshot lifecycle.
- Persists to its own log directory under DataDir.

```go
sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
    ConversationID: convID,
    Members:        []comlink.ReplicaID{a, b, c}, // app picks
    Ordering:       comlink.OrderingTotal,
    StateMachine:   myStore,
})
```

The application is responsible for ensuring every replica in
`Members` actually runs a corresponding substrate. If `a`
creates the substrate with `Members={a, b, c}` but `b` never
calls `NewSubstrate` with the same `ConversationID`, the
ordering layer's wave gate will block waiting for `b`.

### Picking the member subset

The library doesn't have an opinion on which cluster members
should host which conversations. Some viable policies:
- **All conversations on all members** — simple, high
  fan-out cost; appropriate for small clusters.
- **Sharded by hash(convID)** — bound the number of replicas
  per conversation. Apps that pick this need a deterministic
  shard table that every replica agrees on (a natural fit
  for the cluster-scoped metadata above).
- **App-defined topology** — explicit "place tenant X on
  members {a, b, c}" decisions, written to metadata and
  picked up by replicas at substrate-creation time.

The right pick depends on your workload. The library gives
you the primitives; the policy is yours.

## Ordering policies

| Policy             | Semantics                                                                                       | When to use                                                                                       |
|--------------------|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `OrderingPartial`  | Causal order only (psync's natural delivery).                                                   | Commands that don't conflict OR apps that handle conflicts in their own SM.                       |
| `OrderingTotal`    | All replicas see all commands in the same total order.                                          | KV stores, counters, anything where "last writer wins" must be deterministic across replicas.     |
| `OrderingSemOrder` | Paper §3 semantic dependency: configurable per-class commutativity, totally ordered per class.  | Directory-style apps where operations on disjoint names commute. Wire up via `Classifier`.        |

For `OrderingTotal` and `OrderingSemOrder`, every member of
the substrate must keep up — a dead peer blocks wave gates
indefinitely unless you opt into auto-eviction (next section).

## Failure handling: auto-eviction

By default, a silent peer leaves wave gates closed and Submit
blocks. To make a substrate tolerate peer failures, opt in:

```go
sub, _ := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
    ...
    AutoEvict: &comlink.AutoEvictConfig{
        QuietInterval:     150 * time.Millisecond,
        SuspicionInterval: 10 * time.Second,
        OnEvict: func(peer comlink.ReplicaID, surviving []comlink.ReplicaID) {
            log.Printf("evicted %s, survivors %v", peer, surviving)
        },
    },
})
```

Tuning:
- `QuietInterval` — how often a heartbeat fires when the
  substrate has been quiet. Default 150 ms.
- `SuspicionInterval` — how long without a peer heartbeat
  before the substrate freezes that peer's slot. Default 10 s.
  Too short ⇒ a network blip causes a permanent eviction;
  too long ⇒ writes stall during pod restarts.

Eviction is **permanent for the substrate's lifetime**. A
re-admitted replica would need a fresh substrate construction
(typically via `Cluster.VoteIn` plus the app re-creating its
substrate with `AutoBootstrapFromSponsor: true`).

## Snapshot recovery

Apps with `OrderingTotal` or `OrderingSemOrder` substrates
that run long enough to trigger log trim need a way for new
joiners to bootstrap without replaying history that's already
been trimmed.

### Step 1 — implement `Snapshotter` on your SM

```go
type Snapshotter interface {
    Snapshot() (bytes []byte, throughOffset uint64, err error)
    Restore(r io.Reader) error
}
```

- `Snapshot` returns a durably-serializable representation of
  the SM as of its latest applied message, plus that message's
  `Offset` (provided to `Apply` via `Message.Offset`).
- `Restore` consumes a reader and re-installs SM state.
  `io.Reader` rather than `[]byte` so multi-GB snapshots can be
  streamed from disk rather than held in memory.

Implementations must be safe to call from any goroutine; in
particular `Snapshot` runs concurrently with `Apply`. Typical
pattern: take the same internal lock both methods use.

### Step 2 — let comlink handle the wire transfer

When a joiner constructs a substrate against the same `convID`
that's running on the founder:

```go
sub, _ := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
    ConversationID:           convID,
    Members:                  expectedMembers,
    Ordering:                 comlink.OrderingTotal,
    StateMachine:             myStore,    // implements Snapshotter
    AutoBootstrapFromSponsor: true,        // ⇐ this is the magic
})
```

`AutoBootstrapFromSponsor=true` triggers, ONLY when the local
log is empty:
1. A gRPC server-streaming `StreamSnapshot` call to the
   cluster's first sponsor (`cfg.Transport.Sponsors[0]`).
2. Chunks (1 MiB by default) are staged to
   `DataDir/snapshots/<convID>/incoming.snap` — multi-GB
   snapshots stay off-heap.
3. On stream completion, `SM.Restore(file)` is called with
   the assembled reader.
4. Substrate construction continues; the apply pump suppresses
   `Apply` for any message whose `Offset` is `<= snapshot.ThroughOffset`.

If the pull fails (sponsor unreachable, NotFound, etc.), the
substrate comes up empty and falls back to lost-message replay
from peers. Apps that need stricter guarantees should call
`Cluster.PullSnapshot(ctx, peerAddr, convID)` themselves and
pass the result via `SubstrateConfig.InitialSnapshot`.

### Step 3 — tell comlink when your snapshot is durable

```go
// After your app has fsynced its snapshot to its own storage:
sub.AdvanceSnapshotWatermark(throughOffset)
```

The watermark is published to peers via the trim protocol so
the log can be safely compacted (Phase 10(e) — currently
landing; check `PLAN.md`).

### Snapshot cadence

The library doesn't drive snapshotting; apps decide. Typical
heuristic: trigger a snapshot every N applied messages OR
every T seconds, whichever comes first.

## Joiner bootstrap end-to-end

Today's flow for a node joining an existing cluster:

1. **Cluster bootstrap** — `NewCluster` with `Sponsors` set
   (no `Force`). Sponsor handshake runs `VoteIn` on the
   founder side, returns `(ClusterID, members)`, joiner
   persists both.
2. **Substrate bootstrap** — for each conversation the
   joiner participates in, `NewSubstrate` with
   `AutoBootstrapFromSponsor: true` and a `Snapshotter` SM.
   The library auto-pulls a snapshot from the sponsor for
   each substrate's `convID`.
3. **Lost-message replay** — the substrate's psync requests
   any messages beyond the snapshot's `throughOffset` that it
   hasn't seen yet.

For Phase 10's scope, the joiner needs to know *which*
substrates to construct. With cluster-scoped metadata in
the system conv (forthcoming), the joiner reads the conv
registry and creates the corresponding substrates. Today,
the app configures this out-of-band (env vars, config files).

## Wire format and protocol notes

Useful when debugging or tuning:
- gRPC over TCP; one shared server per Cluster.
- Per-conversation traffic is multiplexed via a single
  `transport.Multiplex` (`transport/multiplex.go`); each
  message is wrapped in a `MultiplexFrame{convID, payload}`
  before transmission.
- ClusterID handshake interceptor stamps `x-comlink-cluster-id`
  on every non-bootstrap RPC; mismatched IDs are rejected
  with `PermissionDenied`. Exempt methods (Join,
  StreamSnapshot) live in `transport/grpc.ExemptHandshakeMethods`.
- Snapshot streaming: `Cluster.StreamSnapshot` is a
  server-streaming RPC; default chunk size 1 MiB
  (`DefaultSnapshotChunkBytes`). Each chunk has
  `header` (first chunk only), `chunk_index`, `data`,
  `last` bool.

## Observability

Library metrics are registered on `comlink.MetricsRegistry()`
— a Prometheus registry the app exposes via standard
`promhttp.HandlerFor`. See `metrics.go` for the full list;
the key ones:

- `comlink_cluster_members{cluster_id}`
- `comlink_substrate_messages_submitted_total{conv_id}`
- `comlink_substrate_messages_applied_total{conv_id}`
- `comlink_substrate_apply_duration_seconds{conv_id}`
- `comlink_substrate_submit_duration_seconds{conv_id}`
- `comlink_substrate_auto_evict_total{conv_id}`
- `comlink_membership_votein_total{outcome}`
- `comlink_membership_voteout_total{outcome}`

App-side metrics (in the kvstore example) are registered on
the same registry so a single `/metrics` endpoint exposes both
library and application signals.

## Determinism enforcement

`determinism_test.go` in the root package demonstrates the
divergence detector pattern: each replica's SM gets a
deterministic digest fingerprint, then digests are compared
across replicas. If any `time.Now()` or `rand` call has snuck
into Apply, the digests diverge and the test fails. Adopt the
same pattern for your own SMs.

## Where to look next

- `PLAN.md` — multi-phase implementation roadmap, design
  decisions, open items.
- `examples/kvstore/` — replicated KV store demonstrating
  `OrderingTotal` + `Snapshotter` + Watch.
- `examples/directory/` — replicated directory demonstrating
  `OrderingSemOrder` with per-key classifier.
- `examples/kvstore/cmd/comlink-kvd/` — runnable HTTP front-end
  for hand-on-keyboard testing.
- `examples/kvstore/cmd/comlink-soak/` — soak / chaos test
  driver, demonstrates the AutoEvict failure model.
- `deploy/` — local Kubernetes deployment with Prometheus +
  Grafana + OpenTelemetry collector.

## Roadmap items that affect application design

These are tracked in `PLAN.md` but worth knowing as you design:

- **Per-conversation membership protocol** (Phase 11) — today
  substrate Members is fixed at `NewSubstrate` time. A future
  release will add per-substrate VoteIn/VoteOut so substrates
  can grow and shrink independently of the cluster. Code
  written assuming static substrate membership will continue
  to work; new APIs will extend rather than replace.
- **App-level conversation lifecycle API** (Phase 11) —
  `Cluster.CreateConversation`, `Cluster.ListConversations`,
  `Cluster.DeleteConversation`, plus the system-conv-backed
  Submit/Recv for metadata.
- **`StreamingSnapshotter`** (Phase 11+) — for apps whose
  PRODUCED snapshot is too large to buffer; will add a
  `Persist(io.Writer)` variant of `Snapshotter`.
- **Trim safety using `SnapshotWatermark`** (Phase 10(e),
  landing soon) — log entries become trimmable only when
  every member has both applied past them AND has a snapshot
  covering them.
