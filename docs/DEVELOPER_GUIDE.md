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

## Operator playbook: moving a replica to a different node

The "pod-eviction-and-reschedule" scenario — taking a replica
offline and bringing it back up on a *different physical node*
— is a routine cluster operation. Most often it's K8s draining
a worker so the StatefulSet controller can reschedule the pod
to a healthy node; the PVC follows. Sometimes it's manual:
a planned move to a node with more RAM, or rotating out a node
that's about to be decommissioned.

### What survives the move (and what doesn't)

Survives automatically as long as the PVC is preserved:
- Comlink's persistent state (`DataDir/comlink.bolt`):
  ClusterID, persisted membership list, local message log.
- The application's on-disk snapshot
  (`DataDir/<your-app>/state.snap` if you followed the
  kvstore pattern). The state machine is fully restored from
  this file BEFORE the substrate's apply pump starts — so
  local reads serve the pre-eviction state immediately on
  restart, with zero peer round-trips.

Does NOT survive the move (operator action required):
- The pod's gRPC listen address. When the pod reschedules,
  its IP usually changes. Peers' persisted membership lists
  still point at the OLD address until either DNS catches up
  (for headless-service-named pods) or you tell them
  explicitly via `Cluster.UpdatePeerAddr`.

### Step-by-step playbook

1. **Drain / evict the old pod.** Kubernetes does this for
   you when you `kubectl drain` the node. For manual moves,
   `Cluster.Close()` on the replica is the clean shutdown
   call — it stops accepting new traffic and flushes any
   pending writes to disk.

2. **Wait for the new pod to come up against the same PVC.**
   The StatefulSet controller handles this; in a manual flow,
   start a new comlink-kvd process with the same
   `COMLINK_DATA_DIR` and the same `COMLINK_SELF`. The new
   process re-reads `comlink.bolt` to recover ClusterID + ML.

3. **Verify the snapshot loaded.** `kvstore` exposes a `Get`
   immediately after `New` returns — values written before the
   move are present without waiting on peers. (See
   `TestKVStoreReplicaMigrationToNewNode` for the exact
   assertion the integration test makes.)

4. **Update peer routing on the survivors.** If the new pod
   has a different listen address (almost always true unless
   you've pinned a NodePort), each surviving replica needs:

   ```go
   peerCluster.UpdatePeerAddr(movedReplicaID, newAddr)
   ```

   In K8s with a headless service this happens automatically
   over the next DNS refresh cycle. For a manual flow you
   call it explicitly. The library closes the cached gRPC
   connection to the old addr; the next outbound `Send`
   re-dials at the new addr.

### Known limitation: post-migration write convergence

With kvstore's `OrderingTotal` substrate today, a write
initiated AFTER a peer restart does not reliably converge
on the restarted peer within a bounded time. The new
message arrives at the peer's `psync` layer, but the
local message graph is empty post-restart (the snapshot
only restores SM state, not the vector-clock graph), so
the new message gets deferred waiting for "missing parent"
entries that the lost-message protocol can't always
fulfill from peers' trimmed logs.

The operator workaround today is to bounce the substrate
on EVERY peer after the migration completes — fresh
substrates re-establish their graph from the persisted
log. This works because each peer's local log still has
the entries the restored peer needs.

Closing this gap properly is Phase 12 work (per-substrate
snapshot that includes the vector-clock graph frontier,
OR a restart handshake that re-streams from a peer at
the snapshot's offset). Track the issue in `PLAN.md`.

### Mapping to the K8s deployment

`deploy/manifests/app/20-statefulset.yaml` mounts the PVC at
`/var/lib/comlink`. Both `comlink.bolt` and the kvstore
snapshot under `/var/lib/comlink/kvstore/state.snap` are
preserved through pod reschedules. The headless service
`comlink-kvd-peers` handles peer DNS so `UpdatePeerAddr`
calls aren't usually needed — pods locate each other by
`<pod>.<svc>` DNS names that the K8s control plane
re-resolves to whatever IP the pod currently has. The pod
anti-affinity rule (one pod per worker) means a `kubectl
drain` reliably moves a replica to a fresh node.

For a true 4-node migration demo against the local kind
cluster: scale `kind-config.yaml` to 4 workers, redeploy,
then `kubectl drain kind-worker3` to watch the StatefulSet
controller reschedule one pod onto `kind-worker4`. The
PVC follows automatically.

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
