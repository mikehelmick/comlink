# comlink

A Go implementation of the **Consul** fault-tolerant communication substrate
described in:

> Mishra, S., Peterson, L. L., & Schlichting, R. D. (1993). *Consul: a
> communication substrate for fault-tolerant distributed programs.*
> Distributed Systems Engineering, 1(2), 87–103.
> [DOI 10.1088/0967-1846/1/2/004](https://iopscience.iop.org/article/10.1088/0967-1846/1/2/004).

The goal is a reusable, idiomatic Go library that other distributed systems
can be built on top of: replicated state machines, replicated stores,
group-membership-based services.

## Status

Phases 0–5 are landed: vector-clock causal multicast (psync), partial /
total ordering layers, lost-message recovery, log trim, SuspectDown /
VoteOut / VoteIn membership protocols, a `Cluster` + `Substrate` public
API, persistent cluster membership, sponsor handshake for joiner
bootstrap, gRPC ClusterID handshake interceptor with dynamic routing,
env-var config. Phase 6 (snapshots) is next.

See [`PLAN.md`](PLAN.md) for the multi-session plan and per-phase exit
criteria. For application developers, [`docs/DEVELOPER_GUIDE.md`](docs/DEVELOPER_GUIDE.md)
explains the mental model (Cluster / Conversation / Substrate /
StateMachine), how to assign conversations to members, how to use
the cluster-scoped conv for application metadata, snapshot recovery,
failure handling, and observability.

## Quickstart — a replicated KV store

```go
package main

import (
    "context"
    "encoding/json"
    "sync"

    "github.com/mikehelmick/comlink"
)

// 1. Define your op format and state machine.
type kvOp struct{ Op, K, V string }

type kvStore struct {
    mu   sync.Mutex
    data map[string]string
}

func (s *kvStore) Apply(_ context.Context, msg *comlink.Message) {
    var o kvOp
    _ = json.Unmarshal(msg.Payload, &o)
    s.mu.Lock()
    defer s.mu.Unlock()
    switch o.Op {
    case "set":
        s.data[o.K] = o.V
    case "del":
        delete(s.data, o.K)
    }
}

func (s *kvStore) Set(ctx context.Context, sub *comlink.Substrate, k, v string) error {
    bs, _ := json.Marshal(kvOp{Op: "set", K: k, V: v})
    return sub.Submit(ctx, bs)
}

// 2. Bring up a cluster node and an application substrate.
func main() {
    ctx := context.Background()

    cfg, _ := comlink.LoadConfigFromEnv(ctx) // or build ClusterConfig directly
    cluster, err := comlink.NewCluster(ctx, cfg)
    if err != nil { panic(err) }
    defer cluster.Close()

    store := &kvStore{data: map[string]string{}}
    convID, _ := comlink.NewConversationID()
    sub, err := cluster.NewSubstrate(ctx, comlink.SubstrateConfig{
        ConversationID: convID,
        Members:        cluster.Members(),
        Ordering:       comlink.OrderingTotal,
        StateMachine:   store,
    })
    if err != nil { panic(err) }
    defer sub.Close()

    // Any Submit on `sub` is causally + totally ordered across every
    // replica that runs the same Substrate.
    _ = store.Set(ctx, sub, "hello", "world")
}
```

Founder node (creates a fresh cluster):

```sh
COMLINK_SELF=<hex16> \
COMLINK_MEMBERS=<hex16> \
COMLINK_DATA_DIR=/var/lib/comlink \
COMLINK_BOOTSTRAP_FORCE=true \
COMLINK_TRANSPORT_LISTEN=0.0.0.0:7000 \
  ./your-app
```

Joiner node (sponsor handshake — no `Force`, no shared state):

```sh
COMLINK_SELF=<hex16> \
COMLINK_DATA_DIR=/var/lib/comlink \
COMLINK_TRANSPORT_LISTEN=0.0.0.0:7000 \
COMLINK_TRANSPORT_SPONSORS=<founder_hex>@founder.host:7000 \
  ./your-app
```

The joiner dials a sponsor, gets VoteIn'd via the cluster's membership
protocol, learns the `ClusterID` and current membership list, and proceeds
as a regular replica. Routing follows membership automatically: `VoteIn`
adds a peer, `VoteOut` removes one.

## Determinism

Every `StateMachine.Apply` runs on every replica. It MUST be deterministic
— no `time.Now`, no `rand`, no I/O. comlink's integration tests include a
divergence detector that catches non-deterministic SMs by comparing
per-replica state digests (see `determinism_test.go`).

## License

Apache-2.0.
