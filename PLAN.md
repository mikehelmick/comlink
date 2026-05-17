# comlink — Implementation Plan

A Go implementation of the system described in:

> Mishra, S., Peterson, L. L., & Schlichting, R. D. (1993). *Consul: a communication substrate for fault-tolerant distributed programs.* Distributed Systems Engineering, 1(2), 87–103. DOI [10.1088/0967-1846/1/2/004](https://iopscience.iop.org/article/10.1088/0967-1846/1/2/004).

**Ultimate goal:** a reusable, idiomatic Go library that other distributed systems can be built on top of — replicated state machines, replicated stores, group-membership-based services, etc.

This document is the multi-session plan of record. Update it as decisions land.

---

## 1. Goals & Non-Goals

### Goals
- Faithful implementation of the algorithms in the paper: Psync, Total, SemOrder, FailureDetection, Membership (sf-groups), Recovery (three-stage).
- Idiomatic Go API. The x-kernel-style protocol composition (Dispatcher / Divider / (Re)Start) is replaced by a small Go-native composition layer.
- Thorough, deterministic tests at each phase. Causal-order correctness is property-tested in an in-memory transport that can inject loss / reorder / partition deterministically.
- Demo applications (replicated directory + at least one more) proving the substrate is reusable.

### Non-Goals (initially)
- Real-time guarantees (the paper makes none either).
- Byzantine fault tolerance — the paper assumes crash-stop failures, so do we.
- Cross-language clients. Wire format is protobuf so we *could* add them, but it's not a goal.
- Production-grade ops surface (metrics, tracing, admin APIs) until the algorithmic core is solid.

---

## 2. Design Decisions

### 2.1 Module path
`github.com/mikehelmick/comlink`

### 2.2 Concurrency model — GenServer per replica per protocol layer
We use [`github.com/mikehelmick/go-functional/genserver`](https://github.com/mikehelmick/go-functional). Each protocol instance on each replica is a GenServer:
- `Init()` produces initial state.
- `HandleCall(req, state) -> (resp, newState)` for synchronous queries (e.g. "is this message stable?").
- `HandleCast(msg, state) -> newState` for fire-and-forget message delivery.

Within a replica, the protocol stack is a small set of GenServers wired together by `Cast`s (lower layer pushes into upper layer's mailbox). Network input is delivered by Cast. Application input is also Cast (or Call when an ack is needed). This serializes all state mutation per layer, makes ordering reasoning local, and is naturally testable.

### 2.3 Wire format
Protobuf. Schemas live under `proto/`, generated code under `internal/pb/`.

### 2.4 Real transport — gRPC over TCP
The paper assumes a lossy, reorderable, asynchronous network and bakes per-message retransmit into Psync. We pick gRPC/TCP instead, which gives us reliable in-order point-to-point delivery for free. **Consequences:**

| Concern                                          | Effect                                                                        |
| ------------------------------------------------ | ----------------------------------------------------------------------------- |
| Per-message retransmit timer for normal traffic  | **Removed.** TCP/gRPC handles delivery within a stream.                       |
| Message corruption                               | **Removed.** TLS / gRPC framing handles integrity.                            |
| Predecessor-not-yet-known on receive             | **Still needed.** Two senders → independent streams → arbitrary inter-stream ordering at the receiver. The "ask the sender for the missing message" path stays. |
| Restart-time graph rebuild from peers            | **Still needed.**                                                             |
| Connection liveness as a failure-detection hint  | **Bonus.** Stream errors feed FailureDetection.                               |
| Membership / sf-groups / ordering                | **Unchanged.** All algorithmic content of the paper applies as-is.            |

Transport surface is hidden behind a `Network` interface so we keep an in-memory transport for tests with full control over loss/reorder/delay/partition (necessary to validate the algorithms — gRPC won't let us inject loss easily).

**Correctness footnote — gRPC is an optimization, not a safety property.** `stream.Send()` returns success when data hits the local OS buffer, *not* when the peer has applied it. A stream interruption can lose in-flight messages with no sender notification. Therefore Psync's lost-message protocol (request missing predecessors from the sender on demand) is the actual source of delivery guarantees; gRPC merely lets us skip the per-message retransmit *timer* under normal operation. We must never let the design assume "gRPC delivered it, so it's there" — every message that matters either lives in some replica's `MessageLog` or will be re-fetched via the lost-message path.

### 2.5 Go version
1.26.

### 2.6 License
Apache-2.0 (already in repo).

### 2.7 Determinism for tests
All time-dependent behavior (heartbeats, retransmit timers in Psync's restart path, checkpointing) goes through a `Clock` interface. Tests use a manual clock. The in-memory transport has a deterministic scheduler so the same seed reproduces the same interleaving.

### 2.8 Persistent message log (first-class)
The paper's recovery model treats the context graph as an *implicit* log: every message is durably present at *some* replica's spool until pruned (§2.3, §5.1). We make this explicit with a dedicated `log.MessageLog` abstraction — separate from the lower-level `stable.Storage` blob KV used for membership snapshots, view checkpoints, and replica state.

**Responsibilities of `log.MessageLog`:**
- `Append(msg) -> offset` durable, ordered append for messages this replica has accepted into its local context graph.
- `Lookup(MessageID) -> Message` for retransmit / restart queries from peers.
- `Range(from, to)` for restart-time replay and for serving lost-message requests.
- `Truncate(belowOffset)` for trimming below a safe high-water-mark.
- Crash-safe: an `Append` that returned must survive process kill (`fsync` semantics, batchable for throughput).

**Layering:**
- `log.MessageLog` sits below Psync. Psync writes every received-and-accepted message through it before delivering upward. On restart, Psync rebuilds its in-memory context graph by replaying the log (then asking peers for anything beyond the last log entry, per §5.2).
- `stable.Storage` remains a separate, simpler put/get interface for non-message persistent state (membership list, view checkpoints, replica snapshots). They may share a backend on disk, but the abstractions are independent so each can evolve.

**Implementation strategy:**
- Phase 0: `log.MessageLog` interface + an in-memory impl + a single-file append-only impl with `fsync`-on-append. Good enough for everything through Phase 3.
- Phase 4: segmented file impl (Kafka-style — one file per segment, drop whole segments on truncate) when we need real trimming.

**Synchronous-per-message vs batched append.** Paper §5.1 spools messages to stable storage "at regular intervals" (batched). We choose synchronous append per message (Psync writes to the log before delivering upward) for correctness clarity in v1: a delivered message is always durable, no edge cases around in-flight batches and crashes. The cost is one `fsync` per accepted message. **This is a known performance trade-off, not a permanent decision** — once the algorithmic core is solid (post-Phase 4), we may add a batched-commit mode that delays delivery until a group `fsync` returns. Don't add the batching mode until benchmarks show we need it.

**High-water-mark / trim protocol (Phase 4):**
The paper's safety condition (§5.3): *"the messages in the context graph back to the time of [each replica's] last checkpoint... are available, either from being spooled to local stable storage or from the context graph copies on other processors."* Translated:
- Each replica periodically checkpoints its `Order` view (Phase 2 work) and snapshots its state-machine replica.
- After a checkpoint lands, the replica multicasts a `Watermark(replica, offset)` message announcing the position below which *it* no longer needs the log for its own recovery.
- The group-wide safe-trim frontier is `min(Watermark)` across all currently-functioning replicas (membership-list aware — failed replicas don't pin the frontier; recovering replicas do).
- Each replica trims its local `MessageLog` up to that frontier.
- This is itself a small group-coordination protocol; it lives in a `trim/` package and is wired in by `recovery/` since trim safety is a recovery concern.

We deliberately do *not* trim by anything weaker (e.g. local checkpoint only) — that would risk the paper's "thwart recovery" hazard.

### 2.9 Single conversation per replica
Paper §2.3 simplifying assumption: "we assume only a single participating process is running on each of these processors." We adopt the same simplification for v1: one conversation per replica, the replica's identity *is* the participant identity in that conversation. Multi-conversation support is a deliberate non-goal until after Phase 6 — it would expand the API surface and the routing/composition story significantly. The wire format (§2.10) carries a conversation ID anyway so that future multi-conversation support doesn't require a wire-breaking change.

### 2.10 Identity scheme — vector-clock encoding

- **Conversation ID:** opaque, globally unique, fixed for the life of a conversation. Generated when the conversation is first opened; persisted to `stable.Storage` (paper §5.1). Used as a sanity check on `MessageLog` reopen and as a routing key on the wire.
- **Replica ID:** opaque per-process identifier, stable across restarts of the same logical replica. Persisted to `stable.Storage`. The membership list `ML` is a set of these.
- **Message ID — vector clock:** `(conversation_id, sender, vector_clock)` where:
  - `sender` is the full `ReplicaID` of the originating replica (16 bytes, **not** a slot index — see "sender field" below for why).
  - `vector_clock` is `repeated uint64`, one slot per participant in the conversation, **ordered by insertion order** (the order in which replicas were added to the conversation — original `Members` first in their input order, then each subsequent successful VoteIn appends a new slot at the end). The slot for `sender` carries this message's own monotonic per-replica seq number; other slots carry the highest seq from each respective participant that this message causally depends on.

  *Why insertion-order rather than sort-by-ReplicaID:* both are deterministic across replicas (insertion-order is deterministic because MemberAdd events are totally ordered through the system conversation; every replica processes them in the same order). Insertion-order has one decisive advantage for `VoteIn` evolution: new slots always append to the end, never insert in the middle, so a shorter "old-era" vector is just a prefix of the new shape and lazy padding (zeros at the end) is correct without needing to track membership history.
  - This is a vector-clock encoding (not a single-scalar Lamport clock — that would give only total order, which would break Psync's whole partial-order premise). Two messages M₁ and M₂ are concurrent iff neither vector dominates; M₁ causally precedes M₂ iff every component of M₁'s vector ≤ M₂'s and they're not equal.
  - **The vector replaces an explicit `predecessors` field on the wire envelope.** M's direct DAG parents are derivable on receive: for each slot `i ≠ sender`, the parent at slot `i` is the message from participant `i` with seq `vector_clock[i]` (if `> 0`); the parent on `sender`'s own slot is `sender`'s prior message at seq `vector_clock[sender] − 1`.
  - **Sender field uses the full `ReplicaID` (16 bytes), not a slot index.** A slot index would be cheaper on the wire but ambiguous across membership-change boundaries (the dangerous "same vector length, disagreeing slot order" case if any subset of replicas had a transient view mismatch). The full ReplicaID makes sender identity trivially unambiguous, and the receiver can sort it back into its own slot order locally.
- All three IDs are defined in the Phase 0 protobuf (`comlink.v1`) and used unchanged through every subsequent phase.

#### 2.10.1 Vector-clock evolution under membership change

The vector's length changes when the conversation's participant set changes. Both add and remove are themselves Psync messages, so all replicas apply the change at the same partial-order point and end up with the same shape; this is what makes vector-clock evolution deterministic.

- **Member add.** A `MemberAdd(new_replica_id)` proposal is sent into the conversation as a Psync message. Once it has been **unanimously acknowledged by every existing ML member**, every replica deterministically:
  1. Appends the new `ReplicaID` to the end of the slot order.
  2. Vectors used by future Sends pick up the new length.
  3. Older in-flight messages with shorter vectors are interpreted by lazy zero-padding at the end (`v[k]` for `k >= len(v)` reads as zero).
  Unanimous acknowledgment (rather than quorum) is required because all replicas need to be in lock-step on slot order; a member who didn't acknowledge can't be relied upon to know about the new slot. To add a replica while another is unreachable, first VoteOut the unreachable one (which only requires quorum), then VoteIn the new one against the now-smaller ML.
- **Member remove.** A `MemberRemove(replica_id)` proposal flows through the same path. On incorporation, the slot is **frozen but not deleted**: the vector keeps the slot with whatever value it last had, but no new messages from that replica are accepted. **Vectors only ever grow over the lifetime of a conversation.** This avoids the bookkeeping of slot renumbering and makes cross-era comparison simple — a vector from an earlier era can be padded out with the (frozen) values of subsequently-frozen slots.
- **Recovery (same `ReplicaID`).** A replica that fails and recovers (paper §5.2) keeps its existing slot — its seq counter resumes where it left off. This is *not* a membership change; the slot was never frozen.
- **Re-admission of a removed `ReplicaID`** is out of scope for v1. If it ever becomes a goal, ReplicaIDs would need to be incarnation-stamped.

**Why this is shape-coordination-safe on the wire.** Slot order is insertion-order; new slots always append. Therefore:
- Receiver receives a message M with `len(M.vector_clock) == len(myView)`: parse normally; slot order is guaranteed consistent.
- Receiver receives a message M with `len(M.vector_clock) > len(myView)`: M is from a future membership era. Receiver defers M and catches up via the lost-message protocol — the MemberAdd message is itself a causal predecessor M depends on, so it'll be requested and applied before M is finally processed.
- `len(M.vector_clock) < len(myView)`: M is from a previous era (was sent before some MemberAdd this receiver has processed). The shorter vector is just a prefix; lazy-pad with zeros at the end and process. This case is expected and correct, not a protocol violation.

**Voting / proposal protocol.** The agreement mechanism for `MemberAdd` is itself a small subprotocol designed in Phase 3, reusing the partial-order-driven agreement machinery from §4.2 of the paper. Removal can use either the failure-detection path (involuntary, paper §4.2) or an explicit `MemberRemove` proposal (administrative).

### 2.11 Quorum / partition handling (extension beyond the paper)
The paper notes (§4.1) that quorum-restricted partition handling "is feasible in Consul given that processes know the size of the group of which they are a member" but is not implemented. We implement the feasible design:
- The conversation has a known initial group size `N` (the size of the original participant set; configured at conversation creation, persisted to `stable.Storage`).
- A replica is in the **majority partition** iff `|ML| > N/2`. Tie partitions (`|ML| == N/2`) are minority — no split-brain.
- Majority-partition replicas continue normal operation.
- Minority-partition replicas enter a **quiesced** state: refuse new client requests, stop appending application commands, but continue to receive and process membership / failure-detection messages so they can detect when the partition heals and rejoin via the standard recovery path.
- When a partition heals, replicas in the formerly-minority side use Recovery (Phase 4) to catch up, then the membership protocol re-incorporates them.
- Note the asymmetry: this only protects *application command* progress in the majority. The membership protocol itself can still proceed in either side; that's fine because membership is reconciled at heal time.
- Detail to design in Phase 3: how a replica decides it's in a partition (vs just slow / individual peers down) — likely a count-based heuristic on top of FailureDetection.

### 2.13 Membership protocol — split-suspicion-and-voting design (divergence from paper §4.2)

The paper conflates "I think p is silent right now" with "let's permanently remove p from the conversation" into a single protocol — `(p is down)` is both an observation AND a removal vote. We deliberately split these into two layers because failure is transient (a "downed" replica may simply come back) and we want routine churn to self-heal without permanent ML mutation.

- **Soft suspicion** is informational and recoverable:
  - When `failure.Detector` fires for `p`, the local Manager broadcasts `SuspectDown(p)` and locally `Maskout(p)` (stops accepting `p`'s messages).
  - Receivers add `p` to their `SuspectDownList` and likewise `Maskout(p)`. They DO NOT respond with Ack/Nack — `SuspectDown` is FYI, not a vote.
  - Recovery is implicit: when any replica subsequently receives a message from `p`, they clear `p` from `SuspectDownList` and `Maskin(p)`. No ML mutation occurred; the conversation continues as if `p` was briefly slow.
- **Hard removal** (permanent, deliberate) is the explicit `VoteOut(p)` operation:
  - `Manager.VoteOut(p)` broadcasts a `VoteOut(p)` event into the conversation.
  - Each peer responds with `VoteOutAck(p)` (I haven't received from `p` recently) or `VoteOutNack(p)` (I have evidence `p` is alive).
  - On quorum Ack with no Nack: `p` is removed from `ML` permanently; vectors freeze `p`'s slot per §2.10.1.
  - On any Nack: the VoteOut is aborted.
  - sf-groups (paper §4.2.1) apply: concurrent VoteOuts at the same logical time are merged so they're handled atomically.
  - A voted-out replica cannot auto-readmit; rejoining requires explicit `VoteIn`.
- **Hard addition** is the symmetric `VoteIn(p, addr)` operation:
  - `Manager.VoteIn(p, addr)` broadcasts a `VoteIn(p)` event.
  - Each peer responds `VoteInAck(p)` or `VoteInNack(p)` (Nack reasons: conflict, policy violation; in the simple case, peers just Ack).
  - On quorum Ack: `p` is inserted into `ML` at its sorted-by-ReplicaID position; vectors gain a new slot per §2.10.1; routing tables are updated to reach `p`.

**Why this split.** It cleanly separates "policy" (when to remove a member) from "mechanism" (how the conversation reaches consensus on member changes). The routine `Detector` → `SuspectDown` → mask-in/out flow handles transient failures with no protocol gymnastics. The `VoteOut`/`VoteIn` mechanism is mechanism-only — no auto-trigger; the application (or a Phase 5+ auto-eviction policy component) decides when to invoke it. The §4.1 correctness conditions still apply, just at the VoteOut/VoteIn layer rather than at SuspectDown.

**Recovery is implicit, not explicit.** Failure is *always* assumed transient unless an administrative action (VoteOut) explicitly removes the member. A replica that goes silent and later resumes sending is automatically welcomed back — receivers' SuspectDownList entries clear when they see the replica's traffic (Phase 3(d)), and the replica's own bookkeeping catches up via heartbeats and the standard psync lost-message protocol. The paper's `(p is up)` / `(Ack, p is up)` exchange is therefore not implemented; in our split design it would be redundant with heartbeats + psync.Restart.

**Deferred from Phase 3 (carried into Phase 5 composition layer):**

- **Vector-clock reshape on VoteIn (§2.10.1 promise not yet realized).** When a VoteIn is accepted, the new replica is added to `Manager.membershipList` but psync's `Membership` is *not* yet grown. To finish the story, every replica must on accept: (a) insert a new slot at the new replica's sorted position in psync.Membership; (b) pad in-graph node vectors with 0 at the new slot, OR have the comparison/stability code handle variable-length vectors gracefully; (c) recognize old-shape messages still in flight and either pad-then-process or defer until catch-up. The new replica itself also needs a complete bootstrap path (its own Manager + Conversation + Restart against the existing leaf set).
- **Time-bounded or log-volume-bounded auto-VoteOut.** The base policy is "failure is transient; only admin VoteOut removes." Future enhancement: an optional auto-eviction policy that triggers VoteOut after sustained silence (e.g., 10× the suspicion interval) or excessive log-volume cost (a replica we can't trim past because it never acks). Defaults stay conservative (no auto-eviction) so applications opt in.
- **Membership-only stability function (paper §4.2.2).** Phase 4's trim protocol turned out NOT to need it after all — the trim safety rule "wait for every active member's watermark" handled by `trim.Tracker.SafeFrontier` is sufficient. Phase 5/6 may surface a need; plumb if so.

**Deferred from Phase 4 (carried into Phase 5 composition layer):**

- **Segmented-file `MessageLog` impl** (PLAN §2.8 Phase-4 promise). The current `log.File` impl treats `Truncate` as a logical drop — entries below the threshold are unreadable, but the on-disk file is not reclaimed. The trim PROTOCOL is fully working; what's missing is physical disk reclamation. A Kafka-style segmented impl (one file per segment, drop whole segments on Truncate) is the right shape; engineering work, not algorithmic, hence deferred.
Paper §5.1 says "(Re)Start in particular—save information to recreate the proper connections among various protocols used in Consul following a crash." Because our composition layer (`stack/`, Phase 5) is *static Go code* — not a runtime-configured protocol graph like the x-kernel — there are no "connections" to persist. (Re)Start's persistence concern collapses to: open the same binary; the wiring is the same. We document this so a paper reader doesn't go looking for our equivalent.

---

## 3. Repository Layout (target)

```
comlink/
├── PLAN.md                       (this file)
├── README.md
├── LICENSE
├── go.mod
├── proto/                        protobuf source
│   └── comlink/v1/*.proto
├── internal/pb/                  generated protobuf code
├── clock/                        Clock interface + real + manual impls
├── transport/
│   ├── transport.go              Network interface
│   ├── memory/                   in-memory transport (tests)
│   └── grpc/                     gRPC/TCP transport (real)
├── stable/                       StableStorage interface + impls (memory, file) — non-message persistent KV
├── log/                          MessageLog interface + impls (memory, single-file in P0; segmented file in P4)
├── psync/                        Phase 1
├── order/                        Phase 2 (PartialOrder, Total, SemOrder)
├── failure/                      Phase 3 (FailureDetection)
├── membership/                   Phase 3 (Membership / sf-groups)
├── trim/                         Phase 4: high-water-mark trim protocol
├── recovery/                     Phase 4
├── stack/                        Phase 5: composition + StateMachine API
├── examples/
│   ├── directory/                Phase 6: replicated directory from the paper
│   └── kvstore/                  Phase 6: a second app to prove reusability
└── docs/
    └── notes/                    design notes per phase
```

---

## 4. Testing Strategy

For every phase:
1. **Unit tests** on the GenServer state functions (pure-ish, easy to test).
2. **Scenario tests** in the in-memory transport: spin up N replicas, drive the network via the deterministic scheduler, assert invariants hold for all observed states.
3. **Property tests** (using `testing/quick` or `gopter`) for the core invariants where applicable: causal-order delivery, sf-group equivalence, recovery convergence.
4. **Race-detector** on every test run (`go test -race`).
5. A smoke test on the gRPC transport so we know the wire path works even though most correctness testing happens in-memory.
6. **Benchmarks** tracking the paper's equivalent measurements (Psync round-trip, SemOrder vs Total response time, failure-handling overhead, checkpointing overhead). Each phase that adds an algorithmic feature also adds the corresponding benchmark so we have continuous performance visibility and regressions are caught at the phase that introduced them.

**Phase exit criterion:** the suite for that phase is green and covers every named invariant in the paper for that layer. A phase doesn't ship until that's true.

---

## 5. Phased Roadmap

Each phase has a **scope**, **exit criterion**, and **artifacts**.

### Phase 0 — Foundation
**Scope:** module init; `go.mod` (Go 1.26); add `go-functional` dep; protobuf toolchain; **identity protobufs (`ConversationID`, `ReplicaID`, `MessageID = (conversation_id, replica_id, sequence_number)`) per §2.10** plus a placeholder `comlink.v1.Hello` message that uses them; `Network` interface; in-memory transport with deterministic scheduler & loss/reorder/partition hooks; gRPC/TCP transport (basic, just enough to send a message); `StableStorage` interface (memory + file impl) for non-message persistent KV; **`log.MessageLog` interface** with two impls — in-memory (tests) and single-file append-only with `fsync`-on-append (real); `Clock` interface (real + manual); structured logging; CI config (`go vet`, `staticcheck`, `go test -race -cover`).
**Exit criterion:**
- Both transports can exchange a `Hello` round-trip between two replicas, with the message bearing a real `MessageID` and a `ConversationID` that's been persisted to and reloaded from `StableStorage`.
- In-memory transport reproduces a fixed message ordering across runs given the same seed.
- `MessageLog` passes a crash-safety test: kill the process between `Append` and the next operation; on reopen, every successfully-returned `Append` is present and recoverable via `Range`.
- `MessageLog` rejects a reopen against a different `ConversationID` than the one it was opened with originally (sanity check from §2.10).
- `MessageLog` passes a basic perf smoke test (just to catch obvious mistakes — e.g. one `fsync` per append is OK; one `fsync` per byte is not).
**Artifacts:** `transport/`, `stable/`, `log/`, `clock/`, `internal/pb/comlink/v1/`, `.github/workflows/ci.yml`, `Makefile`.

### Phase 1 — Psync
**Scope:** the heart of the system. Conversation abstraction; in-memory context-graph DAG (parent edges derived on receive from each message's vector clock per §2.10 — there are no explicit predecessor refs on the wire); `Send` / `Receive`; lost-message protocol (request missing predecessors from sender); stability & wave detection; **wave-indexed graph API** (which wave does message M belong to; is wave W complete; enumerate messages in wave W) — needed by SemOrder, Recovery, and Membership downstream; `Maskin` / `Maskout`; **mask state persisted via `stable.Storage` so it survives restart (§5.2)**; `Restart` primitive (broadcast restart message + leaf-set exchange + graph reconstruction); **restart message retry/ack handshake (§2.3): repeated broadcast at intervals until at least one peer responds with a retransmit message**; **durable write to `log.MessageLog` for every accepted message before delivering upward**; **restart-time graph rebuild as the union of (a) replay of local `MessageLog` and (b) leaf-set / lost-message exchange with peers (§2.3) — neither source alone is sufficient when pruning has occurred**; pruning hook (the policy comes in Phase 4 — Phase 1 just exposes the trim point); **placeholder for the membership-only stability definition (§4.2.2): Psync exposes the standard stability function and a separate hook so Phase 3's Membership can plug in its own SuspectDownList-aware variant without us conflating them later**. Implemented as a GenServer per replica.
**Exit criterion:**
- Property test: under arbitrary loss / reorder / delay (within transport limits), every delivered message is delivered after all its causal predecessors at every replica.
- Stability test: a message is reported stable iff every other participant has sent at least one message in its context.
- Wave test: a wave is reported complete iff one of its messages is stable; the wave-indexed API returns consistent results across replicas for the same wave.
- Mask test: after `Maskout(p)`, no further messages from `p` are accepted; after `Maskin(p)`, they are. Mask state survives a restart of the masking replica.
- Restart-handshake test: a `Restart` invocation eventually completes even if the first N broadcasts of the restart message are dropped by the transport.
- **Durability test:** every message that Psync ever delivers to an upper layer was first durably appended to the local `MessageLog` (verified by killing the process mid-flight and confirming no upper-layer-observed message is missing on reopen).
- **Log-driven restart test:** a replica that drops its entire in-memory state rebuilds an identical context graph (modulo pruned regions) by combining its `MessageLog` replay with the leaf-set exchange + lost-message protocol against live peers; the result is consistent with functioning peers.
- **Pruned-region recovery test:** specifically the case where the recovering replica's local `MessageLog` is missing messages that *were* delivered to it pre-failure (pruned away locally but still resident on a peer); the union with peer state must restore them.
- **Benchmark:** Psync one-byte round-trip latency between two replicas, comparable to the paper's Table-1-equivalent (paper reports ~2.9 ms on Sun-3/75 over 10 Mbit Ethernet — modern numbers will be different, the point is to track ours over time).
**Artifacts:** `psync/` package + tests + benchmark.

### Phase 2 — Order protocols
**Scope:** `PartialOrder` (Psync passthrough), `Total` (paper ref [26], building on partial order), `SemOrder` (op-groups, commutativity-class config supporting **k disjoint commutativity sets** per the §3 generalization, not just the binary commutative/non-commutative case; continuation property `C_i`; deferred-execution machinery).
**Exit criterion:**
- Replicated counter demo using each of the three orderings converges.
- `Total`: every replica delivers messages in the same total order.
- `SemOrder`: invocations within a commutative op-group can execute in different orders at different replicas; non-commutative op-groups always execute in the same total order; the directory example from §3 of the paper produces consistent end states across replicas; the k-set generalization is exercised by a test with k=3 (e.g. reads commute with reads, writes commute with writes within a key, deletes are exclusive).
- **Benchmark:** response time for the replicated directory under `Total` vs `SemOrder` across mixes of commutative ops (0/50/75/90/99/100% commutative), tracking the paper's Tables 1 and 2.
**Artifacts:** `order/` package + tests + benchmark.

### Phase 3 — FailureDetection + Membership
**Scope:**
- **Dummy-message generation with the dual purpose from §4** — both (a) a liveness signal and (b) an ack that accelerates standard stability detection. *Idle* replicas must still emit dummies so the context graph keeps progressing; otherwise downstream concerns (Phase 4 trim watermark advancement) stall under low load.
- Suspicion driven by absence of messages; `SuspectDownList`, `SuspectUpList`, `count`; ack/nack heuristic.
- **All membership messages — `(p is down)`, `(Ack, p is down)`, `(Nack, p is down)`, `(Ack, p is up)`, `(p is up)` — are sent into the conversation as ordinary Psync messages so they participate in the partial order (§4.2.3).** Membership is a layer *above* Psync, never a sibling that bypasses it.
- **Membership-only stability function (§4.2.2):** "a message is followed by messages from all processes not in `SuspectDownList`." Lives in the `membership/` package and is wired into Membership only; the standard `psync/` stability function remains for everyone else.
- **Named logical-time concepts (§4.2.2)** modeled as first-class types: `SDT`, `ADT`, `RDT`, `SUT`, `RUT`, plus the membership-check-period boundary. These are how the paper reasons about correctness; they should appear in code and tests, not just comments.
- sf-group construction; partial-order-driven removal allowing different replicas to remove failed processes at different logical times; sf-group merging (§4.2.1) for cases where one or more processes in `S₁` fail before participating in `S₂`'s agreement, or when the protocol initiation message for `S₂` arrives before the agreement messages for `S₁`.
- Recovery-side `(p is up)` handling: every alive process sends `(Ack, p is up)` before incorporating the recovering process; the recovering process knows it's been incorporated when it has received `(Ack, p is up)` from every member of `ML`.
- Partition handling per §2.11: replicas count `|ML| > N/2` to determine majority; minority replicas quiesce.

**Exit criterion — the four §4.1 correctness conditions as separate, named tests:**
- **(§4.1 cond. a)** All functioning processes reach the same decision about a failed (or suspected failed) process. Tested with single-failure, concurrent-failure (sf-group), and false-suspicion scenarios.
- **(§4.1 cond. b)** Every functioning process starts accepting messages from a recovering process at the *same logical time* — i.e. the `(p is up)` becomes stable at the same wave at all functioning replicas, and incorporation happens only after that.
- **(§4.1 cond. c)** `ML` is modified such that stability and wave-completeness remain *stable properties* (once true, never flip back to false). Property test: across a randomized sequence of failure/recovery events, no message ever transitions from stable→unstable, no wave from complete→incomplete.
- **(§4.1 cond. d)** Every process receives all messages in the conversation — no message is lost across membership transitions.

Plus the additional scenario tests:
- Single-failure scenario: surviving replicas converge on `ML` minus the failed process.
- Concurrent-failure scenario: sf-group is formed correctly; processes in the same sf-group are removed simultaneously.
- sf-group-merge scenario: a delayed protocol-initiation message forces two would-be-separate sf-groups into one at some replicas but not others; both outcomes are semantically correct (per §4.2.1).
- False-suspicion scenario: a slow process sending a message in the same logical time as `(p is down)` is *not* removed (nack path).
- Different sf-groups at different replicas are tolerated and produce semantically equivalent outcomes (paper's Figure 7).
- Partition: minority partition quiesces per §2.11; majority continues; on heal, formerly-minority replicas catch up via Recovery (Phase 4 dependency — for Phase 3 the test stops at "minority quiesces and stays quiesced").

- **Benchmark:** response-time overhead of running with FailureDetection + Membership active vs. without (paper Table 3 reports ~0.6 ms / ~15% on Sun-3/75 — track our equivalent).
**Artifacts:** `failure/`, `membership/` packages + tests + benchmark.

### Phase 4 — Recovery + Trim
**Scope:**
- `Order` view checkpoint coordinated with state-machine checkpoint (writes a `view` snapshot through `stable.Storage`).
- **Psync mask state is included in every checkpoint (§5.2: "the mask on the participant set must be saved when a checkpoint is performed so that it is correctly restored upon recovery"),** so the recovering replica's stability calculations for the down period are correct.
- Three-stage recovery (§5.2):
  - **Stage 1 — restore to checkpoint:** rebuild context graph from `MessageLog` + `stable.Storage` view snapshot up to wave(n-view); other protocols quiescent.
  - **Stage 2 — catch up in passive mode:** transmit restart message; functioning peers retransmit missing messages; recovering replica processes them as normal *with two exceptions*: (a) all protocols except Psync run in passive mode (no outbound messages), and (b) **the recovering replica refuses new client/operation requests** (§5.2 line ~764) — they're deferred until Stage 3 completes.
  - **Stage 3 — rejoin membership:** the recovering replica determines it has been incorporated when it has received `(Ack, p is up)` from every member of `ML`; at that point the Recovery protocol switches every protocol back to active mode.
- **Recovering-replica self-mask discipline (§5.2):** during passive catch-up, when the recovering replica processes a `(p is down)` message *about itself*, it masks itself out of its own membership-protocol participant set; on `(p is up)` *about itself*, it masks itself back in. This is what lets `p` make correct stability decisions for commands that occurred while it was down. Tested explicitly.
- **Segmented `MessageLog` impl** (Kafka-style segment files) replacing the Phase-0 single-file impl, so `Truncate` is cheap.
- **`trim/` package — high-water-mark trim protocol:**
  - After each successful local checkpoint, multicast a `Watermark(replica, offset)` message announcing the lowest log offset this replica still needs for its own recovery.
  - Each replica maintains the latest `Watermark` from every member of `ML`.
  - The local safe-trim frontier = `min(Watermark[r])` over `r ∈ ML`. Failed replicas (per Phase 3 membership) drop out of the min; recovering replicas pin it.
  - Periodically call `MessageLog.Truncate(safeTrimFrontier)` on the local log.
**Exit criterion:**
- Kill-and-restart scenario: a recovering replica converges to current state and starts contributing again, without missing or double-applying any command.
- Restart-mid-membership-protocol: a replica that crashes while membership is in progress restarts cleanly.
- **Mask-persistence test:** a replica with a non-trivial mask state crashes; on recovery, the restored mask is identical to the pre-crash mask.
- **Self-mask test:** a recovering replica that processed a `(p is down)` message about itself during catch-up correctly computes stability for messages that occurred during its down period (verified by comparing its post-recovery stability state with that of a never-failed peer).
- **Client-refusal test:** during Stage 2 the recovering replica rejects (or defers) new application command submissions; on Stage 3 completion, deferred commands proceed normally.
- **Trim-safety test:** in a multi-replica run with active trimming, after killing any replica `r` and restarting it, `r` can always reconstruct everything it needs — i.e. for every message `r` needs, *some* live replica still has it in its log. No replica's `Truncate` ever discards a message that another replica's recovery would need.
- **Trim-progress test:** under steady-state load with periodic checkpoints, every replica's `MessageLog` size stays bounded (the watermark advances).
- **Failed-replica frontier test:** when a replica is removed from `ML` (failure agreement reached, Phase 3), its stale `Watermark` is dropped from the min computation and the frontier advances.
- **Recovering-replica frontier test:** a replica in the middle of recovery pins the frontier (via the watermark it advertised before crashing or via the membership protocol's recovery-state signal); the frontier resumes advancing once it's caught up.
- **Benchmark:** checkpointing overhead at varying op rates and checkpoint intervals (paper Table 4 — track our equivalent).
**Artifacts:** `recovery/` package, `trim/` package, segmented `log/` impl, tests, benchmark.

### Phase 5 — Public API: Cluster + Substrates

The public API for building distributed systems on the substrate.
Two-layer architecture decided in collaborative design pass:

- **Cluster** is one node's handle to the deployed group. Owns a
  built-in *system conversation* (well-known ConversationID
  derived from ClusterID) whose membership IS the cluster
  membership. Cluster admin operations (VoteIn/VoteOut, learning
  cluster members) all go through the system conv.
- **Substrate** is one node's handle to a specific application's
  state machine. Substrates are created via Cluster.NewSubstrate;
  each runs on its own ConversationID with a member set that is a
  subset of the cluster's. One node hosts many Substrates over
  one shared transport.

**Cluster identity & bootstrap:**
- `ClusterID` is generated once at cluster creation and persisted
  to stable.Storage. Subsequent startups load it.
- `Bootstrap.Force = true` is REQUIRED to mint a fresh ClusterID;
  default behavior assumes "join existing cluster." Operator
  error preventing accidental cluster fragmentation.
- gRPC connection handshake exchanges ClusterID via interceptors;
  mismatch → reject the connection. Prevents two clusters with
  overlapping ConversationIDs from accidentally merging.

**Sponsors (TransportConfig.Sponsors):**
- A small map of `ReplicaID -> addr` for bootstrap routing; just
  enough to make first contact with the cluster.
- After bootstrap, full peer routing learned from VoteIn events
  is persisted to stable.Storage so subsequent startups don't
  need full sponsor lists, only enough to recover if persisted
  state is lost.

**StateMachine interface (collaborative design pass):**
```go
type StateMachine interface {
    // Apply is invoked once per delivered command on this replica
    // in the substrate's chosen ordering. MUST be deterministic
    // and infallible — same prior state + same Message at every
    // replica yields the same post-state. The app handles its own
    // errors internally; substrate doesn't need a return value.
    Apply(ctx context.Context, msg *Message)
}

type Message struct {
    ID      *MessageID
    Payload []byte
    Sender  ReplicaID
    Offset  uint64  // local log offset; pass to SetWatermark when
                    // app has durably persisted state covering it
    Wave    uint64
}
```

App owns its own storage (no Snapshot/Restore on the SM); the
existing `Substrate.SetWatermark(offset)` is the trim hook.

**Configuration:**
- Plain `Config` struct, idiomatic Go (no builder).
- `env:"..."` tags on env-loadable fields via
  `github.com/sethvargo/go-envconfig`. Helper:
  `comlink.LoadConfigFromEnv(ctx) (Config, error)`.
- TransportConfig is escape-hatch / config: takes either a
  pre-built `transport.Network` (for tests) OR `Listen + Sponsors`
  for production gRPC.

**Security:**
- Transport security delegated to the deployment layer
  (Kubernetes service mesh + proxyless gRPC). Our gRPC stays
  insecure at the transport level.
- ClusterID handshake is the application-level identity check
  preventing cluster cross-contamination.

**Submit semantics:**
- `Substrate.Submit(ctx, payload) error` blocks until applied
  locally. No result return — apps embed correlation IDs in the
  payload if they need request/response.

**Now in scope (was deferred from earlier phases):**
- Vector-clock reshape on VoteIn (PLAN §2.10.1) — load-bearing
  for app substrates whose membership changes after creation.
- Multi-conversation transport routing — one node, many
  Substrates, one gRPC server. ConversationID added to wire
  Frame for dispatch.
- Persistent membership / routing state in stable.Storage.

**Sub-commit plan (~12–14 commits):**
- 5(a) ClusterID proto + generation + persistence + bootstrap discipline. ✅
- 5(b) Multi-conversation transport routing (ConversationID on Frame, dispatch). ✅
- 5(c) Vector-clock reshape on VoteIn (psync.Membership.AddSlot, in-graph reshape). ✅
- 5(d) Cluster scaffolding + system conversation wiring. ✅
- 5(e) App Substrate via Cluster.NewSubstrate + Submit + StateMachine wiring. ✅
- 5(f) Substrate-level heartbeats to make OrderingTotal usable without app cooperation. ✅
- 5(g) Cluster admin API (VoteIn/VoteOut/Members at cluster level) + persistent membership state. ✅
- 5(h) Sponsors + bootstrap fallback (joiner learns ClusterID via sponsor handshake).
- 5(i) gRPC ClusterID handshake interceptor + transport routing updates on admit/evict.
- 5(j) envconfig integration + LoadConfigFromEnv helper.
- 5(k) Determinism-violation detection test.
- 5(l) End-to-end replicated-counter integration test on the public API.
- 5(m) README quickstart.

Phase 5(g) sub-design — persistence of cluster ML:
- The membership.Manager fires a new OnMembershipChange callback
  after each accepted Add/Remove. Cluster's callback writes to
  stable.Storage under "comlink.members" (a PersistedMembership
  proto: list of ClusterMember{id, addr}).
- On (re)start, Cluster loads the persisted set and uses it as the
  system conv's initial Members (not cfg.Members, which becomes a
  bootstrap-only seed). The very first startup persists cfg.Members
  as the seed.
- MemberAdd was extended with an `addr` field so non-proposer
  replicas can persist routing alongside the membership change
  without retaining VoteIn state.
- BootstrapConfig got a ClusterID field so joiners (Phase 5(h))
  and tests can install a specific ID rather than mint one.

**Exit criterion:**
- A new replicated state machine can be built with the public API
  in under ~80 lines of application code (Cluster + Substrate +
  StateMachine impl).
- Multi-conversation: one node can host two simultaneous
  application substrates over one transport.
- Bootstrap: Scenario X (founder Force=true, joiners pull
  ClusterID via sponsors) and Scenario Y (live admin VoteIn pulls
  in a new node) both work end-to-end.
- Determinism-violation test: a SM that calls time.Now in Apply
  is detected by the cross-replica state comparison.
- Vector-clock reshape: a VoteIn during active traffic doesn't
  break in-flight messages or cause divergent state.

**Artifacts:** `comlink/` (new top-level package) — Cluster,
Substrate, Config, StateMachine, Message, ClusterID/ConversationID/
ReplicaID public types. Top-level `README.md` quickstart.

### Phase 6 — Demo apps
**Scope:** the replicated directory from §3 of the paper (canonical example; exercises SemOrder commutativity via `delete`/`insert`/`update`). A second app — likely a replicated KV store with watch — to prove the substrate generalizes.
**Exit criterion:** both apps run on 3- and 5-replica clusters under the gRPC transport, survive single and concurrent failures, and recover correctly when failed nodes restart.
**Artifacts:** `examples/directory/`, `examples/kvstore/`, integration tests under `examples/*/test/`.

---

## 6. Open Questions / Parked Decisions

- **gRPC stream shape:** per-pair bidirectional stream vs. unary RPC per `Send`. Lean toward bidirectional streams (lower per-message overhead, natural fit for the multicast pattern). Decide in Phase 0.
- **GenServer request typing:** the genserver library is generic over `[State, Request, Response]`. Each layer's `Request` will be a sealed-interface tagged union of the message types that layer accepts. Pattern to be locked in during Phase 0.
- **Partition-detection heuristic:** §2.11 specifies the policy (`|ML| > N/2` ⇒ majority); the heuristic for when a replica decides it's actually in a partition (vs just slow / individual peers down) is a Phase 3 design item.
- **Batched `MessageLog` commit:** §2.8 commits to synchronous-per-message append for v1; revisit after benchmarks in Phase 4 / 5.
- **Multi-conversation support:** §2.9 deliberately deferred until after Phase 6.
- **Metrics / tracing:** punt until after Phase 5; revisit when production readiness becomes a goal.
- **Alternative recovery strategies:** the paper mentions ISIS-style state-transfer recovery as a swappable alternative to message-replay recovery. Possible post-Phase-6 addition.

---

## 7. Reference

- Paper PDF (online): https://iopscience.iop.org/article/10.1088/0967-1846/1/2/004/pdf
- Paper extracted text (local, volatile): `/tmp/consul-paper.txt`
- Key paper sections:
  - §2.3 — Psync
  - §3   — Ordering, SemOrder
  - §4   — Membership, sf-groups
  - §5   — Recovery, three stages
- Concurrency lib: https://github.com/mikehelmick/go-functional (`genserver` package)

---

## 8. Status

| Phase                                      | Status      |
| ------------------------------------------ | ----------- |
| 0 — Foundation                             | done        |
| 1 — Psync                                  | done        |
| 2 — Order (PartialOrder, Total, SemOrder)  | done        |
| 3 — FailureDetection + Membership          | done (v1)   |
| 4 — Recovery + Trim (HWM)                  | done (v1)   |
| 5 — Public API: Cluster + Substrates       | in progress |
| 6 — Demo apps                              | not started |

Update this table as each phase moves through `in progress` and `done`.
