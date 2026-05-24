# Local Kubernetes deployment

This directory contains everything needed to run the comlink-kvd
demo on a local kind cluster, with Prometheus + Grafana +
OpenTelemetry collector for observability.

## Layout

```
deploy/
├── README.md                    ← you are here
├── images/comlink-kvd/          Dockerfile + entrypoint
├── local/                       kind cluster bring-up scripts
│   ├── kind-config.yaml         1 control plane + 3 workers
│   ├── up.sh                    create cluster + load image
│   ├── down.sh                  tear down
│   └── smoke-test.sh            cross-replica Set/Get/Delete
└── manifests/
    ├── kustomization.yaml       umbrella — apply everything
    ├── app/                     comlink-kvd StatefulSet + svcs
    ├── prometheus/              scrape pods, NodePort:30090
    ├── grafana/                 provisioned dashboard, NodePort:30030
    └── otel/                    collector (OTLP+prometheus pipelines)
```

## Quickstart

Prerequisites: `docker`, `kind`, `kubectl`. Tested with
kind v0.31, kubectl v1.35.

```sh
# 1. Create kind cluster + build & load the image.
make k8s-up

# 2. Apply ALL manifests (app + prometheus + grafana + otel).
make k8s-apply-all

# 3. Wait a few seconds for the StatefulSet to come up.
kubectl -n comlink rollout status sts/comlink-kvd

# 4. Smoke test: set/get/delete across replicas.
make k8s-smoke
```

Now open in the browser (or curl):

| Endpoint                                    | What         |
|---------------------------------------------|--------------|
| http://127.0.0.1:30080/cluster/info         | kvd HTTP API |
| http://127.0.0.1:30090/                     | Prometheus   |
| http://127.0.0.1:30030/                     | Grafana      |

Grafana opens straight onto the "Comlink kvstore overview"
dashboard (no login — anonymous Admin enabled for the demo).

## Generate some traffic

```sh
# A small write loop to see panels move.
for i in $(seq 1 200); do
  curl -s -XPUT --data-binary "v$i" http://127.0.0.1:30080/kv/k$((i % 20))
done

# A read loop with some misses.
for i in $(seq 1 200); do
  curl -s http://127.0.0.1:30080/kv/k$((i % 25)) >/dev/null
done
```

## What's deployed

### `manifests/app/` — comlink-kvd

A 3-replica StatefulSet, one pod per kind worker node
(`podAntiAffinity`), each with its own 1Gi PVC for the message
log + cluster state.

- Pod-0 is the **founder** — bootstraps a fresh cluster on first
  start, recovers from disk on restart.
- Pod-1 and pod-2 are **joiners** — sponsor handshake against
  pod-0 to learn the ClusterID and current membership.
- Each pod's `COMLINK_SELF` is derived deterministically from
  `sha256(hostname)[:16]` so neighbors agree without
  coordination.
- HTTP front-end on port 8000, exposed as `NodePort:30080`.

### `manifests/prometheus/` — Prometheus

Single-replica `prom/prometheus:v3.0.0` with kubernetes_sd_configs
pod discovery in the `comlink` namespace. Scrapes only pods
annotated `prometheus.io/scrape=true` (the kvd StatefulSet sets
this). 5s scrape interval, 1h retention, NodePort:30090.

### `manifests/grafana/` — Grafana

`grafana/grafana:11.4.0`, single replica, anonymous Admin
access. The "Comlink kvstore overview" dashboard is
file-provisioned with 8 panels:

- Cluster members, total keys, watchers, VoteIn accepted (stats).
- Substrate Submit / Apply rate per replica.
- kvstore ops/sec (set, delete, get hit, get miss).
- Submit latency p50/p95/p99.
- Apply latency p50/p95/p99.

### `manifests/otel/` — OpenTelemetry collector

`otel/opentelemetry-collector-contrib:0.116.1`. Two receivers:

- **OTLP** (gRPC :4317, HTTP :4318) — ready for any in-cluster
  app to push metrics or traces. Comlink doesn't push OTLP yet
  (Phase 8(e) went Prometheus-direct for simplicity); future
  work.
- **prometheus** — scrapes the same kvd pods as the standalone
  Prometheus, just so the collector has data to re-export.

Two exporters: `debug` (basic logging) and `prometheus` server
on :8889 (in-cluster), so any pipeline output is also scrapable.

## Soak / chaos testing

`comlink-soak` runs sustained load while periodically restarting
pods one at a time:

```sh
# Defaults: 5min total, restart a pod every 90s, 4 writers + 8
# readers, 50-key working set.
make k8s-soak

# Skip chaos for a pure load test.
make k8s-soak SOAK_RESTART_EVERY=999h

# Tune the cadence.
make k8s-soak SOAK_DURATION=10m SOAK_RESTART_EVERY=2m
```

The driver prints per-10s status lines, a final summary, and
runs a cross-replica convergence check at the end (writes a
canary key, polls each pod via `kubectl exec` for agreement).

**Known limitation surfaced by the soak**: under `OrderingTotal`
(what kvstore uses), every Submit needs wave completion which
needs every member to advance. A single pod restart causes a
~10–30 s availability window for writes; back-to-back restarts
can compound and the cluster may take minutes to recover write
availability (reads always keep working since they're local).
This is a real consequence of the current design — see PLAN.md
§9 for the planned fix (auto-eviction of unhealthy pods at the
substrate level).

## Tear down

```sh
make k8s-down
```

Deletes the kind cluster entirely (PVCs and all).

## Known limitations

- **No image registry**: `make k8s-up` uses `kind load
  docker-image`. Real K8s clusters need a registry (helm chart
  + published images = a future phase).
- **No persistent storage outside kind**: PVCs use the `standard`
  storage class which on kind is `rancher.io/local-path`.
- **No TLS**: gRPC between replicas is insecure. Production
  deployments should run inside a service mesh (or wait for
  the Phase 9+ TLS work).
- **No HA Prometheus / Grafana**: single replica each. Fine for
  a local demo, NOT for production.
- **Substrate Members are hardcoded**: the entrypoint computes
  the substrate's Members list from `STS_REPLICAS` (default 3).
  Scaling the StatefulSet up/down won't grow/shrink the kvstore
  substrate without further work.
