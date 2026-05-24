#!/bin/sh
# comlink-kvd StatefulSet entrypoint. Derives per-pod identity
# from the pod hostname so the kvd binary itself stays generic.
#
# Conventions (matches the StatefulSet manifest):
#   - Pod hostname is "<sts-name>-<ordinal>" (e.g. "comlink-kvd-0").
#   - Ordinal 0 = founder; ordinals >=1 = joiners that bootstrap
#     via the founder.
#   - COMLINK_SELF = first 16 bytes of sha256(hostname), hex.
#     This is deterministic so every pod and every neighbor agree
#     on every replica's identity without coordination.
#   - HEADLESS_SVC = name of the headless service that gives each
#     pod a stable DNS name (<sts-name>-<ordinal>.<svc>.<ns>.svc...).
#   - POD_NAMESPACE = the namespace, downward-API injected.
#
# Required env at invocation:
#   HEADLESS_SVC, POD_NAMESPACE, COMLINK_DATA_DIR,
#   COMLINK_TRANSPORT_LISTEN, COMLINK_KV_HTTP, COMLINK_KV_CONVID

set -eu

POD_HOST="$(hostname)"
ORDINAL="${POD_HOST##*-}"
STS_NAME="${POD_HOST%-*}"
POD_DNS="${POD_HOST}.${HEADLESS_SVC}.${POD_NAMESPACE}.svc.cluster.local"

# Deterministic 16-byte (32-hex-char) ReplicaID derived from the
# full pod hostname.
self_for() {
    printf '%s' "$1" | sha256sum | cut -c1-32
}

export COMLINK_SELF="$(self_for "$POD_HOST")"

# Advertise to peers using the pod's stable DNS name — the bind
# is on 0.0.0.0 but other pods need a routable address, and the
# DNS name survives pod IP changes across restarts.
export COMLINK_TRANSPORT_ADVERTISE="${POD_DNS}:7000"

# Hardcoded substrate membership: every replica must agree on
# the same Members list for the kvstore Substrate, and
# cluster.Members() can't be used (founder sees {self} only at
# substrate construction; joiners see {founder, self}).
# Compute the full set deterministically from the StatefulSet's
# 0..(N-1) ordinals.
#
# STS_REPLICAS defaults to 3 — set by the manifest's env.
: "${STS_REPLICAS:=3}"
KV_MEMBERS=""
i=0
while [ "$i" -lt "$STS_REPLICAS" ]; do
    mem="$(self_for "${STS_NAME}-${i}")"
    if [ -z "$KV_MEMBERS" ]; then
        KV_MEMBERS="$mem"
    else
        KV_MEMBERS="$KV_MEMBERS,$mem"
    fi
    i=$((i + 1))
done
export COMLINK_KV_MEMBERS="$KV_MEMBERS"

if [ "$ORDINAL" = "0" ]; then
    # Founder pod. Force-bootstrap a fresh cluster on first start;
    # subsequent restarts pick up the persisted ClusterID from the
    # PVC and skip Force (no AllowOverride set, so a re-bootstrap
    # attempt while state exists is correctly refused).
    export COMLINK_MEMBERS="$COMLINK_SELF"
    if [ ! -f "$COMLINK_DATA_DIR/cluster_state/comlink.cluster_id" ]; then
        export COMLINK_BOOTSTRAP_FORCE="true"
        echo "comlink-kvd entrypoint: founder, fresh bootstrap" >&2
    else
        echo "comlink-kvd entrypoint: founder, recovering from disk" >&2
    fi
else
    # Joiner pod. Sponsor = pod-0 in the same StatefulSet.
    SPONSOR_HOST="${STS_NAME}-0.${HEADLESS_SVC}.${POD_NAMESPACE}.svc.cluster.local"
    SPONSOR_SELF="$(self_for "${STS_NAME}-0")"
    export COMLINK_TRANSPORT_SPONSORS="${SPONSOR_SELF}@${SPONSOR_HOST}:7000"
    echo "comlink-kvd entrypoint: joiner, sponsor=${SPONSOR_SELF}@${SPONSOR_HOST}:7000" >&2
fi

echo "comlink-kvd entrypoint: self=${COMLINK_SELF} data=${COMLINK_DATA_DIR}" >&2

exec /usr/local/bin/comlink-kvd
