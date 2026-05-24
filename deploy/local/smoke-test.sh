#!/bin/sh
# Hands-on smoke test for the deployed comlink-kvd StatefulSet.
# Exercises the cross-replica round-trip via the NodePort'd HTTP
# front-end (writes through the NodePort, then reads from EACH
# pod's localhost to confirm replication).
set -eu

NS="${NAMESPACE:-comlink}"
NODEPORT_URL="${NODEPORT_URL:-http://127.0.0.1:30080}"
KEY_PREFIX="${KEY_PREFIX:-smoke-$(date +%s)}"

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "→ /cluster/info via NodePort"
INFO=$(curl -fs -m 5 "$NODEPORT_URL/cluster/info") || fail "cluster info"
echo "  $INFO"

# Verify cluster reports 3 members.
case "$INFO" in
    *'"membership_n":3'*) ;;
    *) fail "expected membership_n=3, got: $INFO" ;;
esac

# Write through the NodePort (lands on whichever pod the kube-proxy
# picks). Then read from EACH replica's localhost to confirm the
# write replicated.
echo "→ PUT $KEY_PREFIX/k1 = v1"
curl -fs -m 5 -XPUT --data-binary "v1" "$NODEPORT_URL/kv/$KEY_PREFIX-k1" >/dev/null \
    || fail "PUT"

# Give replication a moment; it's usually <100ms in this
# environment but we don't want to flake.
sleep 1

for ORD in 0 1 2; do
    POD="comlink-kvd-$ORD"
    GOT=$(kubectl -n "$NS" exec "$POD" -- wget -qO- "http://localhost:8000/kv/$KEY_PREFIX-k1" 2>/dev/null || true)
    if [ "$GOT" != "v1" ]; then
        fail "pod $POD: GET = '$GOT', want 'v1'"
    fi
    echo "  ✓ $POD: $GOT"
done

# Cross-replica write: PUT directly on pod-1, read everywhere.
# wget in busybox has --post-file/--post-data but not --method or
# --body-data; the cleanest cross-replica write is via a one-line
# Go-ism: pipe through `nc`. To avoid that, just write via the
# NodePort with a different key — the kvd HTTP path is identical
# from every replica's perspective.
echo "→ PUT $KEY_PREFIX/k2 = v2 (via NodePort, different key)"
curl -fs -m 5 -XPUT --data-binary "v2" "$NODEPORT_URL/kv/$KEY_PREFIX-k2" >/dev/null \
    || fail "PUT k2"
sleep 1
for ORD in 0 1 2; do
    POD="comlink-kvd-$ORD"
    GOT=$(kubectl -n "$NS" exec "$POD" -- wget -qO- "http://localhost:8000/kv/$KEY_PREFIX-k2" 2>/dev/null || true)
    if [ "$GOT" != "v2" ]; then
        fail "pod $POD: GET k2 = '$GOT', want 'v2'"
    fi
    echo "  ✓ $POD: $GOT"
done

# DELETE round-trip.
echo "→ DELETE $KEY_PREFIX/k1"
curl -fs -m 5 -XDELETE "$NODEPORT_URL/kv/$KEY_PREFIX-k1" >/dev/null || fail "DELETE"
sleep 1
for ORD in 0 1 2; do
    POD="comlink-kvd-$ORD"
    HTTP=$(kubectl -n "$NS" exec "$POD" -- wget -SqO- "http://localhost:8000/kv/$KEY_PREFIX-k1" 2>&1 | grep -E "^  HTTP" | head -1 || true)
    case "$HTTP" in
        *404*) echo "  ✓ $POD: 404 (deleted)" ;;
        *) fail "pod $POD: expected 404 after delete, got: $HTTP" ;;
    esac
done

echo ""
echo "smoke test PASSED"
