#!/bin/sh
# Tear down the local kind cluster. Idempotent.
set -eu

CLUSTER_NAME="${CLUSTER_NAME:-comlink}"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    echo "deleting kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME"
else
    echo "kind cluster '$CLUSTER_NAME' is already absent"
fi
