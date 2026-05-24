#!/bin/sh
# Bring up the local kind cluster + load the comlink-kvd image.
# Idempotent: safe to re-run.
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-comlink}"
IMAGE="${IMAGE:-comlink-kvd:dev}"

# Create the cluster if it doesn't already exist.
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    echo "kind cluster '$CLUSTER_NAME' already exists"
else
    echo "creating kind cluster '$CLUSTER_NAME' (1 control plane + 3 workers)..."
    kind create cluster \
        --name "$CLUSTER_NAME" \
        --config "$SCRIPT_DIR/kind-config.yaml" \
        --wait 60s
fi

# Build + load the image into the cluster's containerd.
echo "building image $IMAGE..."
( cd "$REPO_ROOT" && docker build -f deploy/images/comlink-kvd/Dockerfile -t "$IMAGE" . )

echo "loading $IMAGE into kind..."
kind load docker-image "$IMAGE" --name "$CLUSTER_NAME"

echo ""
echo "kind cluster ready. kubectl context: kind-$CLUSTER_NAME"
echo "next: kubectl apply -k $REPO_ROOT/deploy/manifests/app/"
