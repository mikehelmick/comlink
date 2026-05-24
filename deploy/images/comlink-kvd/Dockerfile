# comlink-kvd image — the kvstore demo HTTP front-end built as a
# minimal Alpine-based container suitable for local Kubernetes
# deployment under kind. Multi-stage build keeps the runtime
# image small; sha256sum + sh are present in the runtime layer
# because the StatefulSet entrypoint script derives COMLINK_SELF
# from the pod hostname.

# ─── builder stage ──────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy go.mod/go.sum first to leverage Docker layer caching of the
# module download step — most rebuilds change source, not deps.
COPY go.mod go.sum ./
RUN go mod download

# Source. Everything because the binary imports across the
# module (psync, order, comlink, examples/kvstore, ...).
COPY . .

# Static binary so we can use a tiny base. CGO disabled for the
# same reason. Linker flags strip debug info for a smaller image.
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o /out/comlink-kvd \
    ./examples/kvstore/cmd/comlink-kvd

# ─── runtime stage ──────────────────────────────────────────────
FROM alpine:3.21

# coreutils for sha256sum; ca-certificates for any future TLS;
# tini as PID 1 (handles SIGTERM cleanly on pod termination).
RUN apk add --no-cache coreutils ca-certificates tini

# Non-root user for the runtime.
RUN addgroup -S comlink && adduser -S -G comlink comlink

COPY --from=builder /out/comlink-kvd /usr/local/bin/comlink-kvd
COPY deploy/images/comlink-kvd/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER comlink

# 7000: comlink gRPC (cluster transport). 8000: HTTP front-end.
EXPOSE 7000 8000

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"]
