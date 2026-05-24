# comlink Makefile
#
# Common developer / CI targets. Phase 0 establishes the baseline; later
# phases extend with their own targets.

GO              ?= go
PROTOC          ?= protoc
PROTO_DIR       := proto
PROTO_OUT       := internal/pb
PROTO_FILES     := $(shell find $(PROTO_DIR) -name '*.proto' 2>/dev/null)

.PHONY: all
all: proto build test

.PHONY: build
build:
	$(GO) build ./...

.PHONY: test
test:
	$(GO) test -race -count=1 ./...

.PHONY: test-cover
test-cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: bench
bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: lint
lint:
	$(GO) vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed; skipping (install with: go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: ci
ci: vet test

# Stress-loop the previously-flaky tests under -race. Used to verify
# Phase 7 hardening — any one of these tripping again is a regression.
# Tunable: STRESS_COUNT (default 10). On a typical laptop:
#   STRESS_COUNT=10  →  ~2  min
#   STRESS_COUNT=50  →  ~10 min
STRESS_COUNT ?= 10
STRESS_TESTS := TestDirectoryUpdateSemantics|TestSubstrateMultiReplicaConverges|TestDeterministicSMReplicasConverge|TestKVStoreFiveReplica|TestKVStoreReplicaRestart
STRESS_PKGS  := . ./examples/directory/ ./examples/kvstore/

.PHONY: stress
stress:
	$(GO) test -race -count=$(STRESS_COUNT) -timeout=1800s -run '$(STRESS_TESTS)' $(STRESS_PKGS)

# ─── Phase 8: Local Kubernetes deployment ─────────────────────────
# All of these target a kind cluster created by `make k8s-up`. The
# image is built locally and `kind load`-ed into the cluster's
# containerd — no registry needed.

DOCKER         ?= docker
IMAGE_NAME     ?= comlink-kvd
IMAGE_TAG      ?= dev
IMAGE          := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker
docker:
	$(DOCKER) build -f deploy/images/comlink-kvd/Dockerfile -t $(IMAGE) .

.PHONY: k8s-up
k8s-up:
	./deploy/local/up.sh

.PHONY: k8s-down
k8s-down:
	./deploy/local/down.sh

.PHONY: k8s-apply
k8s-apply:
	kubectl apply -k deploy/manifests/app/

.PHONY: k8s-apply-all
k8s-apply-all:
	kubectl apply -k deploy/manifests/

.PHONY: k8s-smoke
k8s-smoke:
	./deploy/local/smoke-test.sh

# Soak / chaos test. Tunable via SOAK_* env vars.
SOAK_DURATION      ?= 5m
SOAK_RESTART_EVERY ?= 45s
SOAK_WRITERS       ?= 4
SOAK_READERS       ?= 8
.PHONY: k8s-soak
k8s-soak:
	$(GO) run ./examples/kvstore/cmd/comlink-soak \
		-duration=$(SOAK_DURATION) \
		-restart-every=$(SOAK_RESTART_EVERY) \
		-writers=$(SOAK_WRITERS) \
		-readers=$(SOAK_READERS)

# Generate Go code from .proto files. Outputs land under $(PROTO_OUT).
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc.
.PHONY: proto
proto:
	@command -v $(PROTOC) >/dev/null || (echo "protoc not installed" && exit 1)
	@command -v protoc-gen-go >/dev/null || (echo "protoc-gen-go not installed" && exit 1)
	@command -v protoc-gen-go-grpc >/dev/null || (echo "protoc-gen-go-grpc not installed" && exit 1)
	@mkdir -p $(PROTO_OUT)
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

.PHONY: proto-clean
proto-clean:
	rm -rf $(PROTO_OUT)

.PHONY: clean
clean: proto-clean
	rm -f coverage.out
