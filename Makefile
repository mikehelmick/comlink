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
