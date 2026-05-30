// Copyright 2026 the comlink authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// comlink-kvd is a tiny HTTP front-end for the kvstore demo.
// One process per replica; configure via env vars
// (LoadConfigFromEnv). Exposes:
//
//	GET    /kv/<key>        - read; 200 with value or 404 if absent
//	PUT    /kv/<key>        - body = value; replicated set
//	DELETE /kv/<key>        - replicated delete
//	GET    /cluster/info    - JSON dump of ClusterID + Members
//
// Founder (creates a fresh cluster):
//
//	COMLINK_SELF=$(openssl rand -hex 16) \
//	COMLINK_MEMBERS=$COMLINK_SELF \
//	COMLINK_DATA_DIR=/tmp/r0 \
//	COMLINK_BOOTSTRAP_FORCE=true \
//	COMLINK_TRANSPORT_LISTEN=127.0.0.1:7000 \
//	COMLINK_KV_HTTP=127.0.0.1:8000 \
//	  comlink-kvd
//
// Joiner (learns ClusterID via sponsor handshake):
//
//	COMLINK_SELF=$(openssl rand -hex 16) \
//	COMLINK_DATA_DIR=/tmp/r1 \
//	COMLINK_TRANSPORT_LISTEN=127.0.0.1:7001 \
//	COMLINK_TRANSPORT_SPONSORS=<founder_hex>@127.0.0.1:7000 \
//	COMLINK_KV_HTTP=127.0.0.1:8001 \
//	  comlink-kvd
//
// Then use curl from any terminal:
//
//	curl -XPUT --data-binary world http://127.0.0.1:8000/kv/hello
//	curl http://127.0.0.1:8001/kv/hello   # observes "world"
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mikehelmick/comlink"
	"github.com/mikehelmick/comlink/examples/kvstore"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// envKVHTTP is the bind address for this binary's HTTP
	// front-end. Not part of ClusterConfig — it's a comlink-kvd-
	// specific knob.
	envKVHTTP = "COMLINK_KV_HTTP"
	// envKVConvID is an optional hex-encoded ConversationID for
	// the kvstore substrate. If unset, every replica generates
	// its own — which is fine for the founder but breaks joiners
	// (they'd have a different substrate ID). For multi-replica
	// runs, set this to the same value on every replica.
	envKVConvID = "COMLINK_KV_CONVID"
	// envKVMembers is a comma-separated list of hex ReplicaIDs
	// that ARE in the kvstore Substrate. Required for multi-
	// replica deployments — every replica must agree on the
	// same Members list at substrate construction time, and
	// Cluster.Members() is the WRONG value to use (a joiner sees
	// a different snapshot than the founder did, etc).
	// If empty, defaults to cluster.Members() — usable only for
	// single-node demos.
	envKVMembers = "COMLINK_KV_MEMBERS"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "comlink-kvd:", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := comlink.LoadConfigFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("load env config: %w", err)
	}
	if len(cfg.Self) == 0 {
		return errors.New("COMLINK_SELF is required")
	}
	if cfg.DataDir == "" {
		return errors.New("COMLINK_DATA_DIR is required")
	}
	cfg.Logger = logger

	httpAddr := os.Getenv(envKVHTTP)
	if httpAddr == "" {
		return fmt.Errorf("%s is required", envKVHTTP)
	}

	convID, err := convIDFromEnv()
	if err != nil {
		return err
	}

	cluster, err := comlink.NewCluster(ctx, cfg)
	if err != nil {
		return fmt.Errorf("NewCluster: %w", err)
	}
	defer cluster.Close()
	logger.Info("cluster up",
		"cluster_id", cluster.ClusterID().String(),
		"self", cluster.Self().String(),
		"listen", cluster.ListenAddr(),
		"members", len(cluster.Members()))

	members, err := membersFromEnv()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		members = cluster.Members()
	}
	// Put kvstore's app-side snapshot in DataDir/kvstore/ —
	// same PVC as the comlink log, separate subdir so the
	// boundary between "app state" and "library state" is
	// visible on disk. The Store periodically fsyncs
	// state.snap there and tells the substrate trim can
	// advance past the snapshotted offset.
	snapDir := filepath.Join(cfg.DataDir, "kvstore")
	// Joiner-bootstrap: if this is a joiner (Sponsors set) AND
	// no on-disk snapshot exists yet, pull the snapshot from
	// the sponsor instead of starting empty.
	bootstrap := len(cfg.Transport.Sponsors) > 0
	ackCfg := kvstore.AckConfig{
		Disabled: os.Getenv("COMLINK_KV_ACK_DISABLED") == "true",
	}
	if v := os.Getenv("COMLINK_KV_ACK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			ackCfg.Interval = d
		}
	}
	batchCfg := kvstore.BatchingConfig{
		Disabled: os.Getenv("COMLINK_KV_BATCH_DISABLED") == "true",
	}
	store, err := kvstore.New(ctx, kvstore.Config{
		Cluster:              cluster,
		ConversationID:       convID,
		Members:              members,
		SnapshotDir:          snapDir,
		BootstrapFromSponsor: bootstrap,
		Ack:                  ackCfg,
		Batching:             batchCfg,
	})
	if err != nil {
		return fmt.Errorf("kvstore.New: %w", err)
	}
	defer store.Close()
	logger.Info("kvstore up", "conv_id", convID.String())

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           newRouter(cluster, store, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverDone := make(chan error, 1)
	go func() {
		logger.Info("HTTP listening", "addr", httpAddr)
		serverDone <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down on signal")
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown error", "err", err)
	}
	return nil
}

// membersFromEnv parses COMLINK_KV_MEMBERS (comma-separated
// hex ReplicaIDs). Empty / unset → empty slice → caller falls
// back to cluster.Members().
func membersFromEnv() ([]comlink.ReplicaID, error) {
	raw := strings.TrimSpace(os.Getenv(envKVMembers))
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]comlink.ReplicaID, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, err := comlink.ParseReplicaID(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", envKVMembers, err)
		}
		out = append(out, id)
	}
	return out, nil
}

// convIDFromEnv parses COMLINK_KV_CONVID. Defaults to an
// all-zeros ConversationID so single-process testing without
// the env var still works; multi-replica callers MUST set it
// (otherwise each replica would have its own substrate ID).
func convIDFromEnv() (comlink.ConversationID, error) {
	val := os.Getenv(envKVConvID)
	if val == "" {
		// Deterministic default for casual single-process testing.
		bs, _ := hex.DecodeString("0000000000000000000000000000abcd")
		return comlink.ConversationID(bs), nil
	}
	return comlink.ParseConversationID(val)
}

// newRouter wires the HTTP front-end.
func newRouter(cluster *comlink.Cluster, store *kvstore.Store, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	// HTTP headers for read-consistency:
	//   PUT/DELETE response: X-Comlink-Token: <token>
	//   GET request : X-Comlink-Read-At: <token>     — switches to GetAt
	//                 X-Comlink-Read-Timeout: 1s     — optional, defaults to 1s
	const (
		hdrToken       = "X-Comlink-Token"
		hdrReadAt      = "X-Comlink-Read-At"
		hdrReadTimeout = "X-Comlink-Read-Timeout"
	)

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		tokenHdr := r.Header.Get(hdrReadAt)
		if tokenHdr == "" {
			v, ok := store.Get(key)
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, v)
			return
		}
		// Consistency read.
		timeout := time.Second
		if raw := r.Header.Get(hdrReadTimeout); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil && d > 0 {
				timeout = d
			}
		}
		v, ok, err := store.GetAt(key, kvstore.Token(tokenHdr), timeout)
		switch {
		case errors.Is(err, comlink.ErrReadConsistencyTimeout):
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"error":"read_consistency_timeout"}`, http.StatusServiceUnavailable)
			return
		case errors.Is(err, comlink.ErrInvalidToken),
			errors.Is(err, comlink.ErrTokenWrongConversation):
			http.Error(w, `{"error":"invalid_token"}`, http.StatusBadRequest)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, v)
	})

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		// 16 MiB cap accommodates the soak driver's bulk mode (which
		// pushes 64–256 KiB values to exercise the snapshot streaming
		// path) while still bounding any one request's memory cost.
		body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Bigger payloads + larger keyspace means snapshot streaming
		// can take meaningfully longer than 5s under load — bump to
		// 30s so writes don't fail spuriously when a slow apply pump
		// is processing a recent snapshot trim.
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		token, err := store.Set(ctx, key, string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(hdrToken, string(token))
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		token, err := store.Delete(ctx, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(hdrToken, string(token))
		w.WriteHeader(http.StatusNoContent)
	})

	// Prometheus scrape endpoint. Exposes both comlink-library
	// metrics (cluster size, substrate submit/apply counts &
	// latencies, membership votes) and kvstore-specific ones
	// (set/get/delete rate, key count, watcher count).
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		comlink.MetricsRegistry(),
		promhttp.HandlerOpts{},
	))

	mux.HandleFunc("GET /cluster/info", func(w http.ResponseWriter, r *http.Request) {
		members := cluster.Members()
		hexMembers := make([]string, len(members))
		for i, m := range members {
			hexMembers[i] = m.String()
		}
		out := map[string]any{
			"cluster_id":   cluster.ClusterID().String(),
			"self":         cluster.Self().String(),
			"listen_addr":  cluster.ListenAddr(),
			"members":      hexMembers,
			"membership_n": len(members),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	return logged(mux, logger)
}

// logged wraps an http.Handler with a one-line access log.
func logged(h http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(lw, r)
		logger.Info("http",
			"method", r.Method,
			"path", trimPath(r.URL.Path),
			"status", lw.status,
			"dur", time.Since(start))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func trimPath(p string) string {
	if len(p) > 64 {
		return p[:64] + "…"
	}
	if i := strings.Index(p, "?"); i >= 0 {
		return p[:i]
	}
	return p
}
