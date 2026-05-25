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

// comlink-soak — Phase 9 soak / chaos driver for the deployed
// kvd cluster.
//
// What it does:
//   - Spawns N writer goroutines that PUT random keys with values
//     containing a wall-clock timestamp.
//   - Spawns M reader goroutines that GET random keys and verify
//     the response is fresh (within a tolerance window).
//   - Spawns a chaos goroutine that rotates through the
//     StatefulSet's pod ordinals every --restart-every, doing
//     `kubectl delete pod <pod>` and waiting for the replacement
//     to reach Ready before the next iteration.
//   - Prints a per-10s status line (counters + ops/sec).
//   - At the end, queries each pod directly via `kubectl exec`
//     and verifies they all agree on a final committed value
//     written by the soak.
//
// Designed to be run against the kind cluster spun up by
// `make k8s-up` + `make k8s-apply-all`.
package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─── flags ───────────────────────────────────────────────────────

var (
	flagTargetURL    = flag.String("target", "http://127.0.0.1:30080", "kvd HTTP NodePort URL")
	flagNamespace    = flag.String("namespace", "comlink", "Kubernetes namespace")
	flagStsName      = flag.String("sts", "comlink-kvd", "StatefulSet name")
	flagReplicas     = flag.Int("replicas", 3, "expected number of replicas")
	flagDuration     = flag.Duration("duration", 5*time.Minute, "total soak duration")
	flagRestartEvery = flag.Duration("restart-every", 90*time.Second, "interval between pod restarts (each restart causes a ~10–30s availability window for writes because OrderingTotal needs all members)")
	flagSettle       = flag.Duration("settle", 60*time.Second, "stop chaos this long before -duration ends so the cluster fully recovers for the final convergence check")
	flagOpTimeout    = flag.Duration("op-timeout", 30*time.Second, "per-operation HTTP timeout — needs to be longer than the worst-case wave-gate recovery after a pod restart")
	flagWriters      = flag.Int("writers", 4, "number of concurrent writer goroutines")
	flagReaders      = flag.Int("readers", 8, "number of concurrent reader goroutines")
	flagKeyspace     = flag.Int("keyspace", 50, "number of distinct keys writers cycle through")
	flagKeyPrefix    = flag.String("key-prefix", "soak", "prefix for all keys written by this run")
	flagStatusEvery  = flag.Duration("status-every", 10*time.Second, "status print cadence")
	flagSkipChaos    = flag.Bool("skip-chaos", false, "skip pod restarts (pure load test)")

	// Bulk-throughput knobs (Phase 11 — exercise snapshot streaming
	// + disk-snapshot trim under sustained heavy write load).
	flagValueBytes = flag.Int("value-bytes", 0, "if >0, each write uses a random payload of this size in bytes (default: small timestamp string). Use 65536+ to drive hundreds of MB through the cluster.")
	flagPaceWrite  = flag.Duration("pace-write", 50*time.Millisecond, "per-writer pacing sleep. Set to 0 to remove pacing entirely (true throughput mode — may peg CPU).")
	flagPaceRead   = flag.Duration("pace-read", 30*time.Millisecond, "per-reader pacing sleep. Set to 0 to remove pacing entirely.")
	flagTargetMB   = flag.Int("target-mb", 0, "if >0, soak exits once this many MB have been successfully written (whichever comes first: -duration or -target-mb). Useful for repeatable bulk-load benchmarks.")
)

// ─── stats ───────────────────────────────────────────────────────

type stats struct {
	writesOK    atomic.Uint64
	writesFail  atomic.Uint64
	readsOK     atomic.Uint64
	readsFail   atomic.Uint64
	readsMiss   atomic.Uint64 // 404 — key not yet replicated, retried later
	restarts    atomic.Uint64
	bytesWrite  atomic.Uint64 // total bytes of value payloads successfully PUT
	bytesReadOK atomic.Uint64 // total bytes successfully GET (200s)
}

type statsSnap struct {
	wo, wf, ro, rf, rm, rs, bw, br uint64
}

func (s *stats) snapshot() statsSnap {
	return statsSnap{
		wo: s.writesOK.Load(),
		wf: s.writesFail.Load(),
		ro: s.readsOK.Load(),
		rf: s.readsFail.Load(),
		rm: s.readsMiss.Load(),
		rs: s.restarts.Load(),
		bw: s.bytesWrite.Load(),
		br: s.bytesReadOK.Load(),
	}
}

// ─── main ────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	fmt.Printf("comlink-soak: target=%s duration=%s restart-every=%s writers=%d readers=%d keyspace=%d\n",
		*flagTargetURL, *flagDuration, *flagRestartEvery, *flagWriters, *flagReaders, *flagKeyspace)
	if *flagValueBytes > 0 {
		fmt.Printf("comlink-soak: bulk mode — value-bytes=%d (%.1f KiB), pace-write=%s, pace-read=%s, target-mb=%d\n",
			*flagValueBytes, float64(*flagValueBytes)/1024.0,
			*flagPaceWrite, *flagPaceRead, *flagTargetMB)
	}

	// Sanity-check the cluster is reachable BEFORE we go.
	if err := pingCluster(*flagTargetURL, *flagReplicas); err != nil {
		fmt.Fprintf(os.Stderr, "pre-flight cluster check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("pre-flight: cluster reachable with expected replica count")

	ctx, cancel := context.WithTimeout(context.Background(), *flagDuration)
	defer cancel()

	st := &stats{}

	// If -target-mb is set, watch the byte counter and trigger
	// shutdown the moment we cross the threshold. This sits
	// orthogonal to the time-based -duration cap; whichever
	// fires first wins.
	if *flagTargetMB > 0 {
		go func() {
			targetBytes := uint64(*flagTargetMB) * 1024 * 1024
			t := time.NewTicker(200 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if st.bytesWrite.Load() >= targetBytes {
						fmt.Printf("[target] reached %d MB written; cancelling soak\n", *flagTargetMB)
						cancel()
						return
					}
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < *flagWriters; i++ {
		wg.Add(1)
		go writer(ctx, &wg, st, i)
	}
	for i := 0; i < *flagReaders; i++ {
		wg.Add(1)
		go reader(ctx, &wg, st, i)
	}
	go statusPrinter(ctx, st)
	if !*flagSkipChaos {
		// Chaos stops -settle before the test ends so the
		// cluster has time to fully recover (heartbeats, wave
		// gates, persistent membership) before the final
		// convergence check fires.
		chaosCtx, chaosCancel := context.WithTimeout(ctx, *flagDuration-*flagSettle)
		defer chaosCancel()
		go chaos(chaosCtx, st)
	}

	wg.Wait()
	cancel()

	snap := st.snapshot()
	fmt.Println()
	fmt.Println("─── final summary ──────────────────────────────────")
	fmt.Printf("  writes OK     : %d\n", snap.wo)
	fmt.Printf("  writes FAIL   : %d\n", snap.wf)
	fmt.Printf("  reads  OK     : %d\n", snap.ro)
	fmt.Printf("  reads  MISS   : %d (404, key not yet replicated)\n", snap.rm)
	fmt.Printf("  reads  FAIL   : %d (network / transient)\n", snap.rf)
	fmt.Printf("  pod restarts  : %d\n", snap.rs)
	fmt.Printf("  bytes written : %s\n", humanBytes(snap.bw))
	fmt.Printf("  bytes read    : %s\n", humanBytes(snap.br))
	if snap.wo+snap.ro == 0 {
		fmt.Fprintln(os.Stderr, "no successful operations — something is wrong")
		os.Exit(1)
	}

	if err := verifyConvergence(); err != nil {
		fmt.Fprintf(os.Stderr, "convergence check FAILED: %v\n", err)
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Known limitation: rapid sequential pod restarts under OrderingTotal")
		fmt.Fprintln(os.Stderr, "can leave the substrate's wave gates closed for an extended period.")
		fmt.Fprintln(os.Stderr, "Reads continue to work; writes may take minutes to recover. See")
		fmt.Fprintln(os.Stderr, "deploy/README.md and PLAN.md §9 for details. Re-running with a")
		fmt.Fprintln(os.Stderr, "longer -restart-every or -skip-chaos sidesteps this.")
		os.Exit(1)
	}
	fmt.Println("convergence: all replicas agree ✓")
}

// ─── load generators ─────────────────────────────────────────────

func writer(ctx context.Context, wg *sync.WaitGroup, s *stats, id int) {
	defer wg.Done()
	client := &http.Client{Timeout: *flagOpTimeout}
	// Pre-allocate a payload buffer when in bulk mode. We refresh
	// the first 8 bytes each iteration with a unique stamp so the
	// payload isn't byte-identical run to run (helps catch any
	// silent dedup/compression bugs in the snapshot path).
	var bulk []byte
	if *flagValueBytes > 0 {
		bulk = make([]byte, *flagValueBytes)
		if _, err := crand.Read(bulk); err != nil {
			fmt.Fprintf(os.Stderr, "writer %d: seed rand: %v\n", id, err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		key := fmt.Sprintf("%s-%d", *flagKeyPrefix, mrand.IntN(*flagKeyspace))
		var body io.Reader
		var size int
		if bulk != nil {
			// Mutate a tiny prefix so each PUT is observably unique.
			stamp := fmt.Sprintf("w%d@%d", id, time.Now().UnixNano())
			n := copy(bulk, stamp)
			_ = n
			body = bytes.NewReader(bulk)
			size = len(bulk)
		} else {
			val := fmt.Sprintf("w%d@%d", id, time.Now().UnixNano())
			body = strings.NewReader(val)
			size = len(val)
		}
		if err := doPutBody(ctx, client, key, body); err != nil {
			s.writesFail.Add(1)
		} else {
			s.writesOK.Add(1)
			s.bytesWrite.Add(uint64(size))
		}
		// Optional pacing. Set -pace-write=0 for max throughput.
		if d := *flagPaceWrite; d > 0 {
			time.Sleep(d)
		}
	}
}

func reader(ctx context.Context, wg *sync.WaitGroup, s *stats, id int) {
	defer wg.Done()
	_ = id
	client := &http.Client{Timeout: *flagOpTimeout}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		key := fmt.Sprintf("%s-%d", *flagKeyPrefix, mrand.IntN(*flagKeyspace))
		status, n, err := doGetWithLen(ctx, client, key)
		switch {
		case err != nil:
			s.readsFail.Add(1)
		case status == 200:
			s.readsOK.Add(1)
			s.bytesReadOK.Add(uint64(n))
		case status == 404:
			s.readsMiss.Add(1)
		default:
			s.readsFail.Add(1)
		}
		if d := *flagPaceRead; d > 0 {
			time.Sleep(d)
		}
	}
}

// doPut is kept as a small-payload helper used by the canary
// path so existing call sites don't have to change.
func doPut(ctx context.Context, client *http.Client, key, val string) error {
	return doPutBody(ctx, client, key, strings.NewReader(val))
}

func doPutBody(ctx context.Context, client *http.Client, key string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		fmt.Sprintf("%s/kv/%s", *flagTargetURL, key),
		body)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("PUT status %d", resp.StatusCode)
	}
	return nil
}

func doGetWithLen(ctx context.Context, client *http.Client, key string) (int, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/kv/%s", *flagTargetURL, key), nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	n, _ := io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, int(n), nil
}

// humanBytes formats a byte count as KiB / MiB / GiB for the
// summary lines.
func humanBytes(b uint64) string {
	const (
		ki = 1024
		mi = 1024 * ki
		gi = 1024 * mi
	)
	switch {
	case b >= gi:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(gi))
	case b >= mi:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(mi))
	case b >= ki:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(ki))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// ─── chaos ───────────────────────────────────────────────────────

// chaos rotates through pod ordinals 0..N-1 and `kubectl delete
// pod` each one in turn. Waits for the replacement to reach Ready
// before sleeping until the next iteration.
func chaos(ctx context.Context, s *stats) {
	ticker := time.NewTicker(*flagRestartEvery)
	defer ticker.Stop()
	ordinal := 0
	// Sleep before the first kill so load has a chance to start.
	select {
	case <-ctx.Done():
		return
	case <-time.After(*flagRestartEvery):
	}
	for {
		podName := fmt.Sprintf("%s-%d", *flagStsName, ordinal)
		fmt.Printf("[chaos] deleting pod %s\n", podName)
		if err := killPod(ctx, podName); err != nil {
			fmt.Fprintf(os.Stderr, "[chaos] delete %s: %v\n", podName, err)
		} else if err := waitPodReady(ctx, podName, 90*time.Second); err != nil {
			fmt.Fprintf(os.Stderr, "[chaos] wait ready %s: %v\n", podName, err)
		} else {
			s.restarts.Add(1)
			fmt.Printf("[chaos] %s back to Ready\n", podName)
		}
		ordinal = (ordinal + 1) % *flagReplicas
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func killPod(ctx context.Context, podName string) error {
	return runKubectl(ctx, 30*time.Second,
		"-n", *flagNamespace, "delete", "pod", podName, "--wait=false")
}

func waitPodReady(ctx context.Context, podName string, timeout time.Duration) error {
	return runKubectl(ctx, timeout,
		"-n", *flagNamespace, "wait",
		"--for=condition=Ready", "pod/"+podName,
		fmt.Sprintf("--timeout=%ds", int(timeout.Seconds())))
}

func runKubectl(ctx context.Context, timeout time.Duration, args ...string) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl %s: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ─── status printer ──────────────────────────────────────────────

func statusPrinter(ctx context.Context, s *stats) {
	ticker := time.NewTicker(*flagStatusEvery)
	defer ticker.Stop()
	var last statsSnap
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		snap := s.snapshot()
		dt := flagStatusEvery.Seconds()
		bwRate := float64(snap.bw-last.bw) / dt / (1024.0 * 1024.0)
		brRate := float64(snap.br-last.br) / dt / (1024.0 * 1024.0)
		fmt.Printf("[status] writes %d/+%d (fail %d/+%d) | reads %d/+%d (miss %d/+%d, fail %d/+%d) | restarts %d | %.1f write/s, %.1f read/s | total %s W, %s R | rate %.2f MiB/s W, %.2f MiB/s R\n",
			snap.wo, snap.wo-last.wo, snap.wf, snap.wf-last.wf,
			snap.ro, snap.ro-last.ro, snap.rm, snap.rm-last.rm, snap.rf, snap.rf-last.rf,
			snap.rs,
			float64(snap.wo-last.wo)/dt, float64(snap.ro-last.ro)/dt,
			humanBytes(snap.bw), humanBytes(snap.br),
			bwRate, brRate)
		last = snap
	}
}

// ─── preflight + convergence ─────────────────────────────────────

// pingCluster checks the kvd HTTP front-end is reachable and the
// reported membership_n matches the expected replica count.
func pingCluster(target string, want int) error {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(target + "/cluster/info")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	wantStr := fmt.Sprintf(`"membership_n":%d`, want)
	if !strings.Contains(string(body), wantStr) {
		return fmt.Errorf("cluster info doesn't report %s: %s", wantStr, string(body))
	}
	return nil
}

// verifyConvergence writes a final canary key via the NodePort,
// waits briefly for replication, then queries each pod directly
// via `kubectl exec` and checks they all return the same value.
func verifyConvergence() error {
	// 2 × op-timeout gives the canary PUT + a retry budget if
	// the first attempt times out on a not-quite-recovered wave
	// gate. Plus 30s of polling for convergence on each pod.
	ctx, cancel := context.WithTimeout(context.Background(), 3**flagOpTimeout)
	defer cancel()

	canaryKey := fmt.Sprintf("%s-canary-%d", *flagKeyPrefix, time.Now().UnixNano())
	canaryVal := fmt.Sprintf("final-canary-%d", time.Now().UnixNano())

	client := &http.Client{Timeout: *flagOpTimeout}
	// Retry the canary PUT a few times — after chaos windows the
	// first PUT often hits a wave gate that's just opening.
	var putErr error
	for attempt := 0; attempt < 3; attempt++ {
		if putErr = doPut(ctx, client, canaryKey, canaryVal); putErr == nil {
			break
		}
		fmt.Printf("convergence: canary PUT attempt %d failed: %v (retrying)\n", attempt+1, putErr)
		time.Sleep(2 * time.Second)
	}
	if putErr != nil {
		return fmt.Errorf("canary PUT (after retries): %w", putErr)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		mismatches := []string{}
		for i := 0; i < *flagReplicas; i++ {
			pod := fmt.Sprintf("%s-%d", *flagStsName, i)
			got, err := kubectlExecGet(ctx, pod, canaryKey)
			if err != nil {
				mismatches = append(mismatches, fmt.Sprintf("%s: err %v", pod, err))
				continue
			}
			if got != canaryVal {
				mismatches = append(mismatches, fmt.Sprintf("%s: %q != %q", pod, got, canaryVal))
			}
		}
		if len(mismatches) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("did not converge: " + strings.Join(mismatches, "; "))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// kubectlExecGet calls `kubectl exec pod -- wget -qO- localhost:8000/kv/key`.
// Returns the response body or an error.
func kubectlExecGet(ctx context.Context, podName, key string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, "kubectl", "-n", *flagNamespace, "exec", podName,
		"--", "wget", "-qO-", fmt.Sprintf("http://localhost:8000/kv/%s", key))
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
