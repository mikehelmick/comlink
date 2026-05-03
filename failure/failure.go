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

// Package failure implements the FailureDetection protocol from
// paper §4: dummy heartbeat emission and per-replica suspicion
// timing.
//
// The Detector is wired in by the membership.Manager:
//   - Manager.NoteSent is called every time this replica sends ANY
//     message; the Detector resets its quiet timer.
//   - Manager.NoteReceived(from) is called for every received
//     envelope; the Detector resets the per-replica liveness timer.
//
// The Detector invokes two callbacks:
//   - SendHeartbeat() — when the conversation has been quiet for
//     QuietInterval, emit a ConvFrame.heartbeat.
//   - OnSuspect(r)    — when no message has been received from r
//     for SuspicionInterval, declare r suspected. Membership uses
//     this as the trigger to start its protocol for r.
//
// Per user direction: heartbeats are reactive — they fire ONLY when
// the conversation has been quiet, not at fixed intervals during
// active traffic. This is what the paper §4 describes ("sends a
// dummy message whenever the managing process does not send any
// message for some interval of time").
package failure

import (
	"bytes"
	"sync"
	"time"

	"github.com/mikehelmick/comlink/clock"
	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"google.golang.org/protobuf/proto"
)

// Default intervals. Production deployments tune these for their
// network; tests use much shorter values via Config.
const (
	DefaultQuietInterval     = 500 * time.Millisecond
	DefaultSuspicionInterval = 2 * time.Second
	DefaultTickInterval      = 50 * time.Millisecond
)

// Config configures a Detector.
type Config struct {
	// Self is this replica's ID.
	Self *pb.ReplicaID
	// Members is the full participant set (must include Self).
	Members []*pb.ReplicaID

	// Clock for timer scheduling. Defaults to clock.NewSystem().
	Clock clock.Clock

	// QuietInterval: emit a heartbeat after the local conversation
	// has been quiet (no outbound sends) for this duration.
	// Default: 500ms.
	QuietInterval time.Duration
	// SuspicionInterval: declare a peer suspected after no message
	// has been received from it for this duration.
	// Default: 2s. Should be > QuietInterval so peers' heartbeats
	// arrive before suspicion fires.
	SuspicionInterval time.Duration
	// TickInterval: how often the internal loop checks the timers.
	// Smaller means more responsive but more CPU.
	// Default: 50ms.
	TickInterval time.Duration

	// SendHeartbeat is invoked when the Detector decides a
	// heartbeat should be emitted. Required.
	SendHeartbeat func()
	// OnSuspect is invoked once each time a peer transitions from
	// "alive" to "suspected" (no messages received for
	// SuspicionInterval). The membership protocol drives the
	// decision from there. Required.
	OnSuspect func(replica *pb.ReplicaID)
}

// Detector implements paper §4 FailureDetection.
type Detector struct {
	cfg Config

	mu              sync.Mutex
	now             time.Time
	lastSent        time.Time
	lastReceived    map[string]time.Time // string(ReplicaID.value) -> last seen
	suspected       map[string]struct{}
	memberByteForms [][]byte // pre-computed byte forms of cfg.Members for iteration

	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}
}

// New constructs and starts a Detector. Returns once the loop
// goroutine is running.
func New(cfg Config) *Detector {
	if cfg.Clock == nil {
		cfg.Clock = clock.NewSystem()
	}
	if cfg.QuietInterval == 0 {
		cfg.QuietInterval = DefaultQuietInterval
	}
	if cfg.SuspicionInterval == 0 {
		cfg.SuspicionInterval = DefaultSuspicionInterval
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = DefaultTickInterval
	}
	d := &Detector{
		cfg:          cfg,
		now:          cfg.Clock.Now(),
		lastReceived: make(map[string]time.Time),
		suspected:    make(map[string]struct{}),
		stopped:      make(chan struct{}),
		done:         make(chan struct{}),
	}
	now := cfg.Clock.Now()
	d.now = now
	d.lastSent = now
	for _, m := range cfg.Members {
		v := proto.Clone(m).(*pb.ReplicaID).GetValue()
		d.memberByteForms = append(d.memberByteForms, v)
		// Initialize as if we just heard from each peer; suspicion
		// can only fire after SuspicionInterval has elapsed since
		// startup, giving the system a fair grace period.
		d.lastReceived[string(v)] = now
	}
	go d.loop()
	return d
}

// NoteSent informs the Detector that this replica just sent a
// message. Resets the quiet timer so heartbeats only fire during
// genuine quiet.
func (d *Detector) NoteSent() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSent = d.cfg.Clock.Now()
}

// NoteReceived informs the Detector that a message arrived from
// `from`. Resets the per-replica liveness timer; clears any
// existing suspicion so the next timeout cycle starts fresh.
func (d *Detector) NoteReceived(from *pb.ReplicaID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := string(from.GetValue())
	d.lastReceived[key] = d.cfg.Clock.Now()
	delete(d.suspected, key)
}

// Suspected reports whether the Detector currently considers
// replica suspected.
func (d *Detector) Suspected(replica *pb.ReplicaID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.suspected[string(replica.GetValue())]
	return ok
}

// Close stops the Detector loop. Idempotent.
func (d *Detector) Close() error {
	d.stopOnce.Do(func() {
		close(d.stopped)
	})
	<-d.done
	return nil
}

// loop is the single goroutine that drives heartbeat + suspicion
// checks at TickInterval cadence.
func (d *Detector) loop() {
	defer close(d.done)
	timer := d.cfg.Clock.NewTimer(d.cfg.TickInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C():
			d.tick()
			timer.Reset(d.cfg.TickInterval)
		case <-d.stopped:
			return
		}
	}
}

// tick performs one round of (heartbeat-needed?) and (suspicion-
// needed?) checks. Captures callbacks under the lock to a local
// slice and invokes them OUTSIDE the lock so callbacks can call
// back into Note* without deadlocking.
func (d *Detector) tick() {
	d.mu.Lock()
	now := d.cfg.Clock.Now()
	d.now = now

	var sendHeartbeat bool
	if now.Sub(d.lastSent) >= d.cfg.QuietInterval {
		sendHeartbeat = true
		d.lastSent = now // suppress repeats until next quiet period
	}

	var newlySuspected []*pb.ReplicaID
	for _, mb := range d.memberByteForms {
		if bytes.Equal(mb, d.cfg.Self.GetValue()) {
			continue
		}
		if _, alreadySuspected := d.suspected[string(mb)]; alreadySuspected {
			continue
		}
		last, ok := d.lastReceived[string(mb)]
		if !ok {
			// Should not happen given New initialization, but be safe.
			d.lastReceived[string(mb)] = now
			continue
		}
		if now.Sub(last) >= d.cfg.SuspicionInterval {
			d.suspected[string(mb)] = struct{}{}
			newlySuspected = append(newlySuspected, &pb.ReplicaID{Value: bytes.Clone(mb)})
		}
	}
	d.mu.Unlock()

	if sendHeartbeat && d.cfg.SendHeartbeat != nil {
		d.cfg.SendHeartbeat()
	}
	if d.cfg.OnSuspect != nil {
		for _, r := range newlySuspected {
			d.cfg.OnSuspect(r)
		}
	}
}
