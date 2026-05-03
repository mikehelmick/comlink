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

package order

import (
	"bytes"
	"sort"
	"sync"

	pb "github.com/mikehelmick/comlink/internal/pb/comlink/v1"
	"github.com/mikehelmick/comlink/psync"
)

// Classifier maps an application payload to its commutativity
// class. Class 1 is the most permissive (operations commute among
// themselves AND fire eagerly when causal predecessors are
// executed); classes 2..k each require total intra-wave ordering
// against everything else in 2..k (paper §3 generalization, PLAN
// Phase 2 exit criterion).
//
// All replicas MUST agree on the classification — the classifier
// must be deterministic given the payload.
type Classifier interface {
	ClassOf(payload []byte) int
}

// ClassifierFunc adapts a plain function into a Classifier.
type ClassifierFunc func(payload []byte) int

// ClassOf implements Classifier.
func (f ClassifierFunc) ClassOf(payload []byte) int { return f(payload) }

// SemOrder is the §3 semantic-dependent ordering. It applies
// operations in classes-and-waves:
//
//   - Class-1 ("commutative") operations fire eagerly as soon as
//     all their causal predecessors are executed; different
//     replicas may apply concurrent class-1 operations in different
//     orders.
//   - Class-2..k operations are batched per wave; when a wave is
//     wave-complete (paper §2.3) AND the continuation property
//     holds (every active replica has at least one observed
//     message at this wave or beyond), every class-≥2 operation in
//     the wave is sorted by sender ReplicaID byte order and
//     applied in that order. This produces the same intra-wave
//     class-≥2 sequence at every replica.
//
// SemOrder is self-contained: it tracks the latest vector per
// replica and a per-wave bucket of received messages from
// conv.Recv() events, so it does not call back into the
// Conversation for graph queries.
type SemOrder struct {
	conv       *psync.Conversation
	classifier Classifier
	apply      chan Applied
	stopOnce   sync.Once
	stopped    chan struct{}

	state semState
}

type semState struct {
	membership *psync.Membership
	// currentWave is the next wave whose class-≥2 ops have not yet
	// been processed. Class-1 ops can fire from earlier waves
	// eagerly; class-≥2 ops are processed wave-by-wave starting at
	// currentWave.
	currentWave uint64
	// executed records (sender_bytes, sender_seq) of every applied
	// op so subsequent ops can check predecessor readiness.
	executed map[indexKey]struct{}
	// All received envelopes bucketed by their wave number, plus a
	// per-wave dedup set.
	messagesByWave map[uint64][]*pb.Envelope
	seenInWave     map[uint64]map[indexKey]struct{}
	// latestVector[replica_bytes] is the highest-seq vector clock
	// observed from that replica. Used for stability and
	// continuation-property checks.
	latestVector map[string][]uint64
	// Eagerly-deferred class-1 ops whose predecessors weren't yet
	// executed when first delivered.
	deferredEager []*pb.Envelope
}

type indexKey struct {
	sender string
	seq    uint64
}

// NewSemOrder constructs a SemOrder bound to conv with the given
// classifier. Pump goroutine starts immediately.
func NewSemOrder(conv *psync.Conversation, c Classifier) *SemOrder {
	if c == nil {
		c = ClassifierFunc(func([]byte) int { return 1 })
	}
	o := &SemOrder{
		conv:       conv,
		classifier: c,
		apply:      make(chan Applied, 256),
		stopped:    make(chan struct{}),
		state: semState{
			membership:     conv.Membership(),
			currentWave:    1,
			executed:       make(map[indexKey]struct{}),
			messagesByWave: make(map[uint64][]*pb.Envelope),
			seenInWave:     make(map[uint64]map[indexKey]struct{}),
			latestVector:   make(map[string][]uint64),
		},
	}
	go o.pump()
	return o
}

// Apply implements Order.
func (o *SemOrder) Apply() <-chan Applied { return o.apply }

// Close stops the pump.
func (o *SemOrder) Close() error {
	o.stopOnce.Do(func() { close(o.stopped) })
	return nil
}

func (o *SemOrder) pump() {
	defer close(o.apply)
	for {
		select {
		case d, ok := <-o.conv.Recv():
			if !ok {
				return
			}
			o.observe(d.Envelope)
			// After observing, try to make progress.
			o.tryEagerExecutions()
			o.advanceWave()
		case <-o.stopped:
			return
		}
	}
}

// observe records a newly-received envelope into per-wave buckets
// and updates the latest-vector tracker.
func (o *SemOrder) observe(env *pb.Envelope) {
	st := &o.state
	id := env.GetId()
	senderBytes := id.GetSender().GetValue()
	senderSeq, err := st.membership.SenderSeq(id)
	if err != nil {
		return
	}
	wave := waveOf(id.GetVectorClock())
	key := indexKey{sender: string(senderBytes), seq: senderSeq}

	// Dedup per wave bucket.
	bucket, ok := st.seenInWave[wave]
	if !ok {
		bucket = make(map[indexKey]struct{})
		st.seenInWave[wave] = bucket
	}
	if _, dup := bucket[key]; dup {
		return
	}
	bucket[key] = struct{}{}
	st.messagesByWave[wave] = append(st.messagesByWave[wave], env)

	// Update latest vector for sender if this is later.
	if existing, present := st.latestVector[string(senderBytes)]; !present || dominatesOrEqual(id.GetVectorClock(), existing) {
		st.latestVector[string(senderBytes)] = append([]uint64(nil), id.GetVectorClock()...)
	}
}

// tryEagerExecutions applies any class-1 op (newly arrived or
// previously-deferred) whose causal predecessors are all executed.
func (o *SemOrder) tryEagerExecutions() {
	st := &o.state
	// Re-check every deferred class-1 op + every class-1 op that
	// arrived in waves we've passed (i.e., currentWave or earlier
	// waves still containing unprocessed class-1 entries). For
	// simplicity we walk every wave we've seen — wave count is
	// small in typical workloads.
	progressed := true
	for progressed {
		progressed = false
		for wave, envs := range st.messagesByWave {
			_ = wave
			for _, env := range envs {
				if !o.isClass1(env) {
					continue
				}
				if o.isExecuted(env) {
					continue
				}
				if o.predecessorsExecuted(env) {
					o.applyEnv(env)
					progressed = true
				}
			}
		}
		// Also retry the explicit deferred list (for ops that
		// straddle waves we don't bucket).
		if len(st.deferredEager) > 0 {
			remain := st.deferredEager[:0]
			for _, env := range st.deferredEager {
				if o.isExecuted(env) {
					continue
				}
				if o.predecessorsExecuted(env) {
					o.applyEnv(env)
					progressed = true
				} else {
					remain = append(remain, env)
				}
			}
			st.deferredEager = remain
		}
	}
}

// advanceWave processes all class-≥2 ops in currentWave when
// possible, then moves to the next wave with unexecuted class-≥2
// content. Loop until no further progress is possible.
func (o *SemOrder) advanceWave() {
	st := &o.state
	for {
		if !o.waveCompleteLocal(st.currentWave) {
			return
		}
		if !o.continuationProperty() {
			return
		}
		// Collect class-≥2 ops in currentWave that aren't yet
		// executed; sort by sender bytes; apply in order.
		envs := st.messagesByWave[st.currentWave]
		sort.Slice(envs, func(i, j int) bool {
			return bytes.Compare(
				envs[i].GetId().GetSender().GetValue(),
				envs[j].GetId().GetSender().GetValue(),
			) < 0
		})
		for _, env := range envs {
			if o.isClass1(env) {
				continue
			}
			if o.isExecuted(env) {
				continue
			}
			o.applyEnv(env)
		}
		// After class-≥2 in this wave, retry eager class-1.
		o.tryEagerExecutions()
		// Advance to the next wave that has any class-≥2 op
		// (executed or not — we still need to process its
		// completion gate); if no such wave exists, stop.
		next, ok := o.findNextStrictWave(st.currentWave + 1)
		if !ok {
			return
		}
		st.currentWave = next
	}
}

// findNextStrictWave returns the smallest wave >= start that has
// at least one class-≥2 op. If none, returns ok=false.
func (o *SemOrder) findNextStrictWave(start uint64) (uint64, bool) {
	st := &o.state
	candidate := start
	for {
		envs, present := st.messagesByWave[candidate]
		if present {
			for _, env := range envs {
				if !o.isClass1(env) {
					return candidate, true
				}
			}
		}
		// Nothing class-≥2 in this wave. Try the next.
		// To avoid infinite loop on truly empty future, bound the
		// search at the maximum wave we've observed so far.
		max := o.maxObservedWave()
		if candidate > max {
			return 0, false
		}
		candidate++
	}
}

func (o *SemOrder) maxObservedWave() uint64 {
	var m uint64
	for w := range o.state.messagesByWave {
		if w > m {
			m = w
		}
	}
	return m
}

// waveCompleteLocal mirrors psync's standard wave-completion check
// using only the latestVector tracker SemOrder maintains. A wave
// is complete iff some message M in the wave has, for every other
// active replica r, latestVector[r][M.sender_slot] >= M.sender_seq.
func (o *SemOrder) waveCompleteLocal(w uint64) bool {
	st := &o.state
	envs, present := st.messagesByWave[w]
	if !present || len(envs) == 0 {
		return false
	}
	for _, m := range envs {
		if o.isMessageStableLocal(m) {
			return true
		}
	}
	return false
}

func (o *SemOrder) isMessageStableLocal(m *pb.Envelope) bool {
	st := &o.state
	id := m.GetId()
	senderBytes := id.GetSender().GetValue()
	senderSlot := st.membership.SlotOf(id.GetSender())
	if senderSlot < 0 {
		return false
	}
	senderSeq, err := st.membership.SenderSeq(id)
	if err != nil {
		return false
	}
	for slot := 0; slot < st.membership.Len(); slot++ {
		if slot == senderSlot {
			continue
		}
		if st.membership.IsFrozen(slot) {
			continue
		}
		r := st.membership.Replica(slot)
		if bytes.Equal(r.GetValue(), senderBytes) {
			continue
		}
		latest, present := st.latestVector[string(r.GetValue())]
		if !present {
			return false
		}
		if uint64Slot(latest, senderSlot) < senderSeq {
			return false
		}
	}
	return true
}

// continuationProperty is the §3 gate: every active replica has at
// least one observed message at currentWave or beyond. Without
// this we can't be sure no MORE class-≥2 ops will land in
// currentWave.
func (o *SemOrder) continuationProperty() bool {
	st := &o.state
	for _, r := range st.membership.Replicas() {
		latest, present := st.latestVector[string(r.GetValue())]
		if !present {
			return false
		}
		if waveOf(latest) < st.currentWave {
			return false
		}
	}
	return true
}

// predecessorsExecuted reports whether every causal predecessor of
// env has been applied. For the sender's own slot, the
// predecessor is sender's prior message at sender_seq-1. For
// every other slot i with vector[i] > 0, the predecessor is the
// message from participant i with seq vector[i].
func (o *SemOrder) predecessorsExecuted(env *pb.Envelope) bool {
	st := &o.state
	id := env.GetId()
	senderSlot := st.membership.SlotOf(id.GetSender())
	if senderSlot < 0 {
		return false
	}
	senderSeq, err := st.membership.SenderSeq(id)
	if err != nil {
		return false
	}
	vc := id.GetVectorClock()
	for slot, depSeq := range vc {
		if slot == senderSlot {
			if senderSeq <= 1 {
				continue
			}
			if !o.isExecutedKey(id.GetSender().GetValue(), senderSeq-1) {
				return false
			}
			continue
		}
		if depSeq == 0 {
			continue
		}
		r := st.membership.Replica(slot)
		if !o.isExecutedKey(r.GetValue(), depSeq) {
			return false
		}
	}
	return true
}

func (o *SemOrder) isExecuted(env *pb.Envelope) bool {
	id := env.GetId()
	senderSeq, err := o.state.membership.SenderSeq(id)
	if err != nil {
		return false
	}
	return o.isExecutedKey(id.GetSender().GetValue(), senderSeq)
}

func (o *SemOrder) isExecutedKey(sender []byte, seq uint64) bool {
	_, ok := o.state.executed[indexKey{sender: string(sender), seq: seq}]
	return ok
}

func (o *SemOrder) isClass1(env *pb.Envelope) bool {
	return o.classifier.ClassOf(env.GetPayload()) == 1
}

func (o *SemOrder) applyEnv(env *pb.Envelope) {
	id := env.GetId()
	senderSeq, err := o.state.membership.SenderSeq(id)
	if err != nil {
		return
	}
	o.state.executed[indexKey{sender: string(id.GetSender().GetValue()), seq: senderSeq}] = struct{}{}
	d := psync.Delivery{Envelope: env}
	select {
	case o.apply <- Applied{Delivery: d}:
	case <-o.stopped:
	}
}

// ─── small helpers ────────────────────────────────────────────────

func waveOf(v []uint64) uint64 {
	var m uint64
	for _, x := range v {
		if x > m {
			m = x
		}
	}
	return m
}

func uint64Slot(v []uint64, slot int) uint64 {
	if slot >= 0 && slot < len(v) {
		return v[slot]
	}
	return 0
}

// dominatesOrEqual reports whether a >= b component-wise.
func dominatesOrEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] < b[i] {
			return false
		}
	}
	return true
}
