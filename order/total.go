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

// Total emits messages in a deterministic total order — every
// replica sees the same sequence.
//
// Algorithm: buffer messages by wave number (psync.Node.Wave =
// max(vector_clock)). When the next-pending wave becomes wave-
// complete (some message in that wave is stable per psync §2.3),
// sort that wave's messages by sender ReplicaID byte order and
// emit each. Then advance to the next wave and repeat.
//
// Sender-byte sort is the deterministic tiebreaker — all replicas
// agree on the same order because the membership view is sorted
// the same way at every replica (PLAN §2.10.1).
//
// This is the §2.3 / [26] approach: Total leverages psync's
// partial order (waves are causally ordered by construction) and
// uses the deterministic intra-wave sort for the tiebreak.
type Total struct {
	conv     *psync.Conversation
	apply    chan Applied
	nextWave uint64
	stopOnce sync.Once
	stopped  chan struct{}
}

// NewTotal constructs a Total order bound to conv.
func NewTotal(conv *psync.Conversation) *Total {
	t := &Total{
		conv:     conv,
		apply:    make(chan Applied, 256),
		nextWave: 1, // wave numbers start at 1 (waveOf = max(vector); first sends increment a slot to 1)
		stopped:  make(chan struct{}),
	}
	go t.pump()
	return t
}

func (t *Total) pump() {
	defer close(t.apply)
	for {
		select {
		case _, ok := <-t.conv.Recv():
			if !ok {
				return
			}
			// A new delivery may have advanced wave completion. Try
			// to drain as many newly-complete waves as possible.
			t.drainCompleteWaves()
		case <-t.stopped:
			return
		}
	}
}

// drainCompleteWaves emits every consecutively-complete wave
// starting at nextWave. Stops when the next pending wave is not
// yet complete.
func (t *Total) drainCompleteWaves() {
	for t.conv.WaveComplete(t.nextWave) {
		envs := t.conv.MessagesInWave(t.nextWave)
		sort.Slice(envs, func(i, j int) bool {
			return bytes.Compare(
				envs[i].GetId().GetSender().GetValue(),
				envs[j].GetId().GetSender().GetValue(),
			) < 0
		})
		for _, env := range envs {
			d := psync.Delivery{Envelope: env}
			select {
			case t.apply <- Applied{Delivery: d}:
			case <-t.stopped:
				return
			}
		}
		t.nextWave++
	}
}

// Apply implements Order.
func (t *Total) Apply() <-chan Applied { return t.apply }

// Close stops the pump.
func (t *Total) Close() error {
	t.stopOnce.Do(func() { close(t.stopped) })
	return nil
}

// Compile-time assertion that pb is referenced (used for symmetry
// with future SemOrder; remove when SemOrder lands).
var _ = (*pb.Envelope)(nil)
