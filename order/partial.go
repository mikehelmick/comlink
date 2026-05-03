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
	"sync"

	"github.com/mikehelmick/comlink/psync"
)

// PartialOrder is a passthrough Order: psync deliveries flow
// directly to Apply in causal-arrival order. Useful when the
// application's commands commute fully or already self-handle
// ordering.
type PartialOrder struct {
	apply    chan Applied
	stopOnce sync.Once
	stopped  chan struct{}
}

// NewPartial constructs a PartialOrder bound to conv. Pump
// goroutine starts immediately; the caller drives processing by
// reading Apply().
func NewPartial(conv *psync.Conversation) *PartialOrder {
	bufSize := 256
	o := &PartialOrder{
		apply:   make(chan Applied, bufSize),
		stopped: make(chan struct{}),
	}
	go o.pump(conv)
	return o
}

func (o *PartialOrder) pump(conv *psync.Conversation) {
	defer close(o.apply)
	for {
		select {
		case d, ok := <-conv.Recv():
			if !ok {
				return
			}
			select {
			case o.apply <- Applied{Delivery: d}:
			case <-o.stopped:
				return
			}
		case <-o.stopped:
			return
		}
	}
}

// Apply implements Order.
func (o *PartialOrder) Apply() <-chan Applied { return o.apply }

// Close stops the pump.
func (o *PartialOrder) Close() error {
	o.stopOnce.Do(func() { close(o.stopped) })
	return nil
}
