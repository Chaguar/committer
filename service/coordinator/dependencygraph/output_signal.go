/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"context"
	"sync"
)

// outputSignal coalesces wake-ups for one output goroutine. Work stays in the
// manager-owned queue; cond.L protects only pending, not the queue.
type outputSignal struct {
	cond    *sync.Cond
	pending bool
}

func newOutputSignal() *outputSignal {
	return &outputSignal{cond: sync.NewCond(&sync.Mutex{})}
}

// signal records a wake-up even when the output goroutine is not waiting yet.
func (s *outputSignal) signal() {
	s.cond.L.Lock()
	s.pending = true
	s.cond.Signal()
	s.cond.L.Unlock()
}

// wait consumes one coalesced wake-up, or returns false on cancellation.
func (s *outputSignal) wait(ctx context.Context) bool {
	stop := context.AfterFunc(ctx, s.signal)
	defer stop()

	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	for !s.pending && ctx.Err() == nil {
		s.cond.Wait()
	}
	if ctx.Err() != nil {
		return false
	}
	s.pending = false
	return true
}
