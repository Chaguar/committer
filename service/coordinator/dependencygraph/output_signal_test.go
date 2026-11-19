/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"context"
	"testing"
	"time"
)

func TestOutputSignalBeforeWait(t *testing.T) {
	t.Parallel()
	signal := newOutputSignal()
	signal.signal()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if !signal.wait(ctx) {
		t.Fatal("signal before wait was lost")
	}
}

func TestOutputSignalCoalesces(t *testing.T) {
	t.Parallel()
	signal := newOutputSignal()
	for range 100 {
		signal.signal()
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if !signal.wait(ctx) {
		t.Fatal("pending signal was lost")
	}
	signal.cond.L.Lock()
	pending := signal.pending
	signal.cond.L.Unlock()
	if pending {
		t.Fatal("coalesced signal was not consumed")
	}
}

func TestOutputSignalRepeatedWakeups(t *testing.T) {
	t.Parallel()
	signal := newOutputSignal()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 100 {
		done := make(chan bool, 1)
		go func() {
			done <- signal.wait(ctx)
		}()
		signal.signal()
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("wake-up was lost")
			}
		case <-ctx.Done():
			t.Fatal("wait did not return")
		}
	}
}

func TestOutputSignalCancellation(t *testing.T) {
	t.Parallel()
	for range 100 {
		signal := newOutputSignal()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan bool, 1)
		go func() {
			done <- signal.wait(ctx)
		}()
		cancel()
		select {
		case ok := <-done:
			if ok {
				t.Fatal("cancelled wait reported a signal")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not wake waiter")
		}
	}
}

func TestOutputSignalWaitingCancellation(t *testing.T) {
	t.Parallel()
	signal := newOutputSignal()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		done <- signal.wait(ctx)
	}()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("wait returned without a signal")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting cancellation did not wake waiter")
	}
}

func TestOutputSignalCancelledWithPendingSignal(t *testing.T) {
	t.Parallel()
	signal := newOutputSignal()
	signal.signal()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if signal.wait(ctx) {
		t.Fatal("cancelled wait consumed pending signal")
	}
}
