/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"context"
	"testing"
	"time"

	"github.com/Chaguar/common/api/committerpb"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/utils/channel"
	"github.com/Chaguar/committer/utils/monitoring"
	"github.com/Chaguar/committer/utils/test"
)

type conflictSetTestEnv struct {
	incomingTxs  chan *transactionNodeBatch
	outgoing     chan []TxNodeBatch
	validatedTxs chan TxNodeBatch
	metrics      *perfMetrics
	dm           *conflictSetDependencyManager
}

func newConflictSetTestEnv(t *testing.T, waitingTxsLimit int) *conflictSetTestEnv {
	t.Helper()
	incomingTxs := make(chan *transactionNodeBatch, 10)
	env := &conflictSetTestEnv{
		incomingTxs:  incomingTxs,
		outgoing:     make(chan []TxNodeBatch, 10),
		validatedTxs: make(chan TxNodeBatch, 10),
		metrics: newPerformanceMetrics(monitoring.NewProvider(), &managerQueues{
			gdgInput: incomingTxs,
		}),
	}
	env.dm = newConflictSetDependencyManager(&conflictSetDepConfig{
		incomingTxsNode:      env.incomingTxs,
		outgoingConflictSets: env.outgoing,
		validatedTxsNode:     env.validatedTxs,
		waitingTxsLimit:      waitingTxsLimit,
		metrics:              env.metrics,
	})
	test.RunServiceForTest(t.Context(), t, func(ctx context.Context) error {
		env.dm.run(ctx)
		return nil
	}, nil)
	return env
}

func TestConflictSetIndependentTxs(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 10)
	keys := makeTestKeys(t, 3)

	t1 := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	t2 := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	t3 := createTxNode(t, nil, [][]byte{keys[2]}, nil)
	// Step 1: Emit independent transactions as separate sets.
	t.Log("Step 1: Emit independent transactions as separate sets")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{t1, t2, t3})

	require.Equal(t, []TxNodeBatch{
		{t1},
		{t2},
		{t3},
	}, readConflictSets(t, env.outgoing))

	// Step 2: Validate every set and verify graph drain.
	t.Log("Step 2: Validate every set and verify graph drain")
	env.validatedTxs <- TxNodeBatch{t1, t2, t3}
	ensureProcessedAndValidatedMetrics(t, env.metrics, 3, 3)
	ensureEmptyDetector(t, env.dm.detector, &env.dm.mu)
	ensureWaitingTXsLimit(t, env.metrics, 0)
}

func TestConflictSetLocalChainsAndJoin(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 4)

	// Chain: a <- b. Join: c, d <- e.
	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	c := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	d := createTxNode(t, nil, [][]byte{keys[2]}, nil)
	e := createTxNode(t, nil, [][]byte{keys[1], keys[2]}, nil)
	b.addDependenciesAndUpdateDependents(TxNodeBatch{a})
	e.addDependenciesAndUpdateDependents(TxNodeBatch{c, d})
	// Step 1: Emit the local chain and joined roots.
	t.Log("Step 1: Emit the local chain and joined roots")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, b, c, d, e})

	conflictSets := readConflictSets(t, env.outgoing)
	require.Len(t, conflictSets, 2)
	requireConflictSet(t, conflictSets[0], a, b)
	requireConflictSet(t, conflictSets[1], c, d, e)

	// Step 2: Validate all members and release graph slots.
	t.Log("Step 2: Validate all members and release graph slots")
	env.validatedTxs <- TxNodeBatch{a, b, c, d, e}
	ensureEmptyDetector(t, env.dm.detector, &env.dm.mu)
	ensureWaitingTXsLimit(t, env.metrics, 0)
}

func TestConflictSetWaitsForWholeInFlightConflictSet(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 3)

	// a <- b. x conflicts only with a (keys[2]).
	a := createTxNode(t, nil, [][]byte{keys[0], keys[2]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b.addDependenciesAndUpdateDependents(TxNodeBatch{a})
	// Step 1: Emit a two-member prerequisite set.
	t.Log("Step 1: Emit a two-member prerequisite set")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, b})
	conflictSets := readConflictSets(t, env.outgoing)
	require.Len(t, conflictSets, 1)
	requireConflictSet(t, conflictSets[0], a, b)

	// Step 2: Report one member and submit a conflicting transaction.
	t.Log("Step 2: Report one member and submit a conflicting transaction")
	env.validatedTxs <- TxNodeBatch{a}

	x := createTxNode(t, nil, [][]byte{keys[2]}, nil)
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	// x conflicts only with a, which is already validated, but its conflict set is still in flight.
	requireNoConflictSets(t, env.outgoing)

	// Step 3: Complete the prerequisite set and release its dependent.
	t.Log("Step 3: Complete the prerequisite set and release its dependent")
	env.validatedTxs <- TxNodeBatch{b}
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))
	// A dependency on an in-flight conflict set stores no dependency edge.
	require.Empty(t, x.dependsOnTxs)

	// Step 4: Validate the dependent and drain the detector.
	t.Log("Step 4: Validate the dependent and drain the detector")
	env.validatedTxs <- TxNodeBatch{x}
	ensureEmptyDetector(t, env.dm.detector, &env.dm.mu)
}

func TestConflictSetCountsTxWaitingOnlyOnInFlightConflictSet(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 1)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 1: Emit the prerequisite transaction.
	t.Log("Step 1: Emit the prerequisite transaction")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a})
	require.Equal(t, []TxNodeBatch{{a}}, readConflictSets(t, env.outgoing))

	// x depends only on the in-flight conflict set of a, so it waits on a dependency.
	x := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 2: Count a transaction waiting only on an emitted set.
	t.Log("Step 2: Count a transaction waiting only on an emitted set")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	test.EventuallyIntMetric(t, 1, env.metrics.dependentTxCount, 2*time.Second, 10*time.Millisecond)

	// Step 3: Release the dependent and decrement its gauge.
	t.Log("Step 3: Release the dependent and decrement its gauge")
	env.validatedTxs <- TxNodeBatch{a}
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))
	test.EventuallyIntMetric(t, 0, env.metrics.dependentTxCount, 2*time.Second, 10*time.Millisecond)
}

func TestConflictSetWaitsForAllDependencies(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 2)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	c := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	// Step 1: Emit two independent prerequisites.
	t.Log("Step 1: Emit two independent prerequisites")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, c})
	require.Equal(t, []TxNodeBatch{
		{a},
		{c},
	}, readConflictSets(t, env.outgoing))

	x := createTxNode(t, nil, [][]byte{keys[0], keys[1]}, nil)
	// Step 2: Queue a transaction depending on both sets.
	t.Log("Step 2: Queue a transaction depending on both sets")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	requireNoConflictSets(t, env.outgoing)

	// Step 3: Complete one prerequisite and keep the dependent blocked.
	t.Log("Step 3: Complete one prerequisite and keep the dependent blocked")
	env.validatedTxs <- TxNodeBatch{a}
	requireNoConflictSets(t, env.outgoing)

	// Step 4: Complete the final prerequisite and emit the dependent.
	t.Log("Step 4: Complete the final prerequisite and emit the dependent")
	env.validatedTxs <- TxNodeBatch{c}
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))
}

func TestConflictSetJoinsDependentWaitingConflictSet(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 3)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 1: Emit the prerequisite set.
	t.Log("Step 1: Emit the prerequisite set")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a})
	require.Equal(t, []TxNodeBatch{{a}}, readConflictSets(t, env.outgoing))

	// x waits for a. y depends only on the waiting x, across batches.
	x := createTxNode(t, nil, [][]byte{keys[0], keys[1]}, nil)
	// Step 2: Join waiting transactions across batches.
	t.Log("Step 2: Join waiting transactions across batches")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	y := createTxNode(t, nil, [][]byte{keys[1], keys[2]}, nil)
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{y})
	requireNoConflictSets(t, env.outgoing)
	env.dm.mu.Lock()
	require.Equal(t, TxNodeBatch{x}, y.dependsOnTxs)
	env.dm.mu.Unlock()

	// Step 3: Complete the prerequisite and emit the joined set.
	t.Log("Step 3: Complete the prerequisite and emit the joined set")
	env.validatedTxs <- TxNodeBatch{a}
	conflictSets := readConflictSets(t, env.outgoing)
	require.Len(t, conflictSets, 1)
	requireConflictSet(t, conflictSets[0], x, y)
}

func TestConflictSetMergeOfDependentWaitingConflictSets(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 4)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	// Step 1: Emit independent prerequisite sets.
	t.Log("Step 1: Emit independent prerequisite sets")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, b})
	require.Equal(t, []TxNodeBatch{
		{a},
		{b},
	}, readConflictSets(t, env.outgoing))

	// x waits for a; y waits for b. They are separate waiting conflict sets.
	x := createTxNode(t, nil, [][]byte{keys[0], keys[2]}, nil)
	y := createTxNode(t, nil, [][]byte{keys[1], keys[3]}, nil)
	// Step 2: Queue separate dependent sets.
	t.Log("Step 2: Queue separate dependent sets")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x, y})
	requireNoConflictSets(t, env.outgoing)

	// z merges both waiting conflict sets; the merged conflict set waits for a and b.
	z := createTxNode(t, nil, [][]byte{keys[2], keys[3]}, nil)
	// Step 3: Merge the waiting sets through a joining transaction.
	t.Log("Step 3: Merge the waiting sets through a joining transaction")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{z})
	requireNoConflictSets(t, env.outgoing)

	// Step 4: Complete one prerequisite without releasing the merged set.
	t.Log("Step 4: Complete one prerequisite without releasing the merged set")
	env.validatedTxs <- TxNodeBatch{a}
	requireNoConflictSets(t, env.outgoing)

	// Step 5: Complete the last prerequisite and emit exactly one set.
	t.Log("Step 5: Complete the last prerequisite and emit exactly one set")
	env.validatedTxs <- TxNodeBatch{b}
	conflictSets := readConflictSets(t, env.outgoing)
	require.Len(t, conflictSets, 1)
	requireConflictSet(t, conflictSets[0], x, y, z)
	requireNoConflictSets(t, env.outgoing)
}

func TestConflictSetMergePreservesArrivalOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		prerequisites []int
	}{
		{name: "forward dependencies", prerequisites: []int{0, 1}},
		{name: "reverse dependencies", prerequisites: []int{1, 0}},
		{name: "global dependencies across batches"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metrics := newPerformanceMetrics(monitoring.NewProvider(), &managerQueues{})
			dm := newConflictSetDependencyManager(&conflictSetDepConfig{metrics: metrics, waitingTxsLimit: 10})
			keys := makeTestKeys(t, 3)
			a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
			// Step 1: Emit the common prerequisite.
			t.Log("Step 1: Emit the common prerequisite")
			dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{a}))
			dm.ready = nil

			// Both roots wait for a; the join's dependency order must not affect output.
			x := createTxNode(t, [][]byte{keys[0]}, [][]byte{keys[1]}, nil)
			y := createTxNode(t, [][]byte{keys[0]}, [][]byte{keys[2]}, nil)
			z := createTxNode(t, nil, [][]byte{keys[1], keys[2]}, nil)
			roots := TxNodeBatch{x, y}
			for _, i := range tc.prerequisites {
				z.addDependenciesAndUpdateDependents(TxNodeBatch{roots[i]})
			}
			// Step 2: Build waiting roots and join them despite dependency-order differences.
			t.Log("Step 2: Build waiting roots and join them despite dependency-order differences")
			if len(tc.prerequisites) == 0 {
				dm.addBatch(createTxsNodeBatch(t, roots))
				dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{z}))
			} else {
				dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{x, y, z}))
			}
			require.Empty(t, dm.ready)

			// Step 3: Release the merged set in original arrival order.
			t.Log("Step 3: Release the merged set in original arrival order")
			dm.markValidated(TxNodeBatch{a})
			require.Len(t, dm.ready, 1)
			requireConflictSet(t, dm.ready[0], x, y, z)
		})
	}
}

func TestConflictSetWaitingTxsLimit(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 2)
	keys := makeTestKeys(t, 3)

	t1 := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	t2 := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	// Step 1: Fill all waiting slots.
	t.Log("Step 1: Fill all waiting slots")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{t1, t2})
	require.Equal(t, []TxNodeBatch{
		{t1},
		{t2},
	}, readConflictSets(t, env.outgoing))

	t3 := createTxNode(t, nil, [][]byte{keys[2]}, nil)
	// Step 2: Verify new work waits for a slot.
	t.Log("Step 2: Verify new work waits for a slot")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{t3})
	requireNoConflictSets(t, env.outgoing)
	ensureWaitingTXsLimit(t, env.metrics, 2)

	// Step 3: Release slots and admit the waiting transaction.
	t.Log("Step 3: Release slots and admit the waiting transaction")
	env.validatedTxs <- TxNodeBatch{t1, t2}
	require.Equal(t, []TxNodeBatch{{t3}}, readConflictSets(t, env.outgoing))
}

func TestConflictSetIgnoresInvalidFeedback(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 1)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b.addDependenciesAndUpdateDependents(TxNodeBatch{a})
	// Step 1: Emit a multi-member prerequisite set.
	t.Log("Step 1: Emit a multi-member prerequisite set")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, b})
	require.Len(t, readConflictSets(t, env.outgoing), 1)

	x := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 2: Queue a dependent transaction.
	t.Log("Step 2: Queue a dependent transaction")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	requireNoConflictSets(t, env.outgoing)

	rejected := NewRejectedTransactionNode(&committerpb.TxStatus{
		Ref:    committerpb.NewTxRef("rejected", 0, 0),
		Status: committerpb.Status_MALFORMED_UNSUPPORTED_ENVELOPE_PAYLOAD,
	})
	// Rejected node, duplicate of a, and x (still waiting) must all be ignored.
	// Step 3: Ignore rejected, duplicate, and waiting feedback.
	t.Log("Step 3: Ignore rejected, duplicate, and waiting feedback")
	env.validatedTxs <- TxNodeBatch{rejected, a, a, x}
	test.EventuallyIntMetric(t, 1, env.metrics.gdgValidatedTxProcessedTotal, 2*time.Second, 10*time.Millisecond)
	test.RequireIntMetricValue(t, 3, env.metrics.gdgWaitingTxCount)
	requireNoConflictSets(t, env.outgoing)

	// Step 4: Complete the prerequisite and validate the released dependent.
	t.Log("Step 4: Complete the prerequisite and validate the released dependent")
	env.validatedTxs <- TxNodeBatch{b}
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))
	test.EventuallyIntMetric(t, 2, env.metrics.gdgValidatedTxProcessedTotal, 2*time.Second, 10*time.Millisecond)
	env.validatedTxs <- TxNodeBatch{x}
	ensureProcessedAndValidatedMetrics(t, env.metrics, 3, 3)
}

func TestConflictSetOutputMutationDoesNotAffectManager(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 1)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b.addDependenciesAndUpdateDependents(TxNodeBatch{a})
	// Step 1: Receive consumer-owned output slices.
	t.Log("Step 1: Receive consumer-owned output slices")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a, b})
	conflictSets := readConflictSets(t, env.outgoing)
	require.Len(t, conflictSets, 1)

	// The consumer owns each conflict set's transaction slice and the outer slice.
	// Step 2: Mutate output slices and validate the original members.
	t.Log("Step 2: Mutate output slices and validate the original members")
	conflictSets[0][0], conflictSets[0][1] = nil, nil
	conflictSets[0] = append(conflictSets[0], nil)
	conflictSets[0] = nil
	test.EventuallyIntMetric(t, 0, env.metrics.gdgDepFreeTxCount, 2*time.Second, time.Millisecond)

	env.validatedTxs <- TxNodeBatch{a, b}
	ensureEmptyDetector(t, env.dm.detector, &env.dm.mu)

	x := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 3: Verify subsequent conflicts do not see stale detector entries.
	t.Log("Step 3: Verify subsequent conflicts do not see stale detector entries")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))
}

func TestConflictSetCompletedConflictSetReleasesReferences(t *testing.T) {
	t.Parallel()
	env := newConflictSetTestEnv(t, 20)
	keys := makeTestKeys(t, 1)

	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 1: Emit a prerequisite set.
	t.Log("Step 1: Emit a prerequisite set")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{a})
	require.Equal(t, []TxNodeBatch{{a}}, readConflictSets(t, env.outgoing))

	x := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	// Step 2: Queue its dependent.
	t.Log("Step 2: Queue its dependent")
	env.incomingTxs <- createTxsNodeBatch(t, TxNodeBatch{x})
	requireNoConflictSets(t, env.outgoing)

	// Step 3: Complete the prerequisite and detach its members.
	t.Log("Step 3: Complete the prerequisite and detach its members")
	env.validatedTxs <- TxNodeBatch{a}
	require.Equal(t, []TxNodeBatch{{x}}, readConflictSets(t, env.outgoing))

	// A completed conflict set must not keep newer conflict sets reachable.
	env.dm.mu.Lock()
	defer env.dm.mu.Unlock()
	require.Nil(t, a.conflictSet)
}

func TestConflictSetOutputBacklogMetrics(t *testing.T) {
	t.Parallel()
	incoming := make(chan *transactionNodeBatch, 2)
	outgoing := make(chan []TxNodeBatch)
	validated := make(chan TxNodeBatch, 2)
	metrics := newPerformanceMetrics(monitoring.NewProvider(), &managerQueues{gdgInput: incoming})
	dm := newConflictSetDependencyManager(&conflictSetDepConfig{
		incomingTxsNode:      incoming,
		outgoingConflictSets: outgoing,
		validatedTxsNode:     validated,
		waitingTxsLimit:      10,
		metrics:              metrics,
	})
	test.RunServiceForTest(t.Context(), t, func(ctx context.Context) error {
		dm.run(ctx)
		return nil
	}, nil)
	keys := makeTestKeys(t, 2)
	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)

	// Step 1: Block the first write, so its transactions remain in the backlog.
	t.Log("Step 1: Block output")
	incoming <- createTxsNodeBatch(t, TxNodeBatch{a})
	test.EventuallyIntMetric(t, 1, metrics.gdgTxProcessedTotal, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		dm.mu.Lock()
		defer dm.mu.Unlock()
		return len(dm.ready) == 0
	}, 2*time.Second, time.Millisecond)
	test.RequireIntMetricValue(t, 1, metrics.gdgDepFreeTxCount)

	// Step 2: Construction still proceeds while the output writer is blocked.
	t.Log("Step 2: Queue another set")
	incoming <- createTxsNodeBatch(t, TxNodeBatch{b})
	test.EventuallyIntMetric(t, 2, metrics.gdgDepFreeTxCount, 2*time.Second, time.Millisecond)

	// Step 3: Successful writes drain the backlog, then feedback frees graph slots.
	t.Log("Step 3: Drain output and validate")
	require.Equal(t, []TxNodeBatch{{a}}, <-outgoing)
	require.Equal(t, []TxNodeBatch{{b}}, <-outgoing)
	test.EventuallyIntMetric(t, 0, metrics.gdgDepFreeTxCount, 2*time.Second, time.Millisecond)
	validated <- TxNodeBatch{a, b}
	ensureProcessedAndValidatedMetrics(t, metrics, 2, 2)
	require.Greater(t, test.GetMetricValue(t, metrics.gdgConstructorWaitForLockSeconds), float64(0))
	require.Greater(t, test.GetMetricValue(t, metrics.gdgValidatedTxProcessorWaitForLockSeconds), float64(0))
}

func TestConflictSetManagerEndToEnd(t *testing.T) {
	t.Parallel()
	incomingTxs := make(chan *TransactionBatch, 10)
	outgoing := make(chan []TxNodeBatch, 10)
	validatedTxs := make(chan TxNodeBatch, 10)
	m := NewConflictSetManager(&ConflictSetParameters{
		IncomingTxs:                 incomingTxs,
		OutgoingDepFreeConflictSets: outgoing,
		IncomingValidatedTxsNode:    validatedTxs,
		NumOfLocalDepConstructors:   2,
		WaitingTxsLimit:             20,
		PrometheusMetricsProvider:   monitoring.NewProvider(),
	})
	test.RunServiceForTest(t.Context(), t, func(ctx context.Context) error {
		m.Run(ctx)
		return nil
	}, nil)

	keys := makeTestKeys(t, 4)
	// Block and tx numbers are deliberately all zero: ordering must not rely on them.
	// The first transaction dominates three mutually independent transactions.
	batch1 := []*servicepb.TxWithRef{
		createTxForTest(t, 0, nsID1ForTest, nil, [][]byte{keys[0], keys[1], keys[2]}, nil),
		createTxForTest(t, 0, nsID1ForTest, nil, [][]byte{keys[0]}, nil),
		createTxForTest(t, 0, nsID1ForTest, nil, [][]byte{keys[1]}, nil),
		createTxForTest(t, 0, nsID1ForTest, nil, [][]byte{keys[2]}, nil),
		createTxForTest(t, 0, nsID1ForTest, nil, [][]byte{keys[3]}, nil),
	}
	// Step 1: Emit the first batch without using block or transaction numbers.
	t.Log("Step 1: Emit the first batch without using block or transaction numbers")
	incomingTxs <- &TransactionBatch{ID: 1, Txs: batch1}

	conflictSets := readConflictSets(t, outgoing)
	require.Len(t, conflictSets, 2)
	require.Len(t, conflictSets[0], 4)
	require.Len(t, conflictSets[1], 1)
	require.Same(t, batch1[0], conflictSets[0][0].VerifierTx)
	for i, tx := range conflictSets[0] {
		require.Same(t, batch1[i], tx.VerifierTx)
	}
	require.Same(t, batch1[4], conflictSets[1][0].VerifierTx)

	// Depends on the in-flight first conflict set through keys[1].
	batch2 := []*servicepb.TxWithRef{
		createTxForTest(t, 0, nsID1ForTest, [][]byte{keys[1]}, nil, nil),
	}
	// Step 2: Queue a cross-batch dependent.
	t.Log("Step 2: Queue a cross-batch dependent")
	incomingTxs <- &TransactionBatch{ID: 2, Txs: batch2}
	requireNoConflictSets(t, outgoing)

	// Step 3: Report shuffled partial feedback without releasing the dependent.
	t.Log("Step 3: Report shuffled partial feedback without releasing the dependent")
	validatedTxs <- conflictSets[0][:1]
	// Members may report out of order; partial feedback cannot release the conflict set.
	validatedTxs <- TxNodeBatch{conflictSets[0][3], conflictSets[0][1]}
	requireNoConflictSets(t, outgoing)
	// Step 4: Complete the set and release the next batch.
	t.Log("Step 4: Complete the set and release the next batch")
	validatedTxs <- TxNodeBatch{conflictSets[0][2]}
	conflictSets2 := readConflictSets(t, outgoing)
	require.Len(t, conflictSets2, 1)
	require.Len(t, conflictSets2[0], 1)
	require.Same(t, batch2[0], conflictSets2[0][0].VerifierTx)
}

// readConflictSets bounds the wait for the next output write.
func readConflictSets(t *testing.T, outgoing <-chan []TxNodeBatch) []TxNodeBatch {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	sets, ok := channel.NewReader(ctx, outgoing).Read()
	require.True(t, ok, "expected conflict sets")
	return sets
}

// requireNoConflictSets asserts that nothing is emitted.
func requireNoConflictSets(t *testing.T, outgoing <-chan []TxNodeBatch) {
	t.Helper()
	require.Never(t, func() bool {
		return len(outgoing) > 0
	}, 500*time.Millisecond, 50*time.Millisecond)
}
