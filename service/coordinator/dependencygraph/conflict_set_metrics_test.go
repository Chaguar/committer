/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/utils/monitoring"
	"github.com/Chaguar/committer/utils/test"
)

const (
	activeConflictSetsMetric   = "coordinator_global_dependency_graph_active_conflict_sets"
	oldestConflictSetAgeMetric = "coordinator_global_dependency_graph_oldest_active_conflict_set_age_seconds"
)

func TestConflictSetActiveMetrics(t *testing.T) {
	t.Parallel()
	metrics := newPerformanceMetrics(monitoring.NewProvider(), &managerQueues{})
	dm := newConflictSetDependencyManager(&conflictSetDepConfig{metrics: metrics, waitingTxsLimit: 10})
	require.Zero(t, conflictSetMetricValue(t, metrics, activeConflictSetsMetric))
	require.Zero(t, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric))
	keys := makeTestKeys(t, 3)
	a := createTxNode(t, nil, [][]byte{keys[0], keys[1]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	c := createTxNode(t, nil, [][]byte{keys[2]}, nil)
	b.addDependenciesAndUpdateDependents(TxNodeBatch{a})

	// Step 1: Count sets at emission, even before any output channel accepts them.
	t.Log("Step 1: Emit two sets without delivering output")
	dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{a, b, c}))
	require.InDelta(t, 2, conflictSetMetricValue(t, metrics, activeConflictSetsMetric), 0)
	age := conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric)
	require.Greater(t, age, float64(0))
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		require.Greater(ct, conflictSetMetricValue(ct, metrics, oldestConflictSetAgeMetric), age)
	}, time.Second, time.Millisecond)

	// Step 2: Neither partial feedback nor a waiting dependent set resets the age.
	t.Log("Step 2: Report one member and queue a dependent set")
	x := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{x}))
	dm.markValidated(TxNodeBatch{a, a, x})
	require.InDelta(t, 2, conflictSetMetricValue(t, metrics, activeConflictSetsMetric), 0)
	require.Greater(t, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric), age)

	// Step 3: Completing one set emits its dependent, while the other set remains active.
	t.Log("Step 3: Complete prerequisite and release dependent")
	dm.markValidated(TxNodeBatch{b})
	require.InDelta(t, 2, conflictSetMetricValue(t, metrics, activeConflictSetsMetric), 0)
	dm.markValidated(TxNodeBatch{c})
	require.InDelta(t, 1, conflictSetMetricValue(t, metrics, activeConflictSetsMetric), 0)

	// Step 4: Completion and duplicate feedback leave no retained sets or stale age.
	t.Log("Step 4: Finish every set")
	dm.markValidated(TxNodeBatch{x, a, b, c, x})
	require.Zero(t, conflictSetMetricValue(t, metrics, activeConflictSetsMetric))
	require.Zero(t, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric))
}

func TestConflictSetOldestAgeTracksRemainingSets(t *testing.T) {
	t.Parallel()
	metrics := newPerformanceMetrics(monitoring.NewProvider(), &managerQueues{})
	dm := newConflictSetDependencyManager(&conflictSetDepConfig{metrics: metrics, waitingTxsLimit: 10})
	keys := makeTestKeys(t, 2)
	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	dm.addBatch(createTxsNodeBatch(t, TxNodeBatch{a, b}))
	// Backdate real emission records instead of sleeping or adding a production clock hook.
	now := time.Now()
	dm.active[a.conflictSet] = now.Add(-2 * time.Hour)
	dm.active[b.conflictSet] = now.Add(-time.Hour)
	require.InDelta(t, 7200, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric), 30)

	dm.markValidated(TxNodeBatch{a})
	require.InDelta(t, 3600, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric), 30)
	dm.markValidated(TxNodeBatch{b})
	require.Zero(t, conflictSetMetricValue(t, metrics, oldestConflictSetAgeMetric))
}

// conflictSetMetricValue checks registration as well as the value exposed by a real scrape.
func conflictSetMetricValue(t test.TestingT, metrics *perfMetrics, name string) float64 {
	t.Helper()
	families, err := metrics.provider.Registry().Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.Metric, 1)
			require.NotNil(t, family.Metric[0].Gauge)
			return family.Metric[0].Gauge.GetValue()
		}
	}
	require.FailNow(t, "metric is not registered", name)
	return 0
}
