/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/utils/channel"
	"github.com/Chaguar/committer/utils/monitoring"
	"github.com/Chaguar/committer/utils/test"
)

type conflictSetInvariantEnv struct {
	txs       []*servicepb.TxWithRef
	access    [][]int
	index     map[*servicepb.TxWithRef]int
	seen      []bool
	group     []int
	remaining []int
	pending   TxNodeBatch
	reported  int
}

func TestConflictSetRandomizedInvariants(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		keyCount     int
		writePercent int
	}{
		{name: "independent", keyCount: 64, writePercent: 100},
		{name: "read only", keyCount: 8},
		{name: "hot key", keyCount: 1, writePercent: 75},
		{name: "read heavy", keyCount: 8, writePercent: 25},
		{name: "mixed", keyCount: 8, writePercent: 60},
	} {
		for _, seed := range []uint64{1, 17, 42} {
			for _, limit := range []int{1, 8, 40} {
				t.Run(fmt.Sprintf("%s/seed-%d/limit-%d", tc.name, seed, limit), func(t *testing.T) {
					t.Parallel()
					rng := rand.New(rand.NewPCG(seed, seed+1))
					env := newConflictSetInvariantEnv(t, rng, tc.keyCount, tc.writePercent)
					env.run(t, rng, limit)
				})
			}
		}
	}
}

func newConflictSetInvariantEnv(t *testing.T, rng *rand.Rand, keyCount, writePercent int) *conflictSetInvariantEnv {
	t.Helper()
	const txCount = 64
	keys := makeTestKeys(t, keyCount)
	env := &conflictSetInvariantEnv{
		txs:    make([]*servicepb.TxWithRef, txCount),
		access: make([][]int, txCount),
		index:  make(map[*servicepb.TxWithRef]int, txCount),
		seen:   make([]bool, txCount),
		group:  make([]int, txCount),
	}
	for i := range txCount {
		access := make([]int, keyCount)
		for range 1 + rng.IntN(min(3, keyCount)) {
			key := rng.IntN(keyCount)
			if keyCount == txCount {
				key = i
			}
			mode := 1 // read
			if rng.IntN(100) < writePercent {
				mode = 2 + rng.IntN(2) // write or read-write
			}
			access[key] |= mode
		}
		var reads, writes, readWrites [][]byte
		for key, mode := range access {
			switch mode {
			case 1:
				reads = append(reads, keys[key])
			case 2:
				writes = append(writes, keys[key])
			case 3:
				readWrites = append(readWrites, keys[key])
			default:
				continue
			}
		}
		tx := createTxForTest(t, 0, nsID1ForTest, reads, readWrites, writes)
		env.txs[i] = tx
		env.access[i] = access
		env.index[tx] = i
	}
	return env
}

func (env *conflictSetInvariantEnv) run(t *testing.T, rng *rand.Rand, limit int) {
	t.Helper()
	incoming := make(chan *TransactionBatch, 4)
	outgoing := make(chan []TxNodeBatch, 4)
	validated := make(chan TxNodeBatch)
	m := NewConflictSetManager(&ConflictSetParameters{
		IncomingTxs:                 incoming,
		OutgoingDepFreeConflictSets: outgoing,
		IncomingValidatedTxsNode:    validated,
		NumOfLocalDepConstructors:   3,
		WaitingTxsLimit:             limit,
		PrometheusMetricsProvider:   monitoring.NewProvider(),
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	test.RunServiceForTest(ctx, t, func(ctx context.Context) error {
		m.Run(ctx)
		return nil
	}, nil)

	// Step 1: Supply seeded batches while the real constructor pool runs concurrently.
	t.Log("Step 1: Supply seeded workload")
	batches := env.makeBatches(rng)
	test.RunServiceForTest(ctx, t, func(ctx context.Context) error {
		writer := channel.NewWriter(ctx, incoming)
		for _, batch := range batches {
			if !writer.Write(batch) {
				return nil
			}
		}
		return nil
	}, nil)

	// Step 2: Check output against an independent conflict oracle, then report random subsets.
	t.Log("Step 2: Check ordering and whole-set gates with shuffled partial feedback")
	reader := channel.NewReader(ctx, outgoing)
	writer := channel.NewWriter(ctx, validated)
	for env.reported < len(env.txs) {
		if len(env.pending) > 0 && len(outgoing) == 0 {
			feedback := env.takeFeedback(rng)
			require.True(t, writer.Write(feedback), "feedback stalled")
			env.recordFeedback(feedback)
			continue
		}
		sets, ok := reader.Read()
		require.True(t, ok, "output stalled before all transactions completed")
		for _, set := range sets {
			env.checkSet(t, set)
		}
	}

	// Step 3: Every transaction must finish exactly once and all retained work must drain.
	t.Log("Step 3: Verify completion and detector/gauge drain")
	metrics := m.conflictSetDepManager.metrics
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		require.Equal(ct, len(env.txs), test.GetIntMetricValue(ct, metrics.gdgTxProcessedTotal))
		require.Equal(ct, len(env.txs), test.GetIntMetricValue(ct, metrics.gdgValidatedTxProcessedTotal))
		require.Zero(ct, test.GetIntMetricValue(ct, metrics.gdgWaitingTxCount))
		require.Zero(ct, test.GetIntMetricValue(ct, metrics.gdgDepFreeTxCount))
		require.Zero(ct, test.GetIntMetricValue(ct, metrics.dependentTxCount))
		require.Zero(ct, test.GetIntMetricValue(ct, metrics.gdgActiveConflictSetCount))
		require.Zero(ct, test.GetMetricValue(ct, metrics.gdgOldestActiveConflictSetAgeSeconds))
	}, 2*time.Second, time.Millisecond)
	ensureEmptyDetector(t, m.conflictSetDepManager.detector, &m.conflictSetDepManager.mu)
	require.Empty(t, env.pending)
	for _, remaining := range env.remaining {
		require.Zero(t, remaining)
	}
	require.Empty(t, outgoing, "unexpected extra emission")
}

func (env *conflictSetInvariantEnv) makeBatches(rng *rand.Rand) []*TransactionBatch {
	var batches []*TransactionBatch
	for offset := 0; offset < len(env.txs); {
		size := 1 + rng.IntN(min(4, len(env.txs)-offset))
		batches = append(batches, &TransactionBatch{ID: uint64(len(batches) + 1), Txs: env.txs[offset : offset+size]})
		offset += size
	}
	return batches
}

// checkSet uses only input accesses and consumer feedback, never the manager's dependency edges.
func (env *conflictSetInvariantEnv) checkSet(t *testing.T, set TxNodeBatch) {
	t.Helper()
	require.NotEmpty(t, set)
	positions := make(map[int]int, len(set))
	previous := -1
	for position, tx := range set {
		index, exists := env.index[tx.VerifierTx]
		require.True(t, exists, "unknown emitted transaction")
		require.False(t, env.seen[index], "transaction %d emitted twice", index)
		require.Greater(t, index, previous, "set must preserve arrival order")
		positions[index] = position
		previous = index
	}
	for _, tx := range set {
		env.checkPrerequisites(t, positions, env.index[tx.VerifierTx])
	}
	group := len(env.remaining)
	env.remaining = append(env.remaining, len(set))
	for _, tx := range set {
		index := env.index[tx.VerifierTx]
		env.seen[index] = true
		env.group[index] = group
	}
	env.pending = append(env.pending, set...)
}

func (env *conflictSetInvariantEnv) checkPrerequisites(t *testing.T, positions map[int]int, index int) {
	t.Helper()
	for earlier := range index {
		if !env.conflicts(earlier, index) {
			continue
		}
		if position, sameSet := positions[earlier]; sameSet {
			require.Less(t, position, positions[index])
			continue
		}
		require.True(t, env.seen[earlier], "transaction %d emitted before conflicting transaction %d", index, earlier)
		require.Zero(t, env.remaining[env.group[earlier]],
			"transaction %d emitted before set of %d completed", index, earlier)
	}
}

func (env *conflictSetInvariantEnv) conflicts(a, b int) bool {
	for key, access := range env.access[a] {
		other := env.access[b][key]
		if access != 0 && other != 0 && (access&2 != 0 || other&2 != 0) {
			return true
		}
	}
	return false
}

func (env *conflictSetInvariantEnv) takeFeedback(rng *rand.Rand) TxNodeBatch {
	rng.Shuffle(len(env.pending), func(i, j int) { env.pending[i], env.pending[j] = env.pending[j], env.pending[i] })
	size := 1 + rng.IntN(min(4, len(env.pending)))
	feedback := slices.Clone(env.pending[:size])
	env.pending = env.pending[size:]
	return feedback
}

func (env *conflictSetInvariantEnv) recordFeedback(feedback TxNodeBatch) {
	for _, tx := range feedback {
		env.remaining[env.group[env.index[tx.VerifierTx]]]--
	}
	env.reported += len(feedback)
}
