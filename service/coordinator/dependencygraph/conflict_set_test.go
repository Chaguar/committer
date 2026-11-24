/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConflictSetArrivalOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		deps [][]int
	}{
		{name: "fan out", deps: [][]int{nil, {0}, {0}, {0}}},
		{name: "diamond", deps: [][]int{nil, {0}, {0}, {1, 2}}},
		{name: "multiple roots", deps: [][]int{nil, nil, {0, 1}}},
		{name: "chain", deps: [][]int{nil, {0}, {1}}},
		{name: "uneven branches", deps: [][]int{nil, {0}, {0}, {1}, {2, 3}}},
		{name: "singleton", deps: [][]int{nil}},
		{name: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			keys := makeTestKeys(t, len(tc.deps))
			f := &conflictSet{}
			for i, deps := range tc.deps {
				tx := createTxNode(t, nil, [][]byte{keys[i]}, nil)
				tx.seq = uint64(i)
				for _, dep := range deps {
					tx.addDependenciesAndUpdateDependents(TxNodeBatch{f.txs[dep]})
				}
				f.txs = append(f.txs, tx)
			}

			want := slices.Clone(f.txs)
			// Membership order must not determine execution order.
			slices.Reverse(f.txs)
			got := f.arrivalOrder()
			requireConflictSet(t, got, want...)
			for i, tx := range want {
				require.Same(t, tx, f.txs[len(want)-1-i], "sorting must not mutate membership")
			}
		})
	}
}

func BenchmarkConflictSetArrivalOrder(b *testing.B) {
	chainParents := make([]int, 999)
	for i := range chainParents {
		chainParents[i] = i
	}
	for _, tc := range []struct {
		name    string
		parents []int
		reverse bool
	}{
		{name: "fan out", parents: make([]int, 999)},
		{name: "chain", parents: chainParents},
		{name: "fan out reversed", parents: make([]int, 999), reverse: true},
		{name: "chain reversed", parents: chainParents, reverse: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := &conflictSet{txs: make(TxNodeBatch, 1000)}
			f.txs[0] = &TransactionNode{}
			for i, parent := range tc.parents {
				f.txs[i+1] = &TransactionNode{seq: uint64(i + 1), dependsOnTxs: TxNodeBatch{f.txs[parent]}}
			}
			if tc.reverse {
				slices.Reverse(f.txs)
			}
			b.ReportAllocs()
			for b.Loop() {
				f.arrivalOrder()
			}
		})
	}
}

func TestConflictSetAbsorb(t *testing.T) {
	t.Parallel()
	keys := makeTestKeys(t, 3)
	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	c := createTxNode(t, nil, [][]byte{keys[2]}, nil)

	inFlight := newConflictSet(c)
	inFlight.pending = 1

	fa := newConflictSet(a)
	fb := newConflictSet(b)
	fb.addDependency(inFlight)

	fa.absorb(fb)

	require.ElementsMatch(t, TxNodeBatch{a, b}, fa.txs)
	require.Same(t, fa, b.conflictSet)
	require.Contains(t, fa.dependsOnConflictSets, inFlight)
	require.Contains(t, inFlight.dependentConflictSets, fa)
	require.NotContains(t, inFlight.dependentConflictSets, fb)
}

// requireConflictSet asserts exact membership and that prerequisites precede their dependents.
func requireConflictSet(t *testing.T, got TxNodeBatch, want ...*TransactionNode) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, tx := range want {
		require.Same(t, tx, got[i], "arrival order must be preserved")
	}
	position := make(map[*TransactionNode]int, len(got))
	for i, tx := range got {
		position[tx] = i
	}
	for _, tx := range got {
		for _, dep := range tx.dependsOnTxs {
			require.Contains(t, position, dep)
			require.Less(t, position[dep], position[tx], "prerequisite must precede its dependent")
		}
	}
}
