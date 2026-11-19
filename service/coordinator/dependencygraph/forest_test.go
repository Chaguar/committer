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

func TestForestTopoSort(t *testing.T) {
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
			f := &forest{}
			for i, deps := range tc.deps {
				tx := createTxNode(t, nil, [][]byte{keys[i]}, nil)
				for _, dep := range deps {
					tx.addDependenciesAndUpdateDependents(TxNodeBatch{f.txs[dep]})
				}
				f.txs = append(f.txs, tx)
			}

			want := slices.Clone(f.txs)
			// Membership order must not determine execution order.
			slices.Reverse(f.txs)
			requireForest(t, f.topoSort(), want...)
		})
	}
}

func BenchmarkForestTopoSort(b *testing.B) {
	chainParents := make([]int, 999)
	for i := range chainParents {
		chainParents[i] = i
	}
	for _, tc := range []struct {
		name    string
		parents []int
	}{
		{name: "fan out", parents: make([]int, 999)},
		{name: "chain", parents: chainParents},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f := &forest{txs: make(TxNodeBatch, 1000)}
			f.txs[0] = &TransactionNode{}
			for i, parent := range tc.parents {
				f.txs[i+1] = &TransactionNode{dependsOnTxs: TxNodeBatch{f.txs[parent]}}
			}
			b.ReportAllocs()
			for b.Loop() {
				f.topoSort()
			}
		})
	}
}

func TestForestAbsorb(t *testing.T) {
	t.Parallel()
	keys := makeTestKeys(t, 3)
	a := createTxNode(t, nil, [][]byte{keys[0]}, nil)
	b := createTxNode(t, nil, [][]byte{keys[1]}, nil)
	c := createTxNode(t, nil, [][]byte{keys[2]}, nil)

	inFlight := newForest(c)
	inFlight.inFlight = true

	fa := newForest(a)
	fb := newForest(b)
	fb.addDependency(inFlight)

	fa.absorb(fb)

	require.ElementsMatch(t, TxNodeBatch{a, b}, fa.txs)
	require.Same(t, fa, b.forest)
	require.Contains(t, fa.dependsOnForests, inFlight)
	require.Contains(t, inFlight.dependentForests, fa)
	require.NotContains(t, inFlight.dependentForests, fb)
}

// requireForest asserts exact membership and that prerequisites precede their dependents.
func requireForest(t *testing.T, got TxNodeBatch, want ...*TransactionNode) {
	t.Helper()
	require.ElementsMatch(t, want, got)
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
