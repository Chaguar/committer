/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"cmp"
	"slices"
)

// conflictSet groups transactions connected directly or transitively by read-write,
// write-read, or write-write conflicts. Members need not conflict pairwise.
// Waiting sets can grow and merge; emitted sets stay frozen until every member is validated.
// Transaction dependency edges never cross sets. Dependencies on in-flight sets
// are tracked separately via dependsOnConflictSets and dependentConflictSets.
type conflictSet struct {
	// txs holds the members independently of the consumer-owned output slice.
	txs TxNodeBatch
	// pending counts unreported members after emission; zero before emission and after completion.
	pending int
	// dependentTxCount is the number of members counted in the dependent
	// transactions gauge. It is subtracted from the gauge on emit.
	dependentTxCount int
	// dependsOnConflictSets holds the in-flight conflict sets this waiting conflict set waits for.
	// The conflict set is emitted once dependsOnConflictSets becomes empty.
	dependsOnConflictSets map[*conflictSet]struct{}
	// dependentConflictSets holds the waiting conflict sets that wait for this in-flight conflict set.
	dependentConflictSets map[*conflictSet]struct{}
}

func newConflictSet(tx *TransactionNode) *conflictSet {
	f := &conflictSet{
		txs:                   TxNodeBatch{tx},
		dependsOnConflictSets: make(map[*conflictSet]struct{}),
		dependentConflictSets: make(map[*conflictSet]struct{}),
	}
	tx.conflictSet = f
	return f
}

// absorb moves all members and dependencies of the waiting conflict set other into f.
// After this call, other is no longer referenced by any transaction or dependency.
func (f *conflictSet) absorb(other *conflictSet) {
	for _, tx := range other.txs {
		tx.conflictSet = f
	}
	f.txs = append(f.txs, other.txs...)
	f.dependentTxCount += other.dependentTxCount
	for prerequisite := range other.dependsOnConflictSets {
		delete(prerequisite.dependentConflictSets, other)
		f.addDependency(prerequisite)
	}
}

// addDependency links a waiting set to an in-flight prerequisite.
func (f *conflictSet) addDependency(prerequisite *conflictSet) {
	f.dependsOnConflictSets[prerequisite] = struct{}{}
	prerequisite.dependentConflictSets[f] = struct{}{}
}

// arrivalOrder is topological because every dependency points to an earlier arrival.
// Merging can scramble membership order, so order a consumer-owned copy by sequence.
func (f *conflictSet) arrivalOrder() TxNodeBatch {
	sorted := slices.Clone(f.txs)
	slices.SortFunc(sorted, func(a, b *TransactionNode) int { return cmp.Compare(a.seq, b.seq) })
	return sorted
}
