/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

// forest is a weakly connected set of transactions linked by dependency edges.
// A waiting forest can still grow and merge with other waiting forests.
// An in-flight forest has been emitted and its membership is frozen until
// every member is validated.
// Dependency edges (dependsOnTxs) never cross forests. A dependency on an
// in-flight forest is tracked only at the forest level via dependsOnForests and dependentForests.
type forest struct {
	// txs holds the members independently of the consumer-owned output slice.
	txs TxNodeBatch
	// inFlight is true once the forest has been emitted.
	inFlight bool
	// pending is the number of members whose validation status has not been
	// received. It is set on emit.
	pending int
	// dependentTxCount is the number of members counted in the dependent
	// transactions gauge. It is subtracted from the gauge on emit.
	dependentTxCount int
	// dependsOnForests holds the in-flight forests this waiting forest waits for.
	// The forest is emitted once dependsOnForests becomes empty.
	dependsOnForests map[*forest]struct{}
	// dependentForests holds the waiting forests that wait for this in-flight forest.
	dependentForests map[*forest]struct{}
}

func newForest(tx *TransactionNode) *forest {
	f := &forest{
		txs:              TxNodeBatch{tx},
		dependsOnForests: make(map[*forest]struct{}),
		dependentForests: make(map[*forest]struct{}),
	}
	tx.forest = f
	return f
}

// absorb moves all members and dependencies of the waiting forest other into f.
// After this call, other is no longer referenced by any transaction or dependency.
func (f *forest) absorb(other *forest) {
	for _, tx := range other.txs {
		tx.forest = f
	}
	f.txs = append(f.txs, other.txs...)
	f.dependentTxCount += other.dependentTxCount
	for b := range other.dependsOnForests {
		delete(b.dependentForests, other)
		f.addDependency(b)
	}
}

// addDependency records that the waiting forest f waits for the in-flight forest b.
func (f *forest) addDependency(b *forest) {
	f.dependsOnForests[b] = struct{}{}
	b.dependentForests[f] = struct{}{}
}

// topoSort orders members using Kahn's algorithm so every prerequisite precedes
// its dependents. Edges are internal to the forest and always point to older
// transactions, so cycles cannot occur.
func (f *forest) topoSort() TxNodeBatch {
	inDegree := make(map[*TransactionNode]int, len(f.txs))
	children := make(map[*TransactionNode]TxNodeBatch, len(f.txs))
	queue := make(TxNodeBatch, 0, len(f.txs))
	for _, tx := range f.txs {
		inDegree[tx] = len(tx.dependsOnTxs)
		for _, dep := range tx.dependsOnTxs {
			children[dep] = append(children[dep], tx)
		}
		if len(tx.dependsOnTxs) == 0 {
			queue = append(queue, tx)
		}
	}

	for i := 0; i < len(queue); i++ {
		for _, child := range children[queue[i]] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	return queue
}
