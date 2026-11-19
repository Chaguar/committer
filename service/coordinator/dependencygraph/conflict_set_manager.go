/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dependencygraph

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Chaguar/committer/utils"
	"github.com/Chaguar/committer/utils/channel"
	"github.com/Chaguar/committer/utils/monitoring"
	"github.com/Chaguar/committer/utils/monitoring/promutil"
)

type (
	// ConflictSetManager emits sets of transactions connected directly or transitively
	// by read-write, write-read, or write-write conflicts. Members need not conflict
	// pairwise. Each set follows arrival order, so prerequisites precede dependents.
	// A transaction depending on an emitted set waits for every member to be validated.
	// Unlike Manager, this manager delegates within-set sequencing to its consumer.
	ConflictSetManager struct {
		localDepConstructor   *localDependencyConstructor
		conflictSetDepManager *conflictSetDependencyManager
		parameters            *ConflictSetParameters
	}

	// ConflictSetParameters holds the configuration for the conflict set dependency manager.
	ConflictSetParameters struct {
		// IncomingTxs is the channel for the manager to receive incoming transactions.
		IncomingTxs <-chan *TransactionBatch
		// OutgoingDepFreeConflictSets is the channel for the manager to send dependency-free
		// conflict sets. Each inner slice follows batch-ID order, then transaction order
		// within each batch, without consulting block or transaction numbers.
		// Separate sets may execute concurrently. Set boundaries and cross-set execution
		// order can vary with validation-feedback timing.
		// The consumer owns all emitted slices, but transaction nodes remain shared
		// with the manager.
		OutgoingDepFreeConflictSets chan<- []TxNodeBatch
		// IncomingValidatedTxsNode is the channel for the manager to receive validated
		// transactions. Every transaction of an emitted conflict set must be reported exactly
		// once, in any grouping.
		IncomingValidatedTxsNode <-chan TxNodeBatch
		// NumOfLocalDepConstructors defines the number of local dependency constructors.
		NumOfLocalDepConstructors int
		// WaitingTxsLimit defines the maximum number of transactions held by the manager.
		WaitingTxsLimit int
		// PrometheusMetricsProvider is the provider for Prometheus metrics.
		PrometheusMetricsProvider *monitoring.Provider
	}

	// conflictSetDependencyManager groups transactions into conflict sets and decides when each
	// conflict set is emitted and when it is complete.
	conflictSetDependencyManager struct {
		incomingTxsNode      <-chan *transactionNodeBatch
		outgoingConflictSets chan<- []TxNodeBatch
		validatedTxsNode     <-chan TxNodeBatch

		// mu protects detector, ready, seq, active, and every set and its members' conflictSet fields.
		mu sync.Mutex
		// detector holds the reads and writes of every transaction that is not part of
		// a completed conflict set.
		detector *dependencyDetector
		// ready holds emitted conflict sets not yet written to outgoingConflictSets.
		ready []TxNodeBatch
		// active holds emission times until every member validates, including queued output.
		active map[*conflictSet]time.Time
		// seq assigns transaction order after the local constructor orders batches by ID.
		seq uint64
		// wakeup coalesces notifications; ready holds all work until the output goroutine drains it.
		wakeup *outputSignal

		waitingTxsSlots *utils.Slots
		metrics         *perfMetrics
	}

	conflictSetDepConfig struct {
		incomingTxsNode      <-chan *transactionNodeBatch
		outgoingConflictSets chan<- []TxNodeBatch
		validatedTxsNode     <-chan TxNodeBatch
		waitingTxsLimit      int
		metrics              *perfMetrics
	}
)

// NewConflictSetManager creates a new conflict set dependency manager.
func NewConflictSetManager(p *ConflictSetParameters) *ConflictSetManager {
	outgoingTxsNodeWithLocalDep := make(chan *transactionNodeBatch, cap(p.IncomingTxs))

	metrics := newPerformanceMetrics(p.PrometheusMetricsProvider, &managerQueues{
		ldgInput: p.IncomingTxs,
		gdgInput: outgoingTxsNodeWithLocalDep,
	})

	return &ConflictSetManager{
		localDepConstructor: newLocalDependencyConstructor(p.IncomingTxs, outgoingTxsNodeWithLocalDep, metrics),
		conflictSetDepManager: newConflictSetDependencyManager(&conflictSetDepConfig{
			incomingTxsNode:      outgoingTxsNodeWithLocalDep,
			outgoingConflictSets: p.OutgoingDepFreeConflictSets,
			validatedTxsNode:     p.IncomingValidatedTxsNode,
			waitingTxsLimit:      p.WaitingTxsLimit,
			metrics:              metrics,
		}),
		parameters: p,
	}
}

// Run starts the local dependency constructors and the conflict set dependency manager.
func (m *ConflictSetManager) Run(ctx context.Context) {
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		m.localDepConstructor.run(gCtx, m.parameters.NumOfLocalDepConstructors)
		return nil
	})

	g.Go(func() error {
		m.conflictSetDepManager.run(gCtx)
		return nil
	})

	_ = g.Wait()
}

func newConflictSetDependencyManager(c *conflictSetDepConfig) *conflictSetDependencyManager {
	logger.Info("Initializing newConflictSetDependencyManager")
	dm := &conflictSetDependencyManager{
		incomingTxsNode:      c.incomingTxsNode,
		outgoingConflictSets: c.outgoingConflictSets,
		validatedTxsNode:     c.validatedTxsNode,
		detector:             newDependencyDetector(),
		wakeup:               newOutputSignal(),
		active:               make(map[*conflictSet]time.Time),
		waitingTxsSlots:      utils.NewSlots(int64(c.waitingTxsLimit)),
		metrics:              c.metrics,
	}
	dm.metrics.registerConflictSetMetrics(dm)
	return dm
}

func (dm *conflictSetDependencyManager) run(ctx context.Context) {
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		dm.constructDependencyGraph(gCtx)
		return nil
	})

	g.Go(func() error {
		dm.processValidatedTransactions(gCtx)
		return nil
	})

	g.Go(func() error {
		dm.outputReadyConflictSets(gCtx)
		return nil
	})

	_ = g.Wait()
}

func (dm *conflictSetDependencyManager) constructDependencyGraph(ctx context.Context) {
	done := context.AfterFunc(ctx, dm.waitingTxsSlots.Broadcast)
	defer done()
	incomingTxsNode := channel.NewReader(ctx, dm.incomingTxsNode)
	for {
		batch, ok := incomingTxsNode.Read()
		if !ok {
			return
		}

		dm.waitingTxsSlots.Acquire(ctx, int64(len(batch.txsNode)))
		if ctx.Err() != nil {
			return
		}
		promutil.AddToGauge(dm.metrics.gdgWaitingTxCount, len(batch.txsNode))

		start := time.Now()
		dm.mu.Lock()
		promutil.Observe(dm.metrics.gdgConstructorWaitForLockSeconds, time.Since(start))
		dm.addBatch(batch)
		dm.mu.Unlock()

		promutil.AddToCounter(dm.metrics.gdgTxProcessedTotal, len(batch.txsNode))
	}
}

// addBatch adds every transaction of the batch to the graph, and only then emits
// the conflict sets of this batch that wait for no in-flight conflict set. Emitting only after
// the whole batch is added ensures that a later transaction in the batch cannot
// connect to an already emitted conflict set of the same batch.
// It must be called with dm.mu held.
func (dm *conflictSetDependencyManager) addBatch(batch *transactionNodeBatch) {
	for _, tx := range batch.txsNode {
		dm.addTx(tx)
	}
	dm.detector.mergeWaitingTx(batch.localDepDetector)

	// Emission freezes the set, preventing duplicate emission for shared members.
	for _, tx := range batch.txsNode {
		f := tx.conflictSet
		if f.pending == 0 && len(f.dependsOnConflictSets) == 0 {
			dm.emit(f)
		}
	}
}

// addTx places tx in a waiting conflict set. Dependencies on waiting transactions become
// dependency edges and merge their conflict sets with tx's conflict set. Dependencies on
// in-flight transactions become conflict set-level dependencies; no transaction edge is stored.
// Local dependencies (within the batch) are already set by the local dependency
// constructor and always point to waiting transactions of this batch.
// It must be called with dm.mu held.
func (dm *conflictSetDependencyManager) addTx(tx *TransactionNode) {
	dm.seq++
	tx.seq = dm.seq

	dependsOnWaitingTxs, dependsOnInFlightConflictSets := dm.splitGlobalDependencies(tx)
	// The local dependency constructor already counted tx if it has local dependencies.
	countedLocally := len(tx.dependsOnTxs) > 0
	isDependent := countedLocally || len(dependsOnWaitingTxs) > 0 || len(dependsOnInFlightConflictSets) > 0
	if isDependent && !countedLocally {
		promutil.AddToGauge(dm.metrics.dependentTxCount, 1)
	}
	// Waiting prerequisites join the same conflict set, so transaction edges stay internal.
	// In-flight conflict sets cannot be merged; their dependencies are recorded below.
	tx.dependsOnTxs = append(tx.dependsOnTxs, dependsOnWaitingTxs...)

	// Join the largest connected conflict set and merge the others into it.
	var target *conflictSet
	for _, dep := range tx.dependsOnTxs {
		if target == nil || len(dep.conflictSet.txs) > len(target.txs) {
			target = dep.conflictSet
		}
	}
	if target == nil {
		target = newConflictSet(tx)
	} else {
		target.txs = append(target.txs, tx)
		tx.conflictSet = target
	}
	// Merge the remaining prerequisite conflict sets into target so all waiting dependencies stay internal.
	for _, dep := range tx.dependsOnTxs {
		if dep.conflictSet != target {
			target.absorb(dep.conflictSet)
		}
	}
	for _, prerequisite := range dependsOnInFlightConflictSets {
		target.addDependency(prerequisite)
	}
	if isDependent {
		target.dependentTxCount++
	}
}

// splitGlobalDependencies returns the transactions from earlier batches that tx depends
// on and that are still waiting, and the in-flight conflict sets that tx depends on.
// It must be called with dm.mu held.
func (dm *conflictSetDependencyManager) splitGlobalDependencies(
	tx *TransactionNode,
) (dependsOnWaitingTxs TxNodeBatch, dependsOnInFlightConflictSets []*conflictSet) {
	for _, dep := range dm.detector.getDependenciesOf(tx) {
		if dep.conflictSet.pending > 0 {
			dependsOnInFlightConflictSets = append(dependsOnInFlightConflictSets, dep.conflictSet)
		} else {
			dependsOnWaitingTxs = append(dependsOnWaitingTxs, dep)
		}
	}
	return dependsOnWaitingTxs, dependsOnInFlightConflictSets
}

// emit freezes the set and queues a consumer-owned slice in arrival order.
// It must be called with dm.mu held.
func (dm *conflictSetDependencyManager) emit(f *conflictSet) {
	sorted := f.arrivalOrder()
	f.pending = len(f.txs)
	dm.active[f] = time.Now()

	// An emitted conflict set no longer waits on any dependency in the manager; the consumer
	// owns the sequencing of its internal dependencies.
	promutil.SubFromGauge(dm.metrics.dependentTxCount, f.dependentTxCount)

	// arrivalOrder clones membership so the consumer can modify the output slice.
	promutil.AddToGauge(dm.metrics.gdgDepFreeTxCount, len(f.txs))
	dm.ready = append(dm.ready, sorted)
	dm.wakeup.signal()
}

func (dm *conflictSetDependencyManager) processValidatedTransactions(ctx context.Context) {
	validatedTxsNode := channel.NewReader(ctx, dm.validatedTxsNode)
	for {
		txsNode, ok := validatedTxsNode.Read()
		if !ok {
			return
		}

		start := time.Now()
		dm.mu.Lock()
		promutil.Observe(dm.metrics.gdgValidatedTxProcessorWaitForLockSeconds, time.Since(start))
		released, accepted := dm.markValidated(txsNode)
		dm.mu.Unlock()

		dm.waitingTxsSlots.Release(int64(released))
		promutil.SubFromGauge(dm.metrics.gdgWaitingTxCount, released)
		promutil.AddToCounter(dm.metrics.gdgValidatedTxProcessedTotal, accepted)
	}
}

// markValidated records the validation of each transaction. Once every member of a
// conflict set is validated, the conflict set is removed from the graph and every waiting conflict set
// that no longer waits for any in-flight conflict set is emitted. Transactions not in an
// in-flight conflict set (e.g., rejected transactions) and duplicates are ignored.
// It returns the number of transactions in completed conflict sets and the number of accepted reports.
// It must be called with dm.mu held.
func (dm *conflictSetDependencyManager) markValidated(txsNode TxNodeBatch) (released, accepted int) {
	for _, tx := range txsNode {
		f := tx.conflictSet
		if f == nil || f.pending == 0 || tx.validated {
			continue
		}
		accepted++
		tx.validated = true
		f.pending--
		if f.pending > 0 {
			continue
		}

		delete(dm.active, f)
		dm.detector.removeWaitingTx(f.txs)
		released += len(f.txs)
		// Completion unblocks waiting sets only after their last prerequisite finishes.
		for w := range f.dependentConflictSets {
			delete(w.dependsOnConflictSets, f)
			if len(w.dependsOnConflictSets) == 0 {
				dm.emit(w)
			}
		}
		// Detach the completed conflict set so that it does not keep newer conflict sets reachable.
		// Later duplicate feedback for its members is ignored as their conflict set is nil.
		f.dependentConflictSets = nil
		for _, member := range f.txs {
			member.conflictSet = nil
		}
	}
	return released, accepted
}

// outputReadyConflictSets writes the emitted conflict sets to the output channel. It is the only
// writer of the output channel, so a slow consumer never blocks construction or
// validation processing.
func (dm *conflictSetDependencyManager) outputReadyConflictSets(ctx context.Context) {
	outgoingConflictSets := channel.NewWriter(ctx, dm.outgoingConflictSets)
	for dm.wakeup.wait(ctx) {
		dm.mu.Lock()
		conflictSets := dm.ready
		dm.ready = nil
		dm.mu.Unlock()

		if len(conflictSets) == 0 {
			continue
		}
		// Ownership transfers on write; do not inspect either slice afterward.
		txCount := 0
		for _, f := range conflictSets {
			txCount += len(f)
		}
		if !outgoingConflictSets.Write(conflictSets) {
			return
		}
		promutil.SubFromGauge(dm.metrics.gdgDepFreeTxCount, txCount)
	}
}

func (dm *conflictSetDependencyManager) activeConflictSetCount() float64 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return float64(len(dm.active))
}

func (dm *conflictSetDependencyManager) oldestActiveConflictSetAgeSeconds() float64 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	var oldest time.Time
	for _, emitted := range dm.active {
		if oldest.IsZero() || emitted.Before(oldest) {
			oldest = emitted
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest).Seconds()
}
