/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Chaguar/common/api/committerpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/utils/channel"
	"github.com/Chaguar/committer/utils/test"
)

type committerTestEnv struct {
	c            *transactionCommitter
	validatedTxs chan *validatedTransactions
	txStatus     chan *servicepb.TxStatusBatch
	dbEnv        *DatabaseTestEnv
}

// newCommitterTestEnv starts a committer against a fresh database. Most tests use
// this: they either don't touch snapshots at all, or manually drive/inspect a
// specific intermediate _snapshot state (e.g. PENDING or IN_PROGRESS) right after
// commit. A test that wants a record hashed calls hashLatestSnapshotIfNeeded
// itself, so hashing cannot race the state a test expects to observe.
func newCommitterTestEnv(t *testing.T) *committerTestEnv {
	t.Helper()
	validatedTxs := make(chan *validatedTransactions, 10)
	txStatus := make(chan *servicepb.TxStatusBatch, 10)

	dbEnv := NewDatabaseTestEnv(t)
	metrics := newVCServiceMetrics(&queues{validatedTxs: validatedTxs, txsStatus: txStatus})
	c := newCommitter(validatedTxs, txStatus, metrics)
	test.RunServiceForTest(t.Context(), t, func(ctx context.Context) error {
		return c.run(ctx, dbEnv.DB, 1)
	}, nil)
	return &committerTestEnv{
		c:            c,
		validatedTxs: validatedTxs,
		txStatus:     txStatus,
		dbEnv:        dbEnv,
	}
}

type state struct {
	namespace      string
	keySuffix      int
	updateSequence uint64
}

func writes(isBlind bool, allWrites ...state) namespaceToWrites { //nolint: revive
	ntw := make(namespaceToWrites)
	for _, ww := range allWrites {
		nw := ntw.getOrCreate(ww.namespace)
		var ver uint64
		if !isBlind {
			ver = ww.updateSequence
		}
		nw.append(
			fmt.Appendf(nil, "key%s.%d", ww.namespace, ww.keySuffix),
			fmt.Appendf(nil, "value%s.%d.%d", ww.namespace, ww.keySuffix, ww.updateSequence),
			ver,
		)
	}
	return ntw
}

func TestCommit(t *testing.T) { //nolint:maintidx // cannot improve.
	t.Parallel()
	env := newCommitterTestEnv(t)

	env.dbEnv.populateData(
		t,
		[]string{"1", "2"},
		writes(
			false,
			state{"1", 1, 1},
			state{"1", 2, 1},
			state{"1", 3, 2},
			state{"1", 4, 2},
			state{"2", 1, 0},
			state{"2", 2, 0},
			state{"2", 3, 1},
			state{"2", 4, 1},
		),
		&servicepb.TxStatusBatch{
			TxResult: servicepb.NewTxResults([]*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, string(txs[0]), 1, 1),
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, string(txs[1]), 1, 2),
			}),
		},
		transactionIDToHeight{
			txs[0]: servicepb.NewHeight(1, 1),
			txs[1]: servicepb.NewHeight(1, 2),
		},
	)

	// Note: the order of the subtest is important
	tests := []struct {
		name                      string
		txs                       *validatedTransactions
		expectedTxStatuses        []*committerpb.TxStatus
		expectedNsRows            namespaceToWrites
		unexpectedNsRows          namespaceToWrites
		expectedMaxSeenBlocNumber uint64
	}{
		{
			name: "new writes",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{},
				validTxBlindWrites:    transactionToWrites{},
				newWrites: transactionToWrites{
					"tx-new-1": writes(
						true,
						state{"1", 10, 0},
						state{"1", 11, 0},
						state{"2", 10, 0},
						state{"2", 11, 0},
					),
					"tx-new-2": writes(
						true,
						state{"1", 20, 0},
						state{"1", 21, 0},
						state{"2", 20, 0},
						state{"2", 21, 0},
					),
				},
				invalidTxStatus: map[TxID]committerpb.Status{},
				txIDToHeight: transactionIDToHeight{
					"tx-new-1": servicepb.NewHeight(1, 1),
					"tx-new-2": servicepb.NewHeight(244, 2),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-new-1", 1, 1),
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-new-2", 244, 2),
			},
			expectedNsRows: writes(
				false,
				state{"1", 10, 0},
				state{"1", 11, 0},
				state{"2", 10, 0},
				state{"2", 11, 0},
				state{"1", 20, 0},
				state{"1", 21, 0},
				state{"2", 20, 0},
				state{"2", 21, 0},
			),
			expectedMaxSeenBlocNumber: 244,
		},
		{
			name: "non-blind writes",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{
					"tx-non-blind-1": writes(
						false,
						state{"1", 1, 2},
						state{"1", 2, 2},
						state{"2", 1, 1},
						state{"2", 2, 1},
					),
				},
				validTxBlindWrites: transactionToWrites{},
				newWrites:          transactionToWrites{},
				invalidTxStatus:    map[TxID]committerpb.Status{},
				txIDToHeight: transactionIDToHeight{
					"tx-non-blind-1": servicepb.NewHeight(239, 1),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-non-blind-1", 239, 1),
			},
			expectedNsRows: writes(
				false,
				state{"1", 1, 2},
				state{"1", 2, 2},
				state{"2", 1, 1},
				state{"2", 2, 1},
			),
			expectedMaxSeenBlocNumber: 244,
		},
		{
			name: "blind writes",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{},
				validTxBlindWrites: transactionToWrites{
					"tx-blind-1": writes(
						true,
						state{"1", 1, 3},
						state{"1", 2, 3},
						state{"2", 1, 2},
						state{"2", 2, 2},
					),
				},
				newWrites:       transactionToWrites{},
				invalidTxStatus: map[TxID]committerpb.Status{},
				txIDToHeight: transactionIDToHeight{
					"tx-blind-1": servicepb.NewHeight(1024, 1),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-blind-1", 1024, 1),
			},
			expectedNsRows: writes(
				false,
				state{"1", 1, 3},
				state{"1", 2, 3},
				state{"2", 1, 2},
				state{"2", 2, 2},
			),
			expectedMaxSeenBlocNumber: 1024,
		},
		{
			name: "blind, non-blind, and new writes",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{
					"tx-all-1": writes(
						false,
						state{"1", 1, 4},
						state{"1", 2, 4},
					),
					"tx-all-2": writes(
						false,
						state{"1", 3, 3},
						state{"1", 4, 3},
					),
				},
				validTxBlindWrites: transactionToWrites{
					"tx-all-1": writes(
						true,
						state{"2", 1, 3},
						state{"2", 2, 3},
						state{"2", 5, 0},
					),
					"tx-all-2": writes(
						true,
						state{"2", 3, 2},
						state{"2", 4, 2},
						state{"2", 6, 0},
					),
				},
				newWrites: transactionToWrites{
					"tx-all-1": writes(
						true,
						state{"2", 30, 0},
					),
					"tx-all-2": writes(
						true,
						state{"2", 31, 0},
					),
				},
				invalidTxStatus: map[TxID]committerpb.Status{
					"tx-conflict-1": committerpb.Status_ABORTED_MVCC_CONFLICT,
					"tx-conflict-2": committerpb.Status_ABORTED_MVCC_CONFLICT,
					"tx-conflict-3": committerpb.Status_ABORTED_MVCC_CONFLICT,
				},
				txIDToHeight: transactionIDToHeight{
					"tx-all-1":      servicepb.NewHeight(5, 1),
					"tx-all-2":      servicepb.NewHeight(200, 2),
					"tx-conflict-1": servicepb.NewHeight(1, 1),
					"tx-conflict-2": servicepb.NewHeight(396, 2),
					"tx-conflict-3": servicepb.NewHeight(396, 3),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-all-1", 5, 1),
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-all-2", 200, 2),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-1", 1, 1),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-2", 396, 2),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-3", 396, 3),
			},
			expectedNsRows: writes(
				false,
				state{"1", 1, 4},
				state{"1", 2, 4},
				state{"1", 3, 3},
				state{"1", 4, 3},
				state{"2", 1, 3},
				state{"2", 2, 3},
				state{"2", 3, 2},
				state{"2", 4, 2},
				state{"2", 5, 0},
				state{"2", 6, 0},
				state{"2", 30, 0},
				state{"2", 31, 0},
			),
			expectedMaxSeenBlocNumber: 1024,
		},
		{
			name: "new writes with violating",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{
					"tx-not-violate-1": writes(
						false,
						state{"1", 1, 5},
					),
					"tx-violate-1": writes(
						true,
						state{"1", 3, 4},
					),
				},
				validTxBlindWrites: transactionToWrites{
					"tx-not-violate-1": writes(
						false,
						state{"1", 2, 5},
					),
					"tx-violate-1": writes(
						true,
						state{"1", 4, 4},
					),
				},
				newWrites: transactionToWrites{
					"tx-not-violate-1": writes(
						true,
						state{"1", 22, 0}, // not violate
					),
					"tx-violate-1": writes(
						true,
						state{"1", 10, 0}, // violate
						state{"1", 12, 0}, // not violate
					),
				},
				invalidTxStatus: map[TxID]committerpb.Status{
					"tx-conflict-4": committerpb.Status_ABORTED_MVCC_CONFLICT,
				},
				readToTxIDs: map[comparableRead][]TxID{
					newCmpRead("1", []byte("key1.10"), nil): {"tx-violate-1"},
				},
				txIDToHeight: transactionIDToHeight{
					"tx-violate-1":     servicepb.NewHeight(1, 1),
					"tx-not-violate-1": servicepb.NewHeight(4, 2),
					"tx-conflict-4":    servicepb.NewHeight(1000, 3),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-violate-1", 1, 1),
				committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx-not-violate-1", 4, 2),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-4", 1000, 3),
			},
			expectedNsRows: writes(
				false,
				state{"1", 1, 5},
				state{"1", 2, 5},
				state{"1", 3, 3},
				state{"1", 4, 3},
				state{"1", 10, 0},
				state{"1", 22, 0},
			),
			unexpectedNsRows: writes(
				false,
				state{"1", 12, 0},
			),
			expectedMaxSeenBlocNumber: 10000,
		},
		{
			name: "all invalid txs",
			txs: &validatedTransactions{
				validTxNonBlindWrites: transactionToWrites{
					txs[0]: writes(false, state{"2", 7, 1}),
				},
				validTxBlindWrites: transactionToWrites{
					txs[0]: writes(true, state{"1", 1, 6}),
				},
				newWrites: transactionToWrites{
					txs[0]: writes(true, state{"1", 40, 0}),
				},
				invalidTxStatus: map[TxID]committerpb.Status{
					"tx-conflict-10": committerpb.Status_ABORTED_MVCC_CONFLICT,
					"tx-conflict-11": committerpb.Status_ABORTED_MVCC_CONFLICT,
					"tx-conflict-12": committerpb.Status_ABORTED_MVCC_CONFLICT,
				},
				txIDToHeight: transactionIDToHeight{
					txs[0]:           servicepb.NewHeight(1, 5),
					"tx-conflict-10": servicepb.NewHeight(1, 1),
					"tx-conflict-11": servicepb.NewHeight(4, 2),
					"tx-conflict-12": servicepb.NewHeight(66000, 3),
				},
			},
			expectedTxStatuses: []*committerpb.TxStatus{
				committerpb.NewTxStatus(committerpb.Status_REJECTED_DUPLICATE_TX_ID, string(txs[0]), 1, 5),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-10", 1, 1),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-11", 4, 2),
				committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx-conflict-12", 66000, 3),
			},
			expectedNsRows: writes(
				false,
				state{"1", 1, 5},
			),
			unexpectedNsRows: writes(
				false,
				state{"2", 7, 0},
				state{"1", 40, 0},
			),
			expectedMaxSeenBlocNumber: 66000,
		},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	for _, tt := range tests { //nolint:paralleltest // each test case depends on the previous test case.
		t.Run(tt.name, func(t *testing.T) {
			channel.NewWriter(ctx, env.validatedTxs).Write(tt.txs)
			txStatus, ok := channel.NewReader(ctx, env.txStatus).Read()
			require.True(t, ok)
			test.RequireProtoElementsMatch(t, tt.expectedTxStatuses, resultStatuses(txStatus.TxResult))
			for _, result := range txStatus.TxResult {
				if result.Status.Status != committerpb.Status_COMMITTED {
					require.Empty(t, result.BlindWriteVersions)
				}
			}
			for nsID, expectedRows := range tt.expectedNsRows {
				env.dbEnv.rowExists(t, nsID, *expectedRows)
			}
			for nsID, expectedRows := range tt.unexpectedNsRows {
				env.dbEnv.rowNotExists(t, nsID, expectedRows.keys)
			}
			env.dbEnv.StatusExistsForNonDuplicateTxID(ctx, t, tt.expectedTxStatuses)
		})
	}

	// The rejected attempts return early, so each label proves that its return path is observed:
	// "new writes with violating" is rejected by a conflict, and "all invalid txs" by a duplicate.
	dbMetrics := env.dbEnv.DB.metrics
	require.Positive(t, commitLatency(t, dbMetrics.databaseTxBatchCommitLatencySeconds, commitConflict))
	require.Positive(t, commitLatency(t, dbMetrics.databaseTxBatchCommitLatencySeconds, commitDuplicate))
	require.Positive(t, commitLatency(t, dbMetrics.databaseTxBatchCommitLatencySeconds, commitSuccess))
	require.Positive(t, commitLatency(t, dbMetrics.databaseTxBatchCommitUpdateLatencySeconds, commitSuccess))
}

// commitLatency returns the mean latency a commit histogram recorded under one outcome label.
func commitLatency(t *testing.T, h *prometheus.HistogramVec, status string) float64 {
	t.Helper()
	child, ok := h.WithLabelValues(status).(prometheus.Metric)
	require.True(t, ok)
	return test.GetMetricValue(t, child)
}

func TestLiveBlindWriteVersions(t *testing.T) {
	t.Parallel()
	env := newCommitterTestEnv(t)
	env.dbEnv.populateData(t, []string{"1", "2"},
		writes(false, state{"1", 1, 2}, state{"2", 1, 0}),
		&servicepb.TxStatusBatch{}, transactionIDToHeight{})
	initial := map[string]map[string]uint64{"1": {"key1.1": 3}, "2": {"key2.1": 1}}

	for attempt := range 2 {
		nsWrites := writes(true, state{"1", 1, 0}, state{"1", 2, 0}, state{"2", 1, 0})
		nsWrites["1"].values[0] = nil
		nsWrites["2"].append([]byte{0, 255}, []byte{}, 0)

		batch, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB,
			blindValidated(TxID(string(rune('a'+attempt))), servicepb.NewHeight(10+uint64(attempt), 0), nsWrites))
		require.NoError(t, err)
		require.Len(t, batch.TxResult, 1)
		result := batch.TxResult[0]
		require.Equal(t, committerpb.Status_COMMITTED, result.Status.Status)
		require.Len(t, result.BlindWriteVersions, 2)

		count := 0
		for _, ns := range result.BlindWriteVersions {
			for _, write := range ns.Writes {
				rows := env.dbEnv.FetchKeys(t, ns.NsId, [][]byte{write.Key})
				require.Contains(t, rows, string(write.Key))
				require.Equal(t, rows[string(write.Key)].Version, write.Version)
				require.Equal(t, initial[ns.NsId][string(write.Key)]+uint64(attempt), write.Version)
				count++
			}
		}
		require.Equal(t, 4, count)
	}
}

func TestCoordinatorResubmissionBlindVersions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		storedStatus    committerpb.Status
		submittedHeight *servicepb.Height
		initialVersion  uint64
		initialWrites   namespaceToWrites
		invalidStatus   *committerpb.Status
		expectedStatus  committerpb.Status
	}{
		{
			name: "committed", storedStatus: committerpb.Status_COMMITTED,
			submittedHeight: servicepb.NewHeight(10, 0), initialVersion: 7,
			expectedStatus: committerpb.Status_COMMITTED,
		},
		{
			name: "version-zero", storedStatus: committerpb.Status_COMMITTED,
			submittedHeight: servicepb.NewHeight(10, 0), expectedStatus: committerpb.Status_COMMITTED,
		},
		{
			name: "invalidated", storedStatus: committerpb.Status_COMMITTED,
			submittedHeight: servicepb.NewHeight(10, 0), initialVersion: 7,
			invalidStatus: new(committerpb.Status_ABORTED_MVCC_CONFLICT), expectedStatus: committerpb.Status_COMMITTED,
		},
		{
			name: "different-height", storedStatus: committerpb.Status_COMMITTED,
			submittedHeight: servicepb.NewHeight(11, 0), initialVersion: 7,
			expectedStatus: committerpb.Status_REJECTED_DUPLICATE_TX_ID,
		},
		{
			name: "aborted", storedStatus: committerpb.Status_ABORTED_MVCC_CONFLICT,
			submittedHeight: servicepb.NewHeight(10, 0), initialVersion: 7,
			expectedStatus: committerpb.Status_ABORTED_MVCC_CONFLICT,
		},
		{
			name: "missing-key", storedStatus: committerpb.Status_COMMITTED,
			submittedHeight: servicepb.NewHeight(10, 0), initialWrites: make(namespaceToWrites),
			expectedStatus: committerpb.Status_COMMITTED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newCommitterTestEnv(t)
			seed := tc.initialWrites
			if seed == nil {
				seed = writes(false, state{"1", 1, tc.initialVersion})
			}

			persisted := committerpb.NewTxStatus(tc.storedStatus, "retry", 10, 0)
			env.dbEnv.populateData(t, []string{"1"}, seed,
				&servicepb.TxStatusBatch{TxResult: []*servicepb.TxResult{{Status: persisted}}},
				transactionIDToHeight{"retry": servicepb.NewHeight(10, 0)})

			key := []byte("key1.1")
			before := env.dbEnv.FetchKeys(t, "1", [][]byte{key})

			validated := blindValidated("retry", tc.submittedHeight, writes(true, state{"1", 1, 0}))
			if tc.invalidStatus != nil {
				validated.updateInvalidTxs([]TxID{"retry"}, *tc.invalidStatus)
			}

			batch, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB, validated)
			require.NoError(t, err)

			require.Len(t, batch.TxResult, 1)
			require.Equal(t, tc.expectedStatus, batch.TxResult[0].Status.Status)
			require.Empty(t, batch.TxResult[0].BlindWriteVersions)
			require.Equal(t, before, env.dbEnv.FetchKeys(t, "1", [][]byte{key}))
		})
	}
}

func TestResubmissionAfterLaterUpdateOmitsBlindVersions(t *testing.T) {
	t.Parallel()
	env := newCommitterTestEnv(t)
	env.dbEnv.populateData(t, []string{"1"}, writes(false, state{"1", 1, 5}),
		&servicepb.TxStatusBatch{}, transactionIDToHeight{})

	// Step 1: Commit T1 before the recovery watermark advances.
	t.Log("Step 1: Commit T1 before the recovery watermark advances.")
	first, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB,
		blindValidated("t1", servicepb.NewHeight(10, 0), writes(true, state{"1", 1, 0})))
	require.NoError(t, err)
	require.Equal(t, committerpb.Status_COMMITTED, first.TxResult[0].Status.Status)
	assigned := first.TxResult[0].BlindWriteVersions[0].Writes[0].Version
	require.EqualValues(t, 6, assigned)

	// Step 2: T1's result releases T2, which advances the same key's version.
	t.Log("Step 2: T1's result releases T2, which advances the same key's version.")
	second, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB,
		blindValidated("t2", servicepb.NewHeight(11, 0), writes(true, state{"1", 1, 0})))
	require.NoError(t, err)
	require.Equal(t, committerpb.Status_COMMITTED, second.TxResult[0].Status.Status)
	require.Equal(t, assigned+1, second.TxResult[0].BlindWriteVersions[0].Writes[0].Version)
	before := env.dbEnv.FetchKeys(t, "1", [][]byte{[]byte("key1.1")})

	// Step 3: Coordinator restart replays T1; historical blind versions are unavailable.
	t.Log("Step 3: Coordinator restart replays T1; historical blind versions are unavailable.")
	replay, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB,
		blindValidated("t1", servicepb.NewHeight(10, 0), writes(true, state{"1", 1, 0})))
	require.NoError(t, err)
	test.RequireProtoEqual(t, first.TxResult[0].Status, replay.TxResult[0].Status)
	require.Empty(t, replay.TxResult[0].BlindWriteVersions)
	require.Equal(t, before, env.dbEnv.FetchKeys(t, "1", [][]byte{[]byte("key1.1")}))
}

func blindValidated(id TxID, height *servicepb.Height, nsWrites namespaceToWrites) *validatedTransactions {
	return &validatedTransactions{
		validTxBlindWrites:    transactionToWrites{id: nsWrites},
		validTxNonBlindWrites: make(transactionToWrites), newWrites: make(transactionToWrites),
		invalidTxStatus: make(map[TxID]committerpb.Status), readToTxIDs: make(readToTransactions),
		txIDToHeight: transactionIDToHeight{id: height},
	}
}
