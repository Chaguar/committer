/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package vc

import (
	"maps"
	"testing"

	"github.com/Chaguar/common/api/applicationpb"
	"github.com/Chaguar/common/api/committerpb"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/utils/channel"
)

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

//nolint:gocognit // Keep the retry status/height/invalidation matrix in one test.
func TestCoordinatorResubmissionBlindVersions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                            string
		stored                          committerpb.Status
		sameHeight, invalidate, missing bool
		version                         uint64
	}{
		{name: "committed", stored: committerpb.Status_COMMITTED, sameHeight: true, version: 7},
		{name: "version-zero", stored: committerpb.Status_COMMITTED, sameHeight: true},
		{name: "invalidated", stored: committerpb.Status_COMMITTED, sameHeight: true, invalidate: true, version: 7},
		{name: "different-height", stored: committerpb.Status_COMMITTED, version: 7},
		{name: "aborted", stored: committerpb.Status_ABORTED_MVCC_CONFLICT, sameHeight: true, version: 7},
		{name: "missing-key", stored: committerpb.Status_COMMITTED, sameHeight: true, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newCommitterTestEnv(t)
			height := servicepb.NewHeight(10, 0)
			persisted := committerpb.NewTxStatus(tc.stored, "retry", 10, 0)
			seed := writes(false, state{"1", 1, tc.version})
			if tc.missing {
				seed = make(namespaceToWrites)
			}
			env.dbEnv.populateData(t, []string{"1"}, seed,
				&servicepb.TxStatusBatch{TxResult: []*servicepb.TxResult{{Status: persisted}}},
				transactionIDToHeight{"retry": height})

			key := []byte("key1.1")
			before := env.dbEnv.FetchKeys(t, "1", [][]byte{key})
			if !tc.sameHeight {
				height = servicepb.NewHeight(11, 0)
			}
			validated := blindValidated("retry", height, writes(true, state{"1", 1, 0}))
			if tc.invalidate {
				validated.updateInvalidTxs([]TxID{"retry"}, committerpb.Status_ABORTED_MVCC_CONFLICT)
			}
			require.NotEmpty(t, validated.originalBlindWrites)

			batch, err := env.c.commitTransactions(t.Context(), env.dbEnv.DB, validated)
			if tc.missing {
				require.ErrorContains(t, err, "missing committed blind key")
				require.Nil(t, batch)
				return
			}

			require.NoError(t, err)
			require.Len(t, batch.TxResult, 1)
			result := batch.TxResult[0]

			expected := tc.stored
			if !tc.sameHeight {
				expected = committerpb.Status_REJECTED_DUPLICATE_TX_ID
			}
			require.Equal(t, expected, result.Status.Status)
			if tc.sameHeight && tc.stored == committerpb.Status_COMMITTED {
				require.Len(t, result.BlindWriteVersions, 1)
				require.Len(t, result.BlindWriteVersions[0].Writes, 1)
				require.Equal(t, tc.version, result.BlindWriteVersions[0].Writes[0].Version)
			} else {
				require.Empty(t, result.BlindWriteVersions)
			}

			require.Equal(t, before, env.dbEnv.FetchKeys(t, "1", [][]byte{key}))
		})
	}
}

func TestOriginalBlindWritesBeforePreliminaryRejection(t *testing.T) {
	t.Parallel()
	env := newPrepareTestEnv(t)
	env.txBatch <- &servicepb.VcBatch{Transactions: []*servicepb.VcTx{{
		Ref:                   committerpb.NewTxRef("retry", 10, 0),
		PrelimInvalidTxStatus: new(committerpb.Status_ABORTED_SIGNATURE_INVALID),
		Namespaces: []*applicationpb.TxNamespace{nil, {
			NsId: "1", BlindWrites: []*applicationpb.Write{nil, {Key: []byte("k")}},
		}},
	}}}

	prepared, ok := channel.NewReader(t.Context(), env.preparedTxs).Read()
	require.True(t, ok)
	require.Equal(t, [][]byte{[]byte("k")}, prepared.originalBlindWrites["retry"]["1"].keys)
	require.Empty(t, prepared.txIDToNsBlindWrites)
}

func blindValidated(id TxID, height *servicepb.Height, nsWrites namespaceToWrites) *validatedTransactions {
	blind := transactionToWrites{id: nsWrites}
	return &validatedTransactions{
		validTxBlindWrites: blind, originalBlindWrites: maps.Clone(blind),
		validTxNonBlindWrites: make(transactionToWrites), newWrites: make(transactionToWrites),
		invalidTxStatus: make(map[TxID]committerpb.Status), readToTxIDs: make(readToTransactions),
		txIDToHeight: transactionIDToHeight{id: height},
	}
}
