/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package servicepb

import (
	"testing"

	"github.com/Chaguar/common/api/committerpb"
	"github.com/stretchr/testify/require"
)

func TestTxStatusBatchValidateResults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		batch *TxStatusBatch
	}{
		{name: "empty", batch: &TxStatusBatch{}},
		{name: "feedback only", batch: &TxStatusBatch{
			CheckpointFeedback: &CheckpointFeedback{Signal: CheckpointFeedback_HOLD},
		}},
		{name: "status only", batch: &TxStatusBatch{TxResult: NewTxResults([]*committerpb.TxStatus{
			committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx", 1, 0),
		})}},
		{name: "committed with versions", batch: &TxStatusBatch{TxResult: []*TxResult{{
			Status:             committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx", 1, 0),
			BlindWriteVersions: []*committerpb.NamespaceWriteVersions{{NsId: "1"}},
		}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, tc.batch.ValidateResults())
		})
	}
}

func TestTxStatusBatchValidateResultsInvalid(t *testing.T) {
	t.Parallel()
	const invalidResultError = "invalid transaction result at index 0"
	for _, tc := range []struct {
		name        string
		batch       *TxStatusBatch
		expectedErr string
	}{
		{name: "nil batch", expectedErr: "nil transaction result batch"},
		{
			name: "nil result", batch: &TxStatusBatch{TxResult: []*TxResult{nil}},
			expectedErr: invalidResultError,
		},
		{
			name: "nil status", batch: &TxStatusBatch{TxResult: []*TxResult{{}}},
			expectedErr: invalidResultError,
		},
		{
			name: "nil reference", batch: &TxStatusBatch{TxResult: []*TxResult{{Status: &committerpb.TxStatus{}}}},
			expectedErr: invalidResultError,
		},
		{name: "noncommitted versions", batch: &TxStatusBatch{TxResult: []*TxResult{{
			Status:             committerpb.NewTxStatus(committerpb.Status_ABORTED_MVCC_CONFLICT, "tx", 1, 0),
			BlindWriteVersions: []*committerpb.NamespaceWriteVersions{{NsId: "1"}},
		}}}, expectedErr: "noncommitted result has write versions at index 0"},
		{name: "invalid second result", batch: &TxStatusBatch{TxResult: []*TxResult{
			{Status: committerpb.NewTxStatus(committerpb.Status_COMMITTED, "tx", 1, 0)}, nil,
		}}, expectedErr: "invalid transaction result at index 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.EqualError(t, tc.batch.ValidateResults(), tc.expectedErr)
		})
	}
}
