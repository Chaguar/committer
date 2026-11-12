/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package servicepb

import (
	"github.com/Chaguar/common/api/committerpb"
	"github.com/cockroachdb/errors"
)

// NewTxResults wraps status-only responses without adding version metadata.
func NewTxResults(statuses []*committerpb.TxStatus) []*TxResult {
	if statuses == nil {
		return nil
	}
	results := make([]*TxResult, len(statuses))
	for i, status := range statuses {
		results[i] = &TxResult{Status: status}
	}
	return results
}

// GetStatuses projects results onto the unchanged public status type.
func (x *TxStatusBatch) GetStatuses() []*committerpb.TxStatus {
	if x == nil || x.TxResult == nil {
		return nil
	}
	statuses := make([]*committerpb.TxStatus, len(x.TxResult))
	for i, result := range x.TxResult {
		statuses[i] = result.GetStatus()
	}
	return statuses
}

// ValidateResults rejects malformed results before consumers process their statuses or versions.
func (x *TxStatusBatch) ValidateResults() error {
	if x == nil {
		return errors.New("nil transaction result batch")
	}
	for i, result := range x.TxResult {
		if result == nil || result.Status == nil || result.Status.Ref == nil {
			return errors.Newf("invalid transaction result at index %d", i)
		}
		if result.Status.Status != committerpb.Status_COMMITTED && len(result.BlindWriteVersions) != 0 {
			return errors.Newf("noncommitted result has write versions at index %d", i)
		}
	}
	return nil
}
