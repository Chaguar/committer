/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"testing"
	"time"

	"github.com/Chaguar/common/api/applicationpb"
	"github.com/Chaguar/common/api/committerpb"
	"github.com/stretchr/testify/require"

	"github.com/Chaguar/committer/integration/runner"
)

func TestLiveCommittedWriteVersions(t *testing.T) {
	t.Parallel()
	c := runner.NewRuntime(t, &runner.Config{
		BlockSize: 5, BlockTimeout: 2 * time.Second,
		StreamBlocksRequest: &committerpb.StreamBlocksRequest{IncludeReadWriteSets: true},
	})
	c.Start(t, runner.FullTxPath)
	c.CreateNamespacesAndCommit(t, "1", "2")

	ids := c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{
		{
			NsId:        "1",
			ReadWrites:  []*applicationpb.ReadWrite{{Key: []byte("rw"), Value: []byte("v")}},
			BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: nil}},
		},
		{NsId: "2", BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: []byte("v2")}}},
	}}, []committerpb.Status{committerpb.Status_COMMITTED})

	event := c.LastStreamBlockEvent.Events[0]
	require.Equal(t, ids[0], event.Ref.TxId)
	requireCommittedVersions(t, c, 0, 3)

	c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{
		{
			NsId:        "1",
			ReadWrites:  []*applicationpb.ReadWrite{{Key: []byte("rw"), Version: new(uint64(0)), Value: nil}},
			BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: []byte{}}},
		},
		{NsId: "2", BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: nil}}},
	}}, []committerpb.Status{committerpb.Status_COMMITTED})
	requireCommittedVersions(t, c, 1, 3)

	before := c.DBEnv.FetchKeys(t, "1", [][]byte{[]byte("rw"), []byte("bw")})

	c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{
		{
			NsId: "1",
			ReadWrites: []*applicationpb.ReadWrite{{
				Key: []byte("rw"), Version: new(uint64(0)), Value: []byte("stale"),
			}},
			BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: []byte("invalid")}},
		},
	}}, []committerpb.Status{committerpb.Status_ABORTED_MVCC_CONFLICT})
	require.Empty(t, c.LastStreamBlockEvent.Events[0].CommittedWriteVersions)
	require.Equal(t, before, c.DBEnv.FetchKeys(t, "1", [][]byte{[]byte("rw"), []byte("bw")}))

	c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{
		{NsId: "1", BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: []byte("bad-signature")}}},
	}}, []committerpb.Status{committerpb.Status_ABORTED_SIGNATURE_INVALID})
	require.Empty(t, c.LastStreamBlockEvent.Events[0].CommittedWriteVersions)
	require.Equal(t, before, c.DBEnv.FetchKeys(t, "1", [][]byte{[]byte("rw"), []byte("bw")}))

	c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{
		{
			NsId:       "1",
			ReadWrites: []*applicationpb.ReadWrite{{Key: []byte("regular-only"), Value: nil}},
		},
	}}, []committerpb.Status{committerpb.Status_COMMITTED})
	requireCommittedVersions(t, c, 0, 1)
}

func requireCommittedVersions(t *testing.T, c *runner.CommitterRuntime, expected uint64, count int) {
	t.Helper()
	event := c.LastStreamBlockEvent.Events[0]
	require.Equal(t, committerpb.Status_COMMITTED, event.Status)

	seen := 0
	for _, ns := range event.CommittedWriteVersions {
		require.NotEmpty(t, ns.Writes)
		for _, write := range ns.Writes {
			rows := c.DBEnv.FetchKeys(t, ns.NsId, [][]byte{write.Key})
			require.Contains(t, rows, string(write.Key))
			require.Equal(t, rows[string(write.Key)].Version, write.Version)
			require.Equal(t, expected, write.Version)
			seen++
		}
	}

	require.Equal(t, count, seen)
}
