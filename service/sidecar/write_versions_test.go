/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sidecar

import (
	"testing"

	"github.com/Chaguar/common/api/applicationpb"
	"github.com/Chaguar/common/api/committerpb"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/utils/test"
)

func TestCommittedVersions(t *testing.T) {
	t.Parallel()
	block := versionTestBlock()
	key := []byte{0, 255}
	block.txs[0].Content.Namespaces[0].BlindWrites[0].Key = key
	block.blindWriteVersions[0][0].Writes[0].Key = key

	stream := &blockStream{includeReadWriteSets: true}
	events, err := stream.filterAndBuildEvents(block)
	require.NoError(t, err)

	require.Len(t, events, 1)
	test.RequireProtoElementsMatch(t, []*committerpb.NamespaceWriteVersions{
		{
			NsId: "1",
			Writes: []*committerpb.CommittedWriteVersion{
				{Key: []byte("rw"), Version: 3},
				{Key: key, Version: 4},
			},
		},
		{
			NsId: "2",
			Writes: []*committerpb.CommittedWriteVersion{
				{Key: []byte("rw"), Version: 0},
				{Key: []byte("bw"), Version: 0},
			},
		},
	}, events[0].CommittedWriteVersions)
}

func TestCommittedVersionSubscriberIsolation(t *testing.T) {
	t.Parallel()
	block := versionTestBlock()
	before := proto.Clone(block.txs[0].Content)

	a := &blockStream{includeReadWriteSets: true, filterNamespaces: []string{"1", "1"}}
	b := &blockStream{includeReadWriteSets: true, filterNamespaces: []string{"2"}}
	eventsA, err := a.filterAndBuildEvents(block)
	require.NoError(t, err)
	eventsB, err := b.filterAndBuildEvents(block)
	require.NoError(t, err)

	require.Len(t, eventsA[0].CommittedWriteVersions, 1)
	require.Len(t, eventsB[0].CommittedWriteVersions, 1)
	require.Equal(t, "2", eventsB[0].CommittedWriteVersions[0].NsId)

	eventsA[0].CommittedWriteVersions[0].Writes[0].Version = 99
	require.Zero(t, eventsB[0].CommittedWriteVersions[0].Writes[0].Version)
	require.True(t, proto.Equal(before, block.txs[0].Content))
	require.EqualValues(t, 4, block.blindWriteVersions[0][0].Writes[0].Version)
}

func TestCommittedVersionOmission(t *testing.T) {
	t.Parallel()
	for _, status := range []committerpb.Status{
		committerpb.Status_COMMITTED, committerpb.Status_ABORTED_MVCC_CONFLICT,
		committerpb.Status_ABORTED_SIGNATURE_INVALID, committerpb.Status_REJECTED_DUPLICATE_TX_ID,
	} {
		for _, include := range []bool{false, true} {
			block := versionTestBlock()
			block.statuses[0] = status
			if status != committerpb.Status_COMMITTED {
				block.blindWriteVersions[0] = []*committerpb.NamespaceWriteVersions{nil}
			}

			events, err := (&blockStream{includeReadWriteSets: include}).filterAndBuildEvents(block)
			require.NoError(t, err)
			if !include || status != committerpb.Status_COMMITTED {
				require.Empty(t, events[0].CommittedWriteVersions)
			}
		}
	}
}

func TestCommittedVersionsWithoutBlindMetadata(t *testing.T) {
	t.Parallel()
	block := versionTestBlock()
	block.blindWriteVersions[0] = nil

	events, err := (&blockStream{includeReadWriteSets: true}).filterAndBuildEvents(block)
	require.NoError(t, err)
	require.Len(t, events[0].CommittedWriteVersions, 2)
	for _, ns := range events[0].CommittedWriteVersions {
		require.Len(t, ns.Writes, 1)
		require.Equal(t, []byte("rw"), ns.Writes[0].Key)
	}

	for _, ns := range block.txs[0].Content.Namespaces {
		ns.BlindWrites = nil
	}
	events, err = (&blockStream{includeReadWriteSets: true}).filterAndBuildEvents(block)
	require.NoError(t, err)
	require.Len(t, events[0].CommittedWriteVersions, 2)
}

func TestCommittedVersionRejectsMalformedMetadata(t *testing.T) {
	t.Parallel()
	for _, blind := range [][]*committerpb.NamespaceWriteVersions{
		{nil},
		{{NsId: "unknown"}},
		{{
			NsId:   "1",
			Writes: []*committerpb.CommittedWriteVersion{nil},
		}},
		{{
			NsId:   "1",
			Writes: []*committerpb.CommittedWriteVersion{{Key: []byte("rw")}},
		}},
		{{NsId: "1"}, {NsId: "1"}},
		{{
			NsId:   "1",
			Writes: []*committerpb.CommittedWriteVersion{{Key: []byte("bw")}, {Key: []byte("bw")}},
		}},
	} {
		block := versionTestBlock()
		block.blindWriteVersions[0] = blind

		stream := &blockStream{includeReadWriteSets: true, filterNamespaces: []string{"2"}}
		events, err := stream.filterAndBuildEvents(block)
		require.Error(t, err)
		require.Nil(t, events)
	}
}

func TestCommittedVersionsPreserveWriteOrder(t *testing.T) {
	t.Parallel()
	block := versionTestBlock()
	ns := block.txs[0].Content.Namespaces[0]
	ns.BlindWrites = append(ns.BlindWrites, &applicationpb.Write{Key: []byte("second")})
	blind := block.blindWriteVersions[0]
	blind[0].Writes = append([]*committerpb.CommittedWriteVersion{
		{Key: []byte("second"), Version: 7},
	}, blind[0].Writes...)
	blind[0], blind[1] = blind[1], blind[0]

	events, err := (&blockStream{includeReadWriteSets: true}).filterAndBuildEvents(block)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Len(t, events[0].CommittedWriteVersions, 2)
	test.RequireProtoEqual(t, &committerpb.NamespaceWriteVersions{
		NsId: "1",
		Writes: []*committerpb.CommittedWriteVersion{
			{Key: []byte("rw"), Version: 3},
			{Key: []byte("bw"), Version: 4},
			{Key: []byte("second"), Version: 7},
		},
	}, events[0].CommittedWriteVersions[0])
}

func TestFinalStatusPublishesBlindVersions(t *testing.T) {
	t.Parallel()
	block := &blockWithStatus{
		block:    &common.Block{Header: &common.BlockHeader{Number: 1}},
		txStatus: make([]committerpb.Status, 1), blindWriteVersions: make([][]*committerpb.NamespaceWriteVersions, 1),
	}
	block.pendingCount.Store(1)
	versions := versionTestBlock().blindWriteVersions[0]

	require.NoError(t, block.setFinalStatus(0, committerpb.Status_COMMITTED, versions))
	require.Zero(t, block.pendingCount.Load())
	require.Equal(t, versions, block.blindWriteVersions[0])

	require.Error(t, block.setFinalStatus(0, committerpb.Status_COMMITTED, nil))
	require.Equal(t, versions, block.blindWriteVersions[0])
}

func versionTestBlock() *committedBlockWithTxs {
	return &committedBlockWithTxs{
		header: &common.BlockHeader{Number: 1},
		txs: []*servicepb.TxWithRef{{Ref: committerpb.NewTxRef("tx", 1, 0), Content: &applicationpb.Tx{
			Namespaces: []*applicationpb.TxNamespace{
				{
					NsId: "1",
					ReadWrites: []*applicationpb.ReadWrite{{
						Key: []byte("rw"), Version: new(uint64(2)), Value: []byte("v"),
					}},
					BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: nil}},
				},
				{
					NsId: "2", ReadWrites: []*applicationpb.ReadWrite{{Key: []byte("rw"), Value: []byte("new")}},
					BlindWrites: []*applicationpb.Write{{Key: []byte("bw"), Value: []byte{}}},
				},
			},
		}}},
		statuses: []committerpb.Status{committerpb.Status_COMMITTED},
		blindWriteVersions: [][]*committerpb.NamespaceWriteVersions{{
			{
				NsId:   "1",
				Writes: []*committerpb.CommittedWriteVersion{{Key: []byte("bw"), Version: 4}},
			},
			{
				NsId:   "2",
				Writes: []*committerpb.CommittedWriteVersion{{Key: []byte("bw"), Version: 0}},
			},
		}},
	}
}
