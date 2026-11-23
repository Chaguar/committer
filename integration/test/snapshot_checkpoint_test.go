/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/Chaguar/common/api/applicationpb"
	"github.com/Chaguar/common/api/committerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgxpool"

	"github.com/Chaguar/committer/api/servicepb"
	"github.com/Chaguar/committer/integration/runner"
	"github.com/Chaguar/committer/loadgen/workload"
	"github.com/Chaguar/committer/service/vc"
	"github.com/Chaguar/committer/utils/statedb"
	"github.com/Chaguar/committer/utils/test"
	"github.com/Chaguar/committer/utils/testdb"
)

func TestSnapshotCheckpoint(t *testing.T) {
	t.Parallel()
	c := runner.NewRuntime(t, &runner.Config{CrashTest: true, NumVerifiers: 2, NumVCService: 2})
	// Register both namespaces so TxBuilder emits endorsements; absent entries leave them unsigned.
	// nil selects default MSP signing with generated peer identities, not disabled verification.
	// The verifier still enforces SnapshotEndorsement/CheckpointEndorsement from the config block.
	c.SystemConfig.Policy.NamespacePolicies[committerpb.SnapshotNamespaceID] = nil
	c.SystemConfig.Policy.NamespacePolicies[committerpb.CheckpointNamespaceID] = nil
	var err error
	c.TxBuilder, err = workload.NewTxBuilderFromPolicy(c.SystemConfig.Policy, nil)
	require.NoError(t, err)
	testdb.EnsureSnapshotSchedule(t, c.DBEnv.DBConf.Database)
	c.OrdererEnv.StartServers(t)
	c.Start(t, runner.CommitterTxPath|runner.SnapshotHasher)
	c.CreateNamespacesAndCommit(t, "1", "2")

	live, err := statedb.NewPool(t.Context(), c.DBEnv.DBConf)
	require.NoError(t, err)
	t.Cleanup(live.Close)
	states := statedb.NewSnapshotStateManager(live, c.DBEnv.DBConf.Retry)

	// Sequential lifecycles avoid cluster-global clone-name collisions at equal block heights.
	//nolint:paralleltest // Each case checkpoints before the next snapshot.
	for _, tc := range []struct {
		name     string
		position int
	}{
		{name: "snapshot only", position: -1},
		{name: "snapshot first", position: 0},
		{name: "snapshot middle", position: 1},
		{name: "snapshot last", position: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: Seed data that the clone must preserve or update at the snapshot boundary.
			t.Log("Step 1: Commit 1,000 keys before the snapshot block")
			// ns1 overwrite: "before" -> "snapshot block" (mixed cases) -> "after" -> "checkpointed".
			// The clone must retain the snapshot-boundary value, not the later live-state updates.
			updatedKey := []byte(tc.name + "-key")
			// New ns2 row in the snapshot block: the clone must include "same block" at every marker position.
			snapshotBlockKey := []byte(tc.name + "-block")
			// New ns1 row after the snapshot: live state contains "later", but the clone must exclude it.
			postSnapshotKey := []byte(tc.name + "-later")
			// All other seeded rows must retain their original values and version 0 in the clone.
			keys, seedRows := seedSnapshotTestKeys(t, c, tc.name)

			// Step 2: The marker's position must not change which ordinary writes reach the clone.
			t.Log("Step 2: Commit snapshot block")
			snapshot := c.TxBuilder.MakeTx(&applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{{
				NsId: committerpb.SnapshotNamespaceID,
			}}})
			txs := []*servicepb.LoadGenTx{snapshot}
			cloneValue := []byte("before")
			cloneVersion := uint64(0)
			if tc.position >= 0 {
				txs = []*servicepb.LoadGenTx{
					c.TxBuilder.MakeTx(&applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{{
						NsId: "1", ReadWrites: []*applicationpb.ReadWrite{{
							Key: updatedKey, Value: []byte("snapshot block"), Version: new(uint64(0)),
						}},
					}}}),
					c.TxBuilder.MakeTx(&applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{{
						NsId: "2", BlindWrites: []*applicationpb.Write{{
							Key: snapshotBlockKey, Value: []byte("same block"),
						}},
					}}}),
				}
				txs = slices.Insert(txs, tc.position, snapshot)
				cloneValue = []byte("snapshot block")
				cloneVersion = 1
			}
			//nolint:gosec // Marker positions are bounded to 0..2 by the test table.
			ref := &committerpb.TxRef{
				TxId: snapshot.Id, BlockNum: c.NextExpectedBlockNumber, TxNum: uint32(max(tc.position, 0)),
			}
			cloneName := fmt.Sprintf("snapshot_%d", ref.BlockNum)
			t.Cleanup(func() {
				// The test context is cancelled before cleanup, including on failed hashing.
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, statedb.AdminExec(ctx, c.DBEnv.DBConf,
					fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{cloneName}.Sanitize())))
			})
			submitSnapshotTestBlock(t, c, txs)

			// Step 3: Writes after the barrier must keep flowing without leaking into the clone.
			t.Log("Step 3: Commit later writes while hashing runs")
			c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{{
				NsId: "1", ReadWrites: []*applicationpb.ReadWrite{{
					Key: updatedKey, Value: []byte("after"), Version: new(cloneVersion),
				}}, BlindWrites: []*applicationpb.Write{{Key: postSnapshotKey, Value: []byte("later")}},
			}}}, []committerpb.Status{committerpb.Status_COMMITTED})
			state := requireSnapshotTestState(t, states, ref, committerpb.SnapshotState_COMPLETED)
			require.Equal(t, cloneName, state.CloneDatabase)
			require.Len(t, state.Hash, sha256.Size)
			require.Empty(t, state.Error)

			cloneConfig := *c.DBEnv.DBConf
			cloneConfig.Database = state.CloneDatabase
			clone, poolErr := statedb.NewPool(t.Context(), &cloneConfig)
			require.NoError(t, poolErr)
			t.Cleanup(clone.Close)
			keys = append(keys, postSnapshotKey)
			cloneRows := maps.Clone(seedRows)
			cloneRows[string(updatedKey)] = &vc.ValueVersion{Value: cloneValue, Version: cloneVersion}
			requireSnapshotTestRows(t, clone, "1", keys, cloneRows)
			liveRows := maps.Clone(seedRows)
			liveRows[string(updatedKey)] = &vc.ValueVersion{Value: []byte("after"), Version: cloneVersion + 1}
			liveRows[string(postSnapshotKey)] = &vc.ValueVersion{Value: []byte("later"), Version: 0}
			requireSnapshotTestRows(t, live, "1", keys, liveRows)
			blockRows := map[string]*vc.ValueVersion{}
			if tc.position >= 0 {
				blockRows[string(snapshotBlockKey)] = &vc.ValueVersion{Value: []byte("same block"), Version: 0}
			}
			requireSnapshotTestRows(t, clone, "2", [][]byte{snapshotBlockKey}, blockRows)

			// Step 4: The actual hasher digest must be accepted and persisted by the full pipeline.
			t.Log("Step 4: Commit matching checkpoint")
			checkpointKey := servicepb.CheckpointKey(ref.BlockNum)
			checkpoint := c.TxBuilder.MakeTx(&applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{{
				NsId:       committerpb.CheckpointNamespaceID,
				ReadWrites: []*applicationpb.ReadWrite{{Key: checkpointKey, Value: state.Hash}},
			}}})
			submitSnapshotTestBlock(t, c, []*servicepb.LoadGenTx{checkpoint})
			checkpointed := requireSnapshotTestState(t, states, ref, committerpb.SnapshotState_CHECKPOINTED)
			require.Equal(t, state.Hash, checkpointed.Hash)
			require.Equal(t, state.CloneDatabase, checkpointed.CloneDatabase)
			requireSnapshotTestRows(t, live, committerpb.CheckpointNamespaceID, [][]byte{checkpointKey},
				map[string]*vc.ValueVersion{string(checkpointKey): {Value: state.Hash, Version: 0}})
			requireSnapshotTestRows(t, clone, committerpb.CheckpointNamespaceID, [][]byte{checkpointKey},
				map[string]*vc.ValueVersion{})

			// Step 5: A checkpoint must release intake for ordinary transactions and the next lifecycle.
			t.Log("Step 5: Commit another ordinary write after checkpointing")
			c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{{
				NsId: "1", ReadWrites: []*applicationpb.ReadWrite{{
					Key: updatedKey, Value: []byte("checkpointed"), Version: new(cloneVersion + 1),
				}},
			}}}, []committerpb.Status{committerpb.Status_COMMITTED})
		})
	}
}

func seedSnapshotTestKeys(
	t *testing.T, c *runner.CommitterRuntime, prefix string,
) ([][]byte, map[string]*vc.ValueVersion) {
	t.Helper()
	const numKeys = 1000
	writes := make([]*applicationpb.Write, 0, numKeys)
	writes = append(
		writes,
		&applicationpb.Write{Key: []byte(prefix + "-key"), Value: []byte("before")},
		&applicationpb.Write{Key: []byte(prefix + "-retained"), Value: []byte("retained")},
	)
	for i := range numKeys - len(writes) {
		writes = append(writes, &applicationpb.Write{
			Key:   fmt.Appendf(nil, "%s-seed-%04d", prefix, i),
			Value: fmt.Appendf(nil, "value-%04d", i),
		})
	}
	keys := make([][]byte, 0, numKeys+1)
	expected := make(map[string]*vc.ValueVersion, numKeys)
	for _, write := range writes {
		keys = append(keys, write.Key)
		expected[string(write.Key)] = &vc.ValueVersion{Value: write.Value, Version: 0}
	}
	require.Len(t, expected, numKeys)
	c.MakeAndSendTransactionsToOrderer(t, [][]*applicationpb.TxNamespace{{{
		NsId: "1", BlindWrites: writes,
	}}}, []committerpb.Status{committerpb.Status_COMMITTED})
	return keys, expected
}

func submitSnapshotTestBlock(t *testing.T, c *runner.CommitterRuntime, txs []*servicepb.LoadGenTx) {
	t.Helper()
	expected := &runner.ExpectedStatusInBlock{}
	for _, tx := range txs {
		expected.TxIDs = append(expected.TxIDs, tx.Id)
		expected.Statuses = append(expected.Statuses, committerpb.Status_COMMITTED)
	}
	// Direct submission fixes block composition; the orderer's normal cutter uses size one.
	require.NoError(t, c.OrdererEnv.Orderer.SubmitBlock(t.Context(), workload.MapToOrdererBlock(0, txs)))
	c.ValidateExpectedResultsInCommittedBlock(t, expected)
}

func requireSnapshotTestState(
	t *testing.T, states *statedb.SnapshotStateManager, ref *committerpb.TxRef, status committerpb.SnapshotState_Status,
) *committerpb.SnapshotState {
	t.Helper()
	var state *committerpb.SnapshotState
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		var err error
		state, err = states.Read(ctx, ref.TxId)
		require.NoError(ct, err)
		test.RequireProtoEqual(ct, ref, state.TxRef)
		require.Equal(ct, status, state.Status, "snapshot error: %s", state.Error)
	}, 2*time.Minute, 100*time.Millisecond)
	return state
}

//nolint:revive // Explicit namespace and key selection also asserts missing rows.
func requireSnapshotTestRows(
	t *testing.T, pool *pgxpool.Pool, namespace string, keys [][]byte, expected map[string]*vc.ValueVersion,
) {
	t.Helper()
	query := fmt.Sprintf("SELECT key, value, version FROM %s WHERE key = ANY($1)",
		pgx.Identifier{statedb.TableName(namespace)}.Sanitize())
	rows, err := pool.Query(t.Context(), query, keys)
	require.NoError(t, err)
	defer rows.Close()
	actual := map[string]*vc.ValueVersion{}
	for rows.Next() {
		var key []byte
		value := &vc.ValueVersion{}
		require.NoError(t, rows.Scan(&key, &value.Value, &value.Version))
		actual[string(key)] = value
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, actual)
}
