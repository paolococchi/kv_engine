/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "evp_store_durability_test.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "checkpoint_utils.h"
#include "test_helpers.h"

#include <engines/ep/src/ephemeral_tombstone_purger.h>
#include <programs/engine_testapp/mock_server.h>

class DurabilityEPBucketTest : public STParameterizedBucketTest {
protected:
    void SetUp() {
        STParameterizedBucketTest::SetUp();
        // Add an initial replication topology so we can accept SyncWrites.
        setVBucketToActiveWithValidTopology();
    }

    void setVBucketToActiveWithValidTopology(
            nlohmann::json topology = nlohmann::json::array({{"active",
                                                              "replica"}})) {
        setVBucketStateAndRunPersistTask(
                vbid, vbucket_state_active, {{"topology", topology}});
    }

    /// Test that a prepare of a SyncWrite / SyncDelete is correctly persisted
    /// to disk.
    void testPersistPrepare(DocumentState docState);

    /// Test that a prepare of a SyncWrite / SyncDelete, which is then aborted
    /// is correctly persisted to disk.
    void testPersistPrepareAbort(DocumentState docState);

    /**
     * Test that if a single key is prepared, aborted & re-prepared it is the
     * second Prepare which is kept on disk.
     * @param docState State to use for the second prepare (first is always
     *                 state alive).
     */
    void testPersistPrepareAbortPrepare(DocumentState docState);

    /**
     * Test that if a single key is prepared, aborted re-prepared & re-aborted
     * it is the second Abort which is kept on disk.
     * @param docState State to use for the second prepare (first is always
     *                 state alive).
     */
    void testPersistPrepareAbortX2(DocumentState docState);
};

class DurabilityBucketTest : public STParameterizedBucketTest {
protected:
    template <typename F>
    void testDurabilityInvalidLevel(F& func);
};

class DurabilityEphemeralBucketTest : public STParameterizedBucketTest {
protected:
    template <typename F>
    void testPurgeCompletedPrepare(F& func);
};

void DurabilityEPBucketTest::testPersistPrepare(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto key = makeStoredDocKey("key");
    auto committed = makeCommittedItem(key, "valueA");
    ASSERT_EQ(ENGINE_SUCCESS, store->set(*committed, cookie));
    auto& vb = *store->getVBucket(vbid);
    flushVBucketToDiskIfPersistent(vbid, 1);
    ASSERT_EQ(1, vb.getNumItems());
    auto pending = makePendingItem(key, "valueB");
    if (docState == DocumentState::Deleted) {
        pending->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));

    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, ckptMgr.getNumItemsForPersistence());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    // Committed and Pending will be split into two checkpoints:
    ASSERT_EQ(2, ckptList.size());

    const auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.diskQueueSize);

    // Item must be flushed
    flushVBucketToDiskIfPersistent(vbid, 1);

    // Item must have been removed from the disk queue
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);

    // The item count must not increase when flushing Pending SyncWrites
    EXPECT_EQ(1, vb.getNumItems());

    // @TODO RocksDB
    // @TODO Durability
    // TSan sporadically reports a data race when calling store->get below when
    // running this test under RocksDB. Manifests for both full and value
    // eviction but only seen after adding full eviction variants for this test.
    // Might be the case that running the couchstore full eviction variant
    // beforehand is breaking something.
#ifdef THREAD_SANITIZER
    auto bucketType = std::get<0>(GetParam());
    if (bucketType == "persistentRocksdb") {
        return;
    }
#endif

    // Check the committed item on disk.
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_EQ(*committed, *gv.item);

    // Check the Prepare on disk
    DiskDocKey prefixedKey(key, true /*prepare*/);
    gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_EQ(docState == DocumentState::Deleted, gv.item->isDeleted());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareWrite) {
    testPersistPrepare(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareDelete) {
    testPersistPrepare(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbort(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(0, vb.getNumItems());

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    if (docState == DocumentState::Deleted) {
        pending->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    // A Prepare doesn't account in curr-items
    ASSERT_EQ(0, vb.getNumItems());

    {
        auto res = vb.ht.findForWrite(key);
        ASSERT_TRUE(res.storedValue);
        ASSERT_EQ(CommittedState::Pending, res.storedValue->getCommitted());
        ASSERT_EQ(1, res.storedValue->getBySeqno());
    }
    const auto& stats = engine->getEpStats();
    ASSERT_EQ(1, stats.diskQueueSize);
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    ASSERT_EQ(1, ckptMgr.getNumItemsForPersistence());
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(1, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.front()->getState());
    ASSERT_EQ(1, ckptList.front()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.front()->end()))->getOperation() ==
                      queue_op::pending_sync_write);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // 2 different checkpoints)
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(checkpoint_state::CHECKPOINT_OPEN, ckptList.back()->getState());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::abort_sync_write);
    EXPECT_EQ(2, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(2, stats.diskQueueSize);

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(0, ckptMgr.getNumItemsForPersistence());
    EXPECT_EQ(0, stats.diskQueueSize);

    // At persist-dedup, the Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareWriteAbort) {
    testPersistPrepareAbort(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareDeleteAbort) {
    testPersistPrepareAbort(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbortPrepare(
        DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // First prepare (always a SyncWrite) and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare.
    auto pending2 = makePendingItem(key, "value2");
    if (docState == DocumentState::Deleted) {
        pending2->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending2, cookie));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::pending_sync_write);
    EXPECT_EQ(3, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Abort are in the same key-space, so they will be
    //     deduplicated at Flush
    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Prepare survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isPending());
    EXPECT_EQ(docState == DocumentState::Deleted, gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno(), gv.item->getBySeqno());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepare) {
    testPersistPrepareAbortPrepare(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepareDelete) {
    testPersistPrepareAbortPrepare(DocumentState::Deleted);
}

void DurabilityEPBucketTest::testPersistPrepareAbortX2(DocumentState docState) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // First prepare and abort.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // Second prepare and abort.
    auto pending2 = makePendingItem(key, "value2");
    if (docState == DocumentState::Deleted) {
        pending2->setDeleted(DeleteSource::Explicit);
    }
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending2, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key,
                       {} /*abortSeqno*/,
                       vb.lockCollections(key)));

    // We do not deduplicate Prepare and Abort (achieved by inserting them into
    // different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(4, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    ASSERT_EQ(1,
              (*(--ckptList.back()->end()))->getOperation() ==
                      queue_op::abort_sync_write);
    EXPECT_EQ(4, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Abort are in the same key-space and hence are
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Abort survives
    auto* store = vb.getShard()->getROUnderlying();
    DiskDocKey prefixedKey(key, true /*pending*/);
    auto gv = store->get(prefixedKey, Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isAbort());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(pending2->getBySeqno() + 1, gv.item->getBySeqno());
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortx2) {
    testPersistPrepareAbortX2(DocumentState::Alive);
}

TEST_P(DurabilityEPBucketTest, PersistPrepareAbortPrepareDeleteAbort) {
    testPersistPrepareAbortX2(DocumentState::Deleted);
}

/// Test persistence of a prepared & committed SyncWrite, followed by a
/// prepared & committed SyncDelete.
TEST_P(DurabilityEPBucketTest, PersistSyncWriteSyncDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager (achieved
    // by inserting them into different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    EXPECT_EQ(2, ckptMgr.getNumItemsForPersistence());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // prepare SyncDelete and commit.
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;
    ASSERT_EQ(
            ENGINE_EWOULDBLOCK,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    EXPECT_EQ(1, ckptMgr.getNumItemsForPersistence());

    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key,
                        {} /*commitSeqno*/,
                        vb.lockCollections(key)));

    ASSERT_EQ(4, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
    EXPECT_EQ(1, ckptMgr.getNumItemsForPersistence());

    flushVBucketToDiskIfPersistent(vbid, 1);

    // At persist-dedup, the 2nd Prepare and Commit survive.
    auto* store = vb.getShard()->getROUnderlying();
    auto gv = store->get(DiskDocKey(key), Vbid(0));
    EXPECT_EQ(ENGINE_SUCCESS, gv.getStatus());
    EXPECT_TRUE(gv.item->isCommitted());
    EXPECT_TRUE(gv.item->isDeleted());
    EXPECT_EQ(delInfo.seqno + 1, gv.item->getBySeqno());
}

/// Test SyncDelete on top of SyncWrite
TEST_P(DurabilityBucketTest, SyncWriteSyncDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key, {} /*commitSeqno*/, vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager (achieved
    // by inserting them into different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // prepare SyncDelete and commit.
    uint64_t cas = 0;
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    mutation_descr_t delInfo;

    ASSERT_EQ(1, vb.getNumItems());
    // Ephemeral keeps the completed prepare
    if (persistent()) {
        EXPECT_EQ(0, vb.ht.getNumPreparedSyncWrites());
    } else {
        EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());
    }
    ASSERT_EQ(
            ENGINE_EWOULDBLOCK,
            store->deleteItem(key, cas, vbid, cookie, reqs, nullptr, delInfo));

    EXPECT_EQ(1, vb.getNumItems());
    EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());

    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());

    flushVBucketToDiskIfPersistent(vbid, 1);

    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key, {} /*commitSeqno*/, vb.lockCollections(key)));

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());

    ASSERT_EQ(4, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
}

/// Test delete on top of SyncWrite
TEST_P(DurabilityBucketTest, SyncWriteDelete) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.commit(key, {} /*commitSeqno*/, vb.lockCollections(key)));

    // We do not deduplicate Prepare and Commit in CheckpointManager (achieved
    // by inserting them into different checkpoints)
    const auto& ckptMgr = *store->getVBucket(vbid)->checkpointManager;
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    ckptMgr);
    ASSERT_EQ(2, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());

    // Note: Prepare and Commit are not in the same key-space and hence are not
    //       deduplicated at Flush.
    flushVBucketToDiskIfPersistent(vbid, 2);

    // Perform regular delete.
    uint64_t cas = 0;
    mutation_descr_t delInfo;

    ASSERT_EQ(1, vb.getNumItems());
    auto expectedNumPrepares = persistent() ? 0 : 1;
    EXPECT_EQ(expectedNumPrepares, vb.ht.getNumPreparedSyncWrites());
    ASSERT_EQ(ENGINE_SUCCESS,
              store->deleteItem(key, cas, vbid, cookie, {}, nullptr, delInfo));

    flushVBucketToDiskIfPersistent(vbid, 1);

    EXPECT_EQ(0, vb.getNumItems());
    EXPECT_EQ(expectedNumPrepares, vb.ht.getNumPreparedSyncWrites());

    ASSERT_EQ(3, ckptList.size());
    ASSERT_EQ(1, ckptList.back()->getNumItems());
}

TEST_P(DurabilityEPBucketTest, ActiveLocalNotifyPersistedSeqno) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    const cb::durability::Requirements reqs = {
            cb::durability::Level::PersistToMajority, {}};

    for (uint8_t seqno = 1; seqno <= 3; seqno++) {
        auto item = makePendingItem(
                makeStoredDocKey("key" + std::to_string(seqno)), "value", reqs);
        ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*item, cookie));
    }

    const auto& vb = store->getVBucket(vbid);
    const auto& ckptList =
            CheckpointManagerTestIntrospector::public_getCheckpointList(
                    *vb->checkpointManager);

    auto checkPending = [&ckptList]() -> void {
        ASSERT_EQ(1, ckptList.size());
        const auto& ckpt = *ckptList.front();
        EXPECT_EQ(3, ckpt.getNumItems());
        for (const auto& qi : ckpt) {
            if (!qi->isCheckPointMetaItem()) {
                EXPECT_EQ(queue_op::pending_sync_write, qi->getOperation());
            }
        }
    };

    // No replica has ack'ed yet
    checkPending();

    // Replica acks disk-seqno
    EXPECT_EQ(ENGINE_SUCCESS,
              vb->seqnoAcknowledged("replica", 3 /*preparedSeqno*/));
    // Active has not persisted, so Durability Requirements not satisfied yet
    checkPending();

    // Flusher runs on Active. This:
    // - persists all pendings
    // - and notifies local DurabilityMonitor of persistence
    flushVBucketToDiskIfPersistent(vbid, 3);

    // When seqno:1 is persisted:
    //
    // - the Flusher notifies the local DurabilityMonitor
    // - seqno:1 is satisfied, so it is committed
    // - the open checkpoint containes the seqno:1:prepare, so it is closed and
    //     seqno:1:committed is enqueued in a new open checkpoint (that is how
    //     we avoid SyncWrite de-duplication currently)
    // - the next committed seqnos are enqueued into the same open checkpoint
    //
    // So after Flush we have 2 checkpoints: the first (closed) containing only
    // pending SWs and the second (open) containing only committed SWs
    ASSERT_EQ(2, ckptList.size());

    // Remove the closed checkpoint (that makes the check on Committed easier)
    bool newOpenCkptCreated{false};
    vb->checkpointManager->removeClosedUnrefCheckpoints(*vb,
                                                        newOpenCkptCreated);
    ASSERT_FALSE(newOpenCkptCreated);

    // Durability Requirements satisfied, all committed
    ASSERT_EQ(1, ckptList.size());
    const auto& ckpt = *ckptList.front();
    EXPECT_EQ(3, ckpt.getNumItems());
    for (const auto& qi : ckpt) {
        if (!qi->isCheckPointMetaItem()) {
            EXPECT_EQ(queue_op::commit_sync_write, qi->getOperation());
        }
    }
}

TEST_P(DurabilityEPBucketTest, SetDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->set(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->set(*item, cookie));
}

TEST_P(DurabilityEPBucketTest, AddDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->add(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->add(*item, cookie));
}

TEST_P(DurabilityEPBucketTest, ReplaceDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");

    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE, store->replace(*pending, cookie));

    auto item = makeCommittedItem(key, "value");
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE, store->replace(*item, cookie));
}

TEST_P(DurabilityEPBucketTest, DeleteDurabilityImpossible) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology",
              nlohmann::json::array({{"active", nullptr, nullptr}})}});

    auto key = makeStoredDocKey("key");

    uint64_t cas = 0;
    mutation_descr_t mutation_descr;
    cb::durability::Requirements durabilityRequirements;
    durabilityRequirements.setLevel(cb::durability::Level::Majority);
    EXPECT_EQ(ENGINE_DURABILITY_IMPOSSIBLE,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                durabilityRequirements,
                                nullptr,
                                mutation_descr));

    durabilityRequirements.setLevel(cb::durability::Level::None);
    EXPECT_NE(ENGINE_DURABILITY_IMPOSSIBLE,
              store->deleteItem(key,
                                cas,
                                vbid,
                                cookie,
                                durabilityRequirements,
                                nullptr,
                                mutation_descr));
}

template <typename F>
void DurabilityBucketTest::testDurabilityInvalidLevel(F& func) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto reqs = Requirements(Level::Majority, {});
    auto pending = makePendingItem(key, "value", reqs);
    EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));

    reqs = Requirements(Level::MajorityAndPersistOnMaster, {});
    pending = makePendingItem(key, "value", reqs);
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    }

    reqs = Requirements(Level::PersistToMajority, {});
    pending = makePendingItem(key, "value", reqs);
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, func(pending, cookie));
    }
}

TEST_P(DurabilityBucketTest, SetDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->set(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, AddDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->add(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, ReplaceDurabilityInvalidLevel) {
    auto op = [this](queued_item pending,
                     const void* cookie) -> ENGINE_ERROR_CODE {
        return store->replace(*pending, cookie);
    };
    testDurabilityInvalidLevel(op);
}

TEST_P(DurabilityBucketTest, DeleteDurabilityInvalidLevel) {
    setVBucketStateAndRunPersistTask(vbid, vbucket_state_active);

    using namespace cb::durability;
    auto durabilityRequirements = Requirements(Level::Majority, {});

    auto del = [this](cb::durability::Requirements requirements)
            -> ENGINE_ERROR_CODE {
        auto key = makeStoredDocKey("key");
        uint64_t cas = 0;
        mutation_descr_t mutation_descr;
        return store->deleteItem(
                key, cas, vbid, cookie, requirements, nullptr, mutation_descr);
    };
    EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));

    durabilityRequirements =
            Requirements(Level::MajorityAndPersistOnMaster, {});
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    }

    durabilityRequirements = Requirements(Level::PersistToMajority, {});
    if (persistent()) {
        EXPECT_NE(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    } else {
        EXPECT_EQ(ENGINE_DURABILITY_INVALID_LEVEL, del(durabilityRequirements));
    }
}

TEST_P(DurabilityBucketTest, TakeoverSendsDurabilityAmbiguous) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Make pending
    auto key = makeStoredDocKey("key");
    using namespace cb::durability;
    auto pending = makePendingItem(key, "value");

    // Store it
    EXPECT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));

    // We don't send EWOULDBLOCK to clients
    auto mockCookie = cookie_to_mock_object(cookie);
    EXPECT_EQ(ENGINE_SUCCESS, mockCookie->status);

    // Set state to dead
    EXPECT_EQ(ENGINE_SUCCESS, store->setVBucketState(vbid, vbucket_state_dead));

    // We have set state to dead but we have not yet run the notification task
    EXPECT_EQ(ENGINE_SUCCESS, mockCookie->status);

    auto& lpAuxioQ = *task_executor->getLpTaskQ()[NONIO_TASK_IDX];
    runNextTask(lpAuxioQ);

    // We should have told client the SyncWrite is ambiguous
    EXPECT_EQ(ENGINE_SYNC_WRITE_AMBIGUOUS, mockCookie->status);
}

// Test that if a SyncWrite times out, then a subsequent SyncWrite which
// _should_ fail does indeed fail.
// (Regression test for part of MB-34367 - after using notify_IO_complete
// to report the SyncWrite was timed out with status eambiguous, the outstanding
// cookie context was not correctly cleared.
TEST_P(DurabilityBucketTest, MutationAfterTimeoutCorrect) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});

    // Setup: make pending item and store it; then abort it (at VBucket) level.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    uint64_t cas;
    ASSERT_EQ(ENGINE_EWOULDBLOCK,
              engine->store(cookie,
                            pending.get(),
                            cas,
                            OPERATION_SET,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive));
    ASSERT_TRUE(engine->getEngineSpecific(cookie))
            << "Expected engine specific to be set for cookie after "
               "EWOULDBLOCK";

    auto& vb = *store->getVBucket(vbid);
    ASSERT_EQ(ENGINE_SUCCESS,
              vb.abort(key, {}, vb.lockCollections(key), cookie));

    // Test: Attempt another SyncWrite, which _should_ fail (in this case just
    // use replace against the same non-existent key).
    ASSERT_EQ(ENGINE_KEY_ENOENT,
              engine->store(cookie,
                            pending.get(),
                            cas,
                            OPERATION_REPLACE,
                            pending->getDurabilityReqs(),
                            DocumentState::Alive));
}

template <typename F>
void DurabilityEphemeralBucketTest::testPurgeCompletedPrepare(F& func) {
    setVBucketStateAndRunPersistTask(
            vbid,
            vbucket_state_active,
            {{"topology", nlohmann::json::array({{"active", "replica"}})}});
    auto& vb = *store->getVBucket(vbid);

    // prepare SyncWrite and commit.
    auto key = makeStoredDocKey("key");
    auto pending = makePendingItem(key, "value");
    ASSERT_EQ(ENGINE_EWOULDBLOCK, store->set(*pending, cookie));

    EXPECT_EQ(ENGINE_SUCCESS, func(vb, key));

    EXPECT_EQ(1, vb.ht.getNumPreparedSyncWrites());

    TimeTraveller avenger(10000000);

    EphemeralVBucket::HTTombstonePurger purger(0);
    EphemeralVBucket& evb = dynamic_cast<EphemeralVBucket&>(vb);
    purger.setCurrentVBucket(evb);
    evb.ht.visit(purger);

    EXPECT_EQ(0, vb.ht.getNumPreparedSyncWrites());
}

TEST_P(DurabilityEphemeralBucketTest, PurgeCompletedPrepare) {
    auto op = [this](VBucket& vb, StoredDocKey key) -> ENGINE_ERROR_CODE {
        return vb.commit(key, {} /*commitSeqno*/, vb.lockCollections(key));
    };
    testPurgeCompletedPrepare(op);
}

TEST_P(DurabilityEphemeralBucketTest, PurgeCompletedAbort) {
    auto op = [this](VBucket& vb, StoredDocKey key) -> ENGINE_ERROR_CODE {
        return vb.abort(key, {} /*abortSeqno*/, vb.lockCollections(key));
    };
    testPurgeCompletedPrepare(op);
}

// Test cases which run against all persistent storage backends.
INSTANTIATE_TEST_CASE_P(
        AllBackends,
        DurabilityEPBucketTest,
        STParameterizedBucketTest::persistentAllBackendsConfigValues(),
        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run against all ephemeral.
INSTANTIATE_TEST_CASE_P(AllBackends,
                        DurabilityEphemeralBucketTest,
                        STParameterizedBucketTest::ephConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);

// Test cases which run against all configurations.
INSTANTIATE_TEST_CASE_P(AllBackends,
                        DurabilityBucketTest,
                        STParameterizedBucketTest::allConfigValues(),
                        STParameterizedBucketTest::PrintToStringParamName);
