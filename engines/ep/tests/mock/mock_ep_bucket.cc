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

#include "mock_ep_bucket.h"
#include "ep_engine.h"
#include "executorpool.h"
#include "failover-table.h"
#include "mock_checkpoint_manager.h"
#include "mock_item_freq_decayer.h"

void MockEPBucket::createItemFreqDecayerTask() {
    Configuration& config = engine.getConfiguration();
    itemFreqDecayerTask = std::make_shared<MockItemFreqDecayerTask>(
            &engine, config.getItemFreqDecayerPercent());
}

void MockEPBucket::disableItemFreqDecayerTask() {
    ExecutorPool::get()->cancel(itemFreqDecayerTask->getId());
}

MockItemFreqDecayerTask* MockEPBucket::getMockItemFreqDecayerTask() {
    return dynamic_cast<MockItemFreqDecayerTask*>(itemFreqDecayerTask.get());
}

VBucketPtr MockEPBucket::makeVBucket(
        Vbid id,
        vbucket_state_t state,
        KVShard* shard,
        std::unique_ptr<FailoverTable> table,
        NewSeqnoCallback newSeqnoCb,
        std::unique_ptr<Collections::VB::Manifest> manifest,
        vbucket_state_t initState,
        int64_t lastSeqno,
        uint64_t lastSnapStart,
        uint64_t lastSnapEnd,
        uint64_t purgeSeqno,
        uint64_t maxCas,
        int64_t hlcEpochSeqno,
        bool mightContainXattrs,
        const nlohmann::json& replicationTopology) {
    auto vptr = EPBucket::makeVBucket(id,
                                      state,
                                      shard,
                                      std::move(table),
                                      std::move(newSeqnoCb),
                                      std::move(manifest),
                                      initState,
                                      lastSeqno,
                                      lastSnapStart,
                                      lastSnapEnd,
                                      purgeSeqno,
                                      maxCas,
                                      hlcEpochSeqno,
                                      mightContainXattrs,
                                      replicationTopology);
    // Create a MockCheckpointManager.
    vptr->checkpointManager = std::make_unique<MockCheckpointManager>(
            stats,
            id,
            engine.getCheckpointConfig(),
            lastSeqno,
            lastSnapStart,
            lastSnapEnd,
            /*flusher callback*/ nullptr);
    return vptr;
}

void MockEPBucket::setDurabilityCompletionTask(
        std::shared_ptr<DurabilityCompletionTask> task) {
    durabilityCompletionTask = task;
}
