/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "checkpoint_remover.h"
#include "checkpoint_visitor.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>
#include <memory>

std::pair<bool, size_t>
ClosedUnrefCheckpointRemoverTask::isReductionInCheckpointMemoryNeeded() const {
    /**
     * Cursor dropping will commence if one of the following conditions is met:
     * 1. if the total memory used is greater than the upper threshold which is
     * a percentage of the quota, specified by cursor_dropping_upper_mark
     * 2. if the overall checkpoint memory usage goes above a certain % of the
     * bucket quota, specified by cursor_dropping_checkpoint_mem_upper_mark
     *
     * Once cursor dropping starts, it will continue until memory usage is
     * projected to go under the lower threshold, either
     * cursor_dropping_lower_mark or cursor_dropping_checkpoint_mem_lower_mark
     * based on the trigger condition.
     */
    const auto& config = engine->getConfiguration();
    const auto bucketQuota = config.getMaxSize();

    const auto vBucketChkptMemSize =
            engine->getKVBucket()
                    ->getVBuckets()
                    .getVBucketsTotalCheckpointMemoryUsage();

    const auto chkptMemLimit =
            (bucketQuota * config.getCursorDroppingCheckpointMemUpperMark()) /
            100;

    const bool hitCheckpointMemoryThreshold =
            vBucketChkptMemSize >= chkptMemLimit;

    const bool aboveLowWatermark =
            stats.getEstimatedTotalMemoryUsed() >= stats.mem_low_wat.load();

    const bool ckptMemExceedsCheckpointMemoryThreshold =
            aboveLowWatermark && hitCheckpointMemoryThreshold;

    const bool memUsedExceedsCursorDroppingUpperMark =
            stats.getEstimatedTotalMemoryUsed() >
            stats.cursorDroppingUThreshold.load();

    auto toMB = [](size_t bytes) { return bytes / (1024 * 1024); };
    if (memUsedExceedsCursorDroppingUpperMark ||
        ckptMemExceedsCheckpointMemoryThreshold) {
        size_t amountOfMemoryToClear;

        if (ckptMemExceedsCheckpointMemoryThreshold) {
            // If we were triggered by the fact we hit the low watermark and we
            // are at or over the threshold of allowed checkpoint memory usage,
            // then try to clear memory down to the lower limit of the allowable
            // memory usage threshold.
            amountOfMemoryToClear =
                    stats.getEstimatedTotalMemoryUsed() -
                    ((bucketQuota *
                      config.getCursorDroppingCheckpointMemLowerMark()) /
                     100);
            EP_LOG_INFO(
                    "Triggering memory recovery as checkpoint_memory ({} MB) "
                    "exceeds cursor_dropping_checkpoint_mem_upper_mark ({}%, "
                    "{} MB). Attempting to free {} MB of memory.",
                    toMB(vBucketChkptMemSize),
                    config.getCursorDroppingCheckpointMemUpperMark(),
                    toMB(chkptMemLimit),
                    toMB(amountOfMemoryToClear));

        } else {
            amountOfMemoryToClear = stats.getEstimatedTotalMemoryUsed() -
                                    stats.cursorDroppingLThreshold.load();
            EP_LOG_INFO(
                    "Triggering memory recovery as mem_used ({} MB) "
                    "exceeds cursor_dropping_upper_mark ({}%, {} MB). "
                    "Attempting to free {} MB of memory.",
                    toMB(stats.getEstimatedTotalMemoryUsed()),
                    config.getCursorDroppingUpperMark(),
                    toMB(stats.cursorDroppingUThreshold.load()),
                    toMB(amountOfMemoryToClear));
        }
        // Memory recovery is required.
        return std::make_pair(true, amountOfMemoryToClear);
    }
    // Memory recovery is not required.
    return std::make_pair(false, 0);
}

size_t ClosedUnrefCheckpointRemoverTask::attemptMemoryRecovery(
        MemoryRecoveryMechanism mechanism, size_t amountOfMemoryToClear) {
    size_t memoryCleared = 0;
    KVBucketIface* kvBucket = engine->getKVBucket();
    // Get a list of vbuckets sorted by memory usage
    // of their respective checkpoint managers.
    auto vbuckets = kvBucket->getVBuckets().getVBucketsSortedByChkMgrMem();
    for (const auto& it : vbuckets) {
        if (memoryCleared >= amountOfMemoryToClear) {
            break;
        }
        Vbid vbid = it.first;
        VBucketPtr vb = kvBucket->getVBucket(vbid);
        if (!vb) {
            continue;
        }
        switch (mechanism) {
        case MemoryRecoveryMechanism::checkpointExpel: {
            auto expelResult =
                    vb->checkpointManager->expelUnreferencedCheckpointItems();
            EP_LOG_DEBUG(
                    "Expelled {} unreferenced checkpoint items "
                    "from {} "
                    "and estimated to have recovered {} bytes.",
                    expelResult.expelCount,
                    vb->getId(),
                    expelResult.estimateOfFreeMemory);
            memoryCleared += expelResult.estimateOfFreeMemory;
            break;
        }
        case MemoryRecoveryMechanism::cursorDrop: {
            // Get a list of cursors that can be dropped from the
            // vbucket's checkpoint manager, so as to unreference
            // an estimated number of checkpoints.
            auto cursors = vb->checkpointManager->getListOfCursorsToDrop();
            for (const auto& cursor : cursors) {
                if (memoryCleared < amountOfMemoryToClear) {
                    if (engine->getDcpConnMap().handleSlowStream(
                                vbid, cursor.lock().get())) {
                        auto memoryFreed =
                                vb->getChkMgrMemUsageOfUnrefCheckpoints();
                        ++stats.cursorsDropped;
                        stats.cursorMemoryFreed += memoryFreed;
                        memoryCleared += memoryFreed;
                    }
                } else { // memoryCleared >= amountOfMemoryToClear
                    break;
                }
            }
        } // case cursorDrop
        } // switch (mechanism)
    }
    return memoryCleared;
}

bool ClosedUnrefCheckpointRemoverTask::run(void) {
    TRACE_EVENT0("ep-engine/task", "ClosedUnrefCheckpointRemoverTask");
    bool inverse = true;
    if (available.compare_exchange_strong(inverse, false)) {
        bool shouldReduceMemory{false};
        size_t amountOfMemoryToClear{0};
        size_t amountOfMemoryRecovered{0};

        std::tie(shouldReduceMemory, amountOfMemoryToClear) =
                isReductionInCheckpointMemoryNeeded();
        if (shouldReduceMemory) {
            // Try expelling first, if enabled
            if (engine->getConfiguration().isChkExpelEnabled()) {
                amountOfMemoryRecovered = attemptMemoryRecovery(
                        MemoryRecoveryMechanism::checkpointExpel,
                        amountOfMemoryToClear);
            }
            // If still need to recover more memory, drop cursors
            if (amountOfMemoryToClear > amountOfMemoryRecovered) {
                attemptMemoryRecovery(
                        MemoryRecoveryMechanism::cursorDrop,
                        amountOfMemoryToClear - amountOfMemoryRecovered);
            }
        }
        KVBucketIface* kvBucket = engine->getKVBucket();

        auto pv =
                std::make_unique<CheckpointVisitor>(kvBucket, stats, available);

        // Empirical evidence from perf runs shows that 99.9% of "Checkpoint
        // Remover" task should complete under 50ms
        const auto maxExpectedDurationForVisitorTask =
                std::chrono::milliseconds(50);

        kvBucket->visitAsync(std::move(pv),
                             "Checkpoint Remover",
                             TaskId::ClosedUnrefCheckpointRemoverVisitorTask,
                             maxExpectedDurationForVisitorTask);
    }
    snooze(sleepTime);
    return true;
}
