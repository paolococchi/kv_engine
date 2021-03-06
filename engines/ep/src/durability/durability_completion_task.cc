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

#include "durability_completion_task.h"

#include "ep_engine.h"
#include "executorpool.h"
#include "vbucket.h"

#include <climits>

using namespace std::chrono_literals;

DurabilityCompletionTask::DurabilityCompletionTask(
        EventuallyPersistentEngine& engine)
    : GlobalTask(&engine, TaskId::DurabilityCompletionTask),
      pendingVBs(engine.getConfiguration().getMaxVbuckets()),
      vbid(0) {
    for (auto& vb : pendingVBs) {
        vb.store(false);
    }
}

bool DurabilityCompletionTask::run() {
    if (engine->getEpStats().isShutdown) {
        return false;
    }

    // Start by putting ourselves back to sleep once run() completes.
    // If a new VB is notified (or a VB is re-notified after it is processed in
    // the loop below) then that will cause the task to be re-awoken.
    snooze(INT_MAX);
    // Clear the wakeUpScheduled flag - that allows notifySyncWritesToComplete()
    // to wake up (re-schedule) this task if new vBuckets have SyncWrites which
    // need completing.
    wakeUpScheduled.store(false);

    const auto startTime = std::chrono::steady_clock::now();

    // Loop for each vBucket, starting from where we previously left off.
    // For each vbucket, if the pending flag is set then clear it, and process
    // its resolved SyncWrites.
    for (size_t count = 0; count < pendingVBs.size();
         count++, vbid = (vbid + 1) % pendingVBs.size()) {
        if (pendingVBs[vbid].exchange(false)) {
            auto vb = engine->getVBucket(Vbid(vbid));
            if (vb) {
                vb->processResolvedSyncWrites();
            }
        }
        // Yield back to scheduler if we have exceeded the maximum runtime
        // for a single execution.
        auto runtime = std::chrono::steady_clock::now() - startTime;
        if (runtime > maxChunkDuration) {
            wakeUp();
            break;
        }
    }

    return true;
}

void DurabilityCompletionTask::notifySyncWritesToComplete(Vbid vbid) {
    bool expected = false;
    if (pendingVBs[vbid.get()].compare_exchange_strong(expected, true)) {
        // This VBucket transitioned from false -> true - wake ourselves up so
        // we can start to process the SyncWrites.
        expected = false;

        // Performance: Only wake up the task once (and don't repeatedly try to
        // wake if it's already scheduled to wake) - ExecutorPool::wake() isn't
        // super cheap so avoid it if already pending.
        if (wakeUpScheduled.compare_exchange_strong(expected, true)) {
            ExecutorPool::get()->wake(getId());
        }
    }
}

const std::chrono::steady_clock::duration
        DurabilityCompletionTask::maxChunkDuration = 25ms;
