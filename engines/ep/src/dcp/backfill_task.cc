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

#include "backfill_task.h"
#include "backfill-manager.h"
#include "ep_engine.h"

#include <phosphor/phosphor.h>

void BackfillTask::queue(const std::string& connection,
                         std::weak_ptr<BackfillManager> manager) {
    // Add to map if entry does not exist, NOP otherwise
    managers.wlock()->emplace(connection, manager);
}

bool BackfillTask::run() {
    TRACE_EVENT0("ep-engine/task", "BackfillTask");

    // @todo: Issue: need to minimize the scope of the exclusive lock,
    //     queue() will potentially block for long otherwise
    auto& map = *managers.wlock();

    for (const auto& pair : map) {
        // Lock weak pointer and cleanup if the BackfillManager no longer exists
        auto manager = pair.second.lock();
        if (!manager) {
            map.erase(pair.first);
            continue;
        }

        auto status = manager->backfill();

        switch (status) {
        case backfill_success:
            // The previous code does nothing in this case, but need to
            // check all the usages of it.
            break;
        case backfill_finished:
            continue;
        case backfill_snooze:
            snooze(1 /*secs*/);
            break;
        }

        if (engine->getEpStats().isShutdown) {
            return false;
        }
    }

    if (map.empty()) {
        // No backfills left, cancel ourself and stop running.
        cancel();
        return false;
    }

    return true;
}

std::string BackfillTask::getDescription() {
    // @todo: update
    return "Backfilling items for a DCP Connection";
}

std::chrono::microseconds BackfillTask::maxExpectedDuration() {
    // Empirical evidence suggests this task runs under 300ms 99.999% of
    // the time.
    return std::chrono::milliseconds(300);
}
