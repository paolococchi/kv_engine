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

#pragma once

#include "globaltask.h"

#include <folly/Synchronized.h>

class BackfillManager;

using Managers =
        std::unordered_map<std::string, std::weak_ptr<BackfillManager>>;

class BackfillTask : public GlobalTask {
public:
    BackfillTask(EventuallyPersistentEngine& e,
                 double sleeptime = 0,
                 bool completeBeforeShutdown = false)
        : GlobalTask(
                  &e, TaskId::BackfillTask, sleeptime, completeBeforeShutdown) {
    }

    // @todo
    void queue(const std::string& connection,
               std::weak_ptr<BackfillManager> manager);

    bool run();

    std::string getDescription();

    std::chrono::microseconds maxExpectedDuration();

    static auto getSleepTime() {
        return sleepTime;
    }

private:
    // @todo: update comment
    //
    // A weak pointer to the backfill manager which owns this
    // task. The manager is owned by the DcpProducer, but we need to
    // give the BackfillManagerTask access to the manager as it runs
    // concurrently in a different thread.
    // If the manager is deleted (by the DcpProducer) then the
    // ManagerTask simply cancels itself and stops running.
    folly::Synchronized<Managers> managers;

    static constexpr uint8_t sleepTime = 1;
};
