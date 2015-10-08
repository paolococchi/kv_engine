/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "platform/platform.h"
#include "common.h"

/**
 *   Reader/Write lock abstraction for platform provided cb_rw_lock
 *
 *   The lock allows many readers but mutual exclusion with a writer.
 */
class RWLock {
public:
    RWLock() {
        cb_rw_lock_initialize(&lock);
    }

    ~RWLock() {
        cb_rw_lock_destroy(&lock);
    }

    int readerLock() {
        return cb_rw_reader_enter(&lock);
    }

    int readerUnlock() {
        return cb_rw_reader_exit(&lock);
    }

    int writerLock() {
        return cb_rw_writer_enter(&lock);
    }

    int writerUnlock() {
        return cb_rw_writer_exit(&lock);
    }

private:
    cb_rwlock_t lock;

    DISALLOW_COPY_AND_ASSIGN(RWLock);
};
