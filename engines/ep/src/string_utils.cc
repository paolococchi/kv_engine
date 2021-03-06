/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "string_utils.h"

bool cb_stob(const std::string& s) {
    if (s == "true") {
        return true;
    } else if (s == "false") {
        return false;
    } else {
        throw invalid_argument_bool("Argument was not `true` or `false`");
    }
}

bool cb_isPrefix(const std::string& input, const std::string& prefix) {
    return (input.compare(0, prefix.length(), prefix) == 0);
}

bool cb_isPrefix(cb::const_char_buffer input, const std::string& prefix) {
    if (prefix.size() > input.size()) {
        return false;
    }

    return std::memcmp(input.data(), prefix.data(), prefix.size()) == 0;
}
