/*
 * Copyright 2018-2020 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "batch_execution_state.h"

#include <cstddef>
#include <atomic>
#include <string>

#include "batch_execution_info.h"

namespace jogasaki::executor::batch {


bool batch_execution_state::error_aborting() const noexcept {
    return error_aborting_;
}

std::pair<status, std::string> batch_execution_state::error_info() const noexcept {
    return {status_code_, status_message_};
}

bool batch_execution_state::error_info(status val, std::string_view msg) noexcept {
    error_aborting_ = true;
    status s;
    do {
        s = status_code_.load();
        if (s != status::ok) {
            return false;
        }
    } while (!status_code_.compare_exchange_strong(s, val));

    if(val != status::ok) {  // to ensure status::ok has no error msg
        status_message_.assign(msg);
    }
    return true;
}

std::atomic_size_t &batch_execution_state::running_statements() noexcept {
    return running_statements_;
}

bool batch_execution_state::finish() noexcept {
    bool s{};
    do {
        s = finished_.load();
        if (s) {
            return false;
        }
    } while (! finished_.compare_exchange_strong(s, true));
    return true;
}

bool batch_execution_state::finished() const noexcept {
    return finished_;
}

void finish(batch_execution_info const& info, batch_execution_state& state) {
    if(info.completion_callback()) {
        if(state.finish()) {
            info.completion_callback()();
        }
    }
}

}