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

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

#include "batch_execution_info.h"

namespace jogasaki::executor::batch {


bool batch_execution_state::error_aborting() const noexcept {
    return error_aborting_;
}

std::shared_ptr<error::error_info> batch_execution_state::error_info() const noexcept {
    return std::atomic_load(std::addressof(error_info_));
}

bool batch_execution_state::set_error_status(status st, std::shared_ptr<error::error_info> info) noexcept {  //NOLINT(performance-unnecessary-value-param)
    error_aborting_ = true;
    std::shared_ptr<error::error_info> s{};
    s = std::atomic_load(std::addressof(error_info_));
    do {
        if (s && (*s)) {
            if(info->status() != status::err_inactive_transaction &&
                info->code() != error_code::inactive_transaction_exception) {
                // Inactive tx occurs very frequentyly, so avoid logging here.
                VLOG_LP(log_error) << "Error " << info->code() << "(\"" << info->message() << "\")"
                                                                                              " is reported subsequently following the original error " << s->code() << ".";
            }
            return false;
        }
    } while (! std::atomic_compare_exchange_strong(std::addressof(error_info_), std::addressof(s), info));
    status_code_ = st;
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

status batch_execution_state::status_code() const noexcept {
    return status_code_;
}

void finish(batch_execution_info const& info, batch_execution_state& state) {
    if(info.completion_callback()) {
        if(state.finish()) {
            info.completion_callback()();
        }
    }
}

}