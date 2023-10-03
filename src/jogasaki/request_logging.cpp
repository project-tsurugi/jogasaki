/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "request_logging.h"

#include <string_view>
#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/utils/convert_control_characters.h>

namespace jogasaki {

static constexpr std::string_view timing_job_accepted = "/:jogasaki:timing:job_accepted";
static constexpr std::string_view timing_job_submitting = "/:jogasaki:timing:job_submitting";
static constexpr std::string_view timing_job_started = "/:jogasaki:timing:job_started";
static constexpr std::string_view timing_job_finishing = "/:jogasaki:timing:job_finishing";

std::string_view if_empty(std::string_view arg) {
    if(arg.empty()) return "<empty>";
    return arg;
}

std::string_view trim_string(std::string_view arg) {
    constexpr static std::size_t str_len_request_log = 32;
    if(arg.size() > str_len_request_log) {
        return {arg.data(), str_len_request_log};
    }
    return arg;
}

void log_request(const scheduler::request_detail &req, bool success) {
    if(req.status() == scheduler::request_detail_status::accepted) {
        VLOG(log_debug_timing_event_fine) << timing_job_accepted
            << " job_id:" << utils::hex(req.id())
            << " kind:" << req.kind()
            << " tx:" << if_empty(req.transaction_id())
            << " sql:{" << utils::convert_control_characters(if_empty(trim_string(req.statement_text()))) << "}"
            << " tx_options:{" << if_empty(req.transaction_option_spec()) << "}"
            ;
        return;
    }
    if(req.status() == scheduler::request_detail_status::submitted) {
        VLOG(log_debug_timing_event_fine) << timing_job_submitting
            << " "
            << "job_id:" << utils::hex(req.id());
        return;
    }
    if(req.status() == scheduler::request_detail_status::executing) {
        VLOG(log_debug_timing_event_fine) << timing_job_started
            << " "
            << "job_id:" << utils::hex(req.id());
        return;
    }
    if(req.status() == scheduler::request_detail_status::finishing) {
        VLOG(log_debug_timing_event_fine) << timing_job_finishing
            << " job_id:" << utils::hex(req.id())
            << " status:" << (success ? "success" : "failure")  //NOLINT
            << " hybrid_execution_mode:" << req.hybrid_execution_mode();  //NOLINT

        return;
    }
}
} // namespace
