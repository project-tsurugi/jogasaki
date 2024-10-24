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
#include "commit_common.h"

#include <atomic>
#include <memory>

#include <jogasaki/commit_response.h>
#include <jogasaki/external_log/event_logging.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/external_log_utils.h>
#include <jogasaki/utils/hex.h>

namespace jogasaki {

void log_commit_end(request_context& rctx) {
    auto txid = rctx.transaction()->transaction_id();
    auto jobid = rctx.job()->id();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:committed "
        << txid
        << " job_id:"
        << utils::hex(jobid);
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
        << txid
        << " status:"
        << (rctx.status_code() == status::ok ? "committed" : "aborted");
    rctx.transaction()->profile()->set_commit_job_completed();
    auto tx_type = utils::tx_type_from(*rctx.transaction());
    auto result = utils::result_from(rctx.status_code());
    rctx.transaction()->end_time(transaction_context::clock::now());
    external_log::tx_end(
        rctx.req_info(),
        "",
        txid,
        tx_type,
        result,
        rctx.transaction()->duration<std::chrono::nanoseconds>().count(),
        rctx.transaction()->label()
    );
}

}  // namespace jogasaki
