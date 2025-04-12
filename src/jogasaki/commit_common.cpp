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
#include <jogasaki/external_log/events.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/external_log_utils.h>
#include <jogasaki/utils/hex.h>

namespace jogasaki {

void log_end_of_tx(
    transaction_context& tx,
    bool aborted,
    request_info const& req_info
) {
    tx.end_time(transaction_context::clock::now());
    auto txid = tx.transaction_id();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
        << txid
        << " status:"
        << (aborted ? "aborted" : "committed");
    auto tx_type = utils::tx_type_from(tx);
    auto result = aborted ? external_log::result_value::fail : external_log::result_value::success;
    external_log::tx_end(
        req_info,
        "",
        txid,
        tx_type,
        result,
        tx.duration<std::chrono::nanoseconds>().count(),
        tx.label()
    );
}

void log_end_of_commit_request(request_context& rctx) {
    auto txid = rctx.transaction()->transaction_id();
    auto jobid = rctx.job()->id();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:committed "
        << txid
        << " job_id:"
        << utils::hex(jobid);
    rctx.transaction()->profile()->set_commit_job_completed();
}

void log_end_of_tx_and_commit_request(request_context& rctx) {
    log_end_of_commit_request(rctx);
    log_end_of_tx(*rctx.transaction(), rctx.status_code() != status::ok, rctx.req_info());
}

}  // namespace jogasaki
