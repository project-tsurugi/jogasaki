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
#include "event_logging.h"

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

#ifdef ENABLE_ALTIMETER
#include "details/altimeter_event_logging.h"
#endif

namespace jogasaki::external_log {

void tx_start(
    request_context const& rctx,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type
) {
    if(rctx.configuration() && rctx.configuration()->trace_external_log()) {
        VLOG_LP(log_info) <<
            "message:\"" << message << "\"" <<
            " tx_id:" << tx_id <<
            " tx_type:" << tx_type <<
            "";
    }
#ifdef ENABLE_ALTIMETER
    details::tx_start(rctx, message, tx_id, tx_type);
#endif
}

void tx_end(
    request_context const& rctx,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::int64_t result
) {
    if(rctx.configuration() && rctx.configuration()->trace_external_log()) {
        VLOG_LP(log_info) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " result:" << result <<
        "";
    }
#ifdef ENABLE_ALTIMETER
    details::tx_end(rctx, message, tx_id, tx_type, result);
#endif
}

void stmt_start(
    request_context const& rctx,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view statement,
    std::string_view parameter
) {
    if(rctx.configuration() && rctx.configuration()->trace_external_log()) {
        VLOG_LP(log_info) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " job_id:" << job_id <<
        " statement:\"" << statement << "\"" <<
        " parameter:\"" << parameter << "\"" <<
        "";
    }
#ifdef ENABLE_ALTIMETER
    details::stmt_start(rctx, message, tx_id, tx_type, job_id, statement, parameter);
#endif
}

void stmt_end(
    request_context const& rctx,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view statement,
    std::string_view parameter,
    std::int64_t result,
    std::string_view state_code,
    std::int64_t fetched,
    std::int64_t inserted,
    std::int64_t updated,
    std::int64_t deleted,
    std::int64_t merged
) {
    if(rctx.configuration() && rctx.configuration()->trace_external_log()) {
        VLOG_LP(log_info) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " job_id:" << job_id <<
        " statement:\"" << statement << "\"" <<
        " parameter:\"" << parameter << "\"" <<
        " result:" << result <<
        " state_code:" << state_code <<
        " fetched:" << fetched <<
        " inserted:" << inserted <<
        " updated:" << updated <<
        " deleted:" << deleted <<
        " merged:" << merged <<
        "";
    }
#ifdef ENABLE_ALTIMETER
    details::stmt_end(
        rctx,
        message,
        tx_id,
        tx_type,
        job_id,
        statement,
        parameter,
        result,
        state_code,
        fetched,
        inserted,
        updated,
        deleted,
        merged
    );
#endif
}

void stmt_explain(
    request_context const& rctx,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view data
) {
    if(rctx.configuration() && rctx.configuration()->trace_external_log()) {
        VLOG_LP(log_info) <<
        "tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " job_id:" << job_id <<
        " data:" << data <<
        "";
    }
#ifdef ENABLE_ALTIMETER
    details::stmt_explain(rctx, tx_id, tx_type, job_id, data);
#endif
}

}  // namespace jogasaki::external_log
