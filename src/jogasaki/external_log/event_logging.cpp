/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <ostream>
#include <type_traits>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/transaction_context.h>

#ifdef ENABLE_ALTIMETER
#include "details/altimeter_event_logging.h"
#endif

namespace jogasaki::external_log {

static_assert(std::is_same_v<clock, request_statistics::clock>);
static_assert(std::is_same_v<clock, transaction_context::clock>);

void tx_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view tx_label
) {
    auto& cfg = global::config_pool();
    if(cfg && cfg->trace_external_log()) {
        VLOG_LP(log_trace) <<
            "message:\"" << message << "\"" <<
            " tx_id:" << tx_id <<
            " tx_type:" << tx_type <<
            " tx_label:" << tx_label <<
            " instance_id:" << (req_info.request_source() ? req_info.request_source()->database_info().instance_id() : "null") <<
            "";
    }
    (void) req_info;
#ifdef ENABLE_ALTIMETER
    details::tx_start(req_info, message, tx_id, tx_type, tx_label);
#endif
}

void tx_end(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::int64_t result,
    std::int64_t duration_time_ns,
    std::string_view tx_label
) {
    auto& cfg = global::config_pool();
    if(cfg && cfg->trace_external_log()) {
        VLOG_LP(log_trace) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " tx_label:" << tx_label <<
        " result:" << result <<
        " duration_time:" << duration_time_ns <<
        " instance_id:" << (req_info.request_source() ? req_info.request_source()->database_info().instance_id() : "null") <<
        "";
    }
    (void) req_info;
#ifdef ENABLE_ALTIMETER
    details::tx_end(req_info, message, tx_id, tx_type, result, duration_time_ns, tx_label);
#endif
}

void stmt_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view statement,
    std::string_view parameter,
    std::string_view tx_label
) {
    auto& cfg = global::config_pool();
    if(cfg && cfg->trace_external_log()) {
        VLOG_LP(log_trace) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " tx_label:" << tx_label <<
        " job_id:" << job_id <<
        " statement:\"" << statement << "\"" <<
        " parameter:\"" << parameter << "\"" <<
        " instance_id:" << (req_info.request_source() ? req_info.request_source()->database_info().instance_id() : "null") <<
        "";
    }
    (void) req_info;
#ifdef ENABLE_ALTIMETER
    details::stmt_start(req_info, message, tx_id, tx_type, job_id, statement, parameter, tx_label);
#endif
}

void stmt_end(
    request_info const& req_info,
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
    std::int64_t merged,
    std::int64_t duration_time_ns,
    std::string_view tx_label
) {
    auto& cfg = global::config_pool();
    if(cfg && cfg->trace_external_log()) {
        VLOG_LP(log_trace) <<
        "message:\"" << message << "\"" <<
        " tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " tx_label:" << tx_label <<
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
        " duration_time:" << duration_time_ns <<
        " instance_id:" << (req_info.request_source() ? req_info.request_source()->database_info().instance_id() : "null") <<
        "";
    }
    (void) req_info;
#ifdef ENABLE_ALTIMETER
    details::stmt_end(
        req_info,
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
        merged,
        duration_time_ns,
        tx_label
    );
#endif
}

void stmt_explain(
    request_info const& req_info,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view data,
    std::string_view tx_label
) {
    auto& cfg = global::config_pool();
    if(cfg && cfg->trace_external_log()) {
        VLOG_LP(log_trace) <<
        "tx_id:" << tx_id <<
        " tx_type:" << tx_type <<
        " job_id:" << job_id <<
        " tx_label:" << tx_label <<
        " data:" << data <<
        " instance_id:" << (req_info.request_source() ? req_info.request_source()->database_info().instance_id() : "null") <<
        "";
    }
    (void) req_info;
#ifdef ENABLE_ALTIMETER
    details::stmt_explain(req_info, tx_id, tx_type, job_id, data, tx_label);
#endif
}

}  // namespace jogasaki::external_log
