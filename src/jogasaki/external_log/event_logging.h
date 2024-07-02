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
#pragma once

#include <cstdint>
#include <string_view>

#include <jogasaki/request_info.h>

namespace jogasaki::external_log {

using clock = std::chrono::steady_clock;

void tx_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view tx_label
);

void tx_end(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::int64_t result,
    std::int64_t duration_time_ns,
    std::string_view tx_label
);

void stmt_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view statement,
    std::string_view parameter,
    std::string_view tx_label
);

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
);

void stmt_explain(
    request_info const& req_info,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view data,
    std::string_view tx_label
);

}  // namespace jogasaki::external_log
