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
#pragma once

#include <string_view>

namespace jogasaki::external_log {
// log category list
namespace log_category {
static constexpr std::string_view event = "event";
static constexpr std::string_view audit = "audit";
} // namespace log_category

// event log type list
namespace log_type::event {
static constexpr std::string_view session_start = "session_start";
static constexpr std::string_view session_end = "session_end";
static constexpr std::string_view tx_start = "tx_start";
static constexpr std::string_view tx_end = "tx_end";
static constexpr std::string_view stmt_start = "stmt_start";
static constexpr std::string_view stmt_end = "stmt_end";
static constexpr std::string_view stmt_explain = "stmt_explain";
} // namespace log_type::event

// event log item list
namespace log_item::event {
static constexpr std::string_view user = "user";
static constexpr std::string_view dbname = "dbname";
static constexpr std::string_view pid = "pid";
static constexpr std::string_view remote_host = "remote_host";
static constexpr std::string_view application_name = "application_name";
static constexpr std::string_view session_id = "session_id";
static constexpr std::string_view session_label = "session_label";
static constexpr std::string_view tx_id = "tx_id";
static constexpr std::string_view tx_type = "tx_type";
static constexpr std::string_view job_id = "job_id";
static constexpr std::string_view result = "result";
static constexpr std::string_view state_code = "state_code";
static constexpr std::string_view message = "message";
static constexpr std::string_view statement = "statement";
static constexpr std::string_view parameter = "parameter";
static constexpr std::string_view fetched = "fetched";
static constexpr std::string_view returned = "returned";
static constexpr std::string_view inserted = "inserted";
static constexpr std::string_view updated = "updated";
static constexpr std::string_view deleted = "deleted";
static constexpr std::string_view merged = "merged";
static constexpr std::string_view data = "data";
}

// event log level list
namespace log_level::event {
static constexpr int error = 30;
static constexpr int info = 50;
} // namespace log_level::event

// values for tx_type
namespace tx_type_value {
static constexpr std::int64_t occ = 1;
static constexpr std::int64_t ltx = 2;
static constexpr std::int64_t rtx = 3;
}  // namespace tx_type_value

}  // namespace jogasaki::external_log
