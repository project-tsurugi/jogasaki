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
#include "altimeter_event_logging.h"

#ifdef ENABLE_ALTIMETER
#include <altimeter/event/constants.h>
#include <altimeter/event/event_logger.h>
#include <altimeter/log_item.h>
#include <altimeter/logger.h>
#endif

#include <memory>
#include <sys/types.h>

#include <tateyama/api/server/database_info.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/session_info.h>

#include <jogasaki/request_info.h>

namespace jogasaki::external_log::details {

void fill_common_properties(
    request_info const& req_info,
    ::altimeter::log_item& item
) {
    item.category(::altimeter::event::category);
    auto& req = req_info.request_source();
    if(! req) {
        return;
    }
    auto const& database_info = req->database_info();
    auto const& session_info = req->session_info();
    if(auto database_name = database_info.name(); ! database_name.empty()) {
        item.add(::altimeter::event::item::dbname, database_name);
    }
    item.add(::altimeter::event::item::pid, static_cast<pid_t>(database_info.process_id()));
    if(auto connection_information = session_info.connection_information(); ! connection_information.empty()) {
        item.add(::altimeter::event::item::remote_host, connection_information);
    }
    if(auto application_name = session_info.application_name(); ! application_name.empty()) {
        item.add(::altimeter::event::item::application_name, application_name);
    }
    if(auto session_label = session_info.label(); ! session_label.empty()) {
        item.add(::altimeter::event::item::session_label, session_label);
    }
    item.add(::altimeter::event::item::session_id, static_cast<std::int64_t>(session_info.id()));
}

void tx_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view tx_label
) {
    if(! ::altimeter::logger::is_log_on(::altimeter::event::category, ::altimeter::event::level::transaction)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(::altimeter::event::type::tx_start);
    item.level(::altimeter::event::level::transaction);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(::altimeter::event::item::message, message);
    }
    item.add(::altimeter::event::item::tx_id, tx_id);
    item.add(::altimeter::event::item::tx_type, tx_type);
    item.add(::altimeter::event::item::tx_label, tx_label);
    ::altimeter::logger::log(item);
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
    if(! ::altimeter::logger::is_log_on(::altimeter::event::category, ::altimeter::event::level::transaction)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(::altimeter::event::type::tx_end);
    item.level(::altimeter::event::level::transaction);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(::altimeter::event::item::message, message);
    }
    item.add(::altimeter::event::item::tx_id, tx_id);
    item.add(::altimeter::event::item::tx_type, tx_type);
    item.add(::altimeter::event::item::result, result);
    item.add(::altimeter::event::item::duration_time, duration_time_ns);
    item.add(::altimeter::event::item::tx_label, tx_label);
    ::altimeter::logger::log(item);
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
    if(! ::altimeter::logger::is_log_on(::altimeter::event::category, ::altimeter::event::level::statement)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(::altimeter::event::type::stmt_start);
    item.level(::altimeter::event::level::statement);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(::altimeter::event::item::message, message);
    }
    item.add(::altimeter::event::item::tx_id, tx_id);
    item.add(::altimeter::event::item::tx_type, tx_type);
    item.add(::altimeter::event::item::job_id, job_id);
    item.add(::altimeter::event::item::statement, statement);
    item.add(::altimeter::event::item::parameter, parameter);
    item.add(::altimeter::event::item::tx_label, tx_label);
    ::altimeter::logger::log(item);
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
    if(! ::altimeter::logger::is_log_on(::altimeter::event::category, ::altimeter::event::level::statement) &&
       ! ::altimeter::event::event_logger::is_over_stmt_duration_threshold(duration_time_ns)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(::altimeter::event::type::stmt_end);
    item.level(
        ::altimeter::event::event_logger::is_over_stmt_duration_threshold(duration_time_ns)
            ? altimeter::event::level::min
            : altimeter::event::level::statement
    );

    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(::altimeter::event::item::message, message);
    }
    item.add(::altimeter::event::item::tx_id, tx_id);
    item.add(::altimeter::event::item::tx_type, tx_type);
    item.add(::altimeter::event::item::job_id, job_id);
    item.add(::altimeter::event::item::statement, statement);
    item.add(::altimeter::event::item::parameter, parameter);
    item.add(::altimeter::event::item::result, result);
    item.add(::altimeter::event::item::state_code, state_code);
    item.add(::altimeter::event::item::fetched, fetched);
    item.add(::altimeter::event::item::inserted, inserted);
    item.add(::altimeter::event::item::updated, updated);
    item.add(::altimeter::event::item::deleted, deleted);
    item.add(::altimeter::event::item::merged, merged);
    item.add(::altimeter::event::item::duration_time, duration_time_ns);
    item.add(::altimeter::event::item::tx_label, tx_label);
    ::altimeter::logger::log(item);
}

void stmt_explain(
    request_info const& req_info,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view data,
    std::string_view tx_label
) {
    if(! ::altimeter::logger::is_log_on(::altimeter::event::category, ::altimeter::event::level::min)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(::altimeter::event::type::stmt_explain);
    item.level(::altimeter::event::level::min);
    fill_common_properties(req_info, item);
    item.add(::altimeter::event::item::tx_id, tx_id);
    item.add(::altimeter::event::item::tx_type, tx_type);
    item.add(::altimeter::event::item::job_id, job_id);
    item.add(::altimeter::event::item::data, data);
    item.add(::altimeter::event::item::tx_label, tx_label);
    ::altimeter::logger::log(item);
}

}  // namespace jogasaki::external_log::details
