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
#include <altimeter/logger.h>
#include <altimeter/configuration.h>
#include <altimeter/log_item.h>
#endif

#include <jogasaki/request_info.h>
#include <jogasaki/external_log/events.h>

namespace jogasaki::external_log::details {

void fill_common_properties(
    request_info const& req_info,
    ::altimeter::log_item& item
) {
    item.category(log_category::event);
    item.level(log_level::event::info);
    auto req = req_info.request_source();
    if(! req) {
        return;
    }
    auto const& database_info = req->database_info();
    auto const& session_info = req->session_info();
    if(auto database_name = database_info.name(); ! database_name.empty()) {
        item.add(log_item::event::dbname, database_name);
    }
    item.add(log_item::event::pid, static_cast<pid_t>(database_info.process_id()));
    if(auto connection_information = session_info.connection_information(); ! connection_information.empty()) {
        item.add(log_item::event::remote_host, connection_information);
    }
    if(auto application_name = session_info.application_name(); ! application_name.empty()) {
        item.add(log_item::event::application_name, application_name);
    }
    if(auto session_label = session_info.label(); ! session_label.empty()) {
        item.add(log_item::event::session_label, session_label);
    }
    item.add(log_item::event::session_id, static_cast<std::int64_t>(session_info.id()));
}

void tx_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type
) {
    if(! ::altimeter::logger::is_log_on(log_category::event, log_level::event::info)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(log_type::event::tx_start);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(log_item::event::message, message);
    }
    item.add(log_item::event::tx_id, tx_id);
    item.add(log_item::event::tx_type, tx_type);
    ::altimeter::logger::log(item);
}

void tx_end(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::int64_t result
) {
    if(! ::altimeter::logger::is_log_on(log_category::event, log_level::event::info)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(log_type::event::tx_end);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(log_item::event::message, message);
    }
    item.add(log_item::event::tx_id, tx_id);
    item.add(log_item::event::tx_type, tx_type);
    item.add(log_item::event::result, result);
    ::altimeter::logger::log(item);
}

void stmt_start(
    request_info const& req_info,
    std::string_view message,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view statement,
    std::string_view parameter
) {
    if(! ::altimeter::logger::is_log_on(log_category::event, log_level::event::info)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(log_type::event::stmt_start);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(log_item::event::message, message);
    }
    item.add(log_item::event::tx_id, tx_id);
    item.add(log_item::event::tx_type, tx_type);
    item.add(log_item::event::job_id, job_id);
    item.add(log_item::event::statement, statement);
    item.add(log_item::event::parameter, parameter);
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
    std::int64_t merged
) {
    if(! ::altimeter::logger::is_log_on(log_category::event, log_level::event::info)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(log_type::event::stmt_end);
    fill_common_properties(req_info, item);
    if(! message.empty()) {
        item.add(log_item::event::message, message);
    }
    item.add(log_item::event::tx_id, tx_id);
    item.add(log_item::event::tx_type, tx_type);
    item.add(log_item::event::job_id, job_id);
    item.add(log_item::event::statement, statement);
    item.add(log_item::event::parameter, parameter);
    item.add(log_item::event::result, result);
    item.add(log_item::event::state_code, state_code);
    item.add(log_item::event::fetched, fetched);
    item.add(log_item::event::inserted, inserted);
    item.add(log_item::event::updated, updated);
    item.add(log_item::event::deleted, deleted);
    item.add(log_item::event::merged, merged);
    ::altimeter::logger::log(item);
}

void stmt_explain(
    request_info const& req_info,
    std::string_view tx_id,
    std::int64_t tx_type,
    std::string_view job_id,
    std::string_view data
) {
    if(! ::altimeter::logger::is_log_on(log_category::event, log_level::event::info)) {
        return;
    }
    if(! req_info.request_source()) {
        return;
    }
    ::altimeter::log_item item{};
    item.type(log_type::event::stmt_explain);
    fill_common_properties(req_info, item);
    item.add(log_item::event::tx_id, tx_id);
    item.add(log_item::event::tx_type, tx_type);
    item.add(log_item::event::job_id, job_id);
    item.add(log_item::event::data, data);
    ::altimeter::logger::log(item);
}

}  // namespace jogasaki::external_log::details
