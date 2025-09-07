/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "describe.h"

#include <takatori/type/character.h>
#include <takatori/type/octet.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/utils/string_manipulation.h>
#include <jogasaki/request_logging.h>

namespace jogasaki::executor {

using takatori::util::string_builder;

static void set_column_type(
    takatori::type::data const& type,
    dto::common_column& c
) {
    using k = takatori::type::type_kind;
    using atom_type = dto::common_column::atom_type;
    atom_type typ = atom_type::unknown;
    switch(type.kind()) {
        case k::boolean: typ = atom_type::boolean; break;
        case k::int4: typ = atom_type::int4; break;
        case k::int8: typ = atom_type::int8; break;
        case k::float4: typ = atom_type::float4; break;
        case k::float8: typ = atom_type::float8; break;
        case k::decimal: {
            typ = atom_type::decimal;
            auto prec = static_cast<takatori::type::decimal const&>(type).precision(); //NOLINT
            auto scale = static_cast<takatori::type::decimal const&>(type).scale(); //NOLINT
            if (prec.has_value()) {
                c.precision_ = static_cast<std::uint32_t>(prec.value());
            } else {
                c.precision_ = true;
            }
            if (scale.has_value()) {
                c.scale_ = static_cast<std::uint32_t>(scale.value());
            } else {
                c.scale_ = true;
            }
            break;
        }
        case k::character: {
            typ = atom_type::character;
            auto len = static_cast<takatori::type::character const&>(type).length(); //NOLINT
            auto varying = static_cast<takatori::type::character const&>(type).varying(); //NOLINT
            if (len.has_value()) {
                c.length_ = static_cast<std::uint32_t>(len.value());
            } else {
                c.length_ = true;
            }
            c.varying_ = varying;
            break;
        }
        case k::octet: {
            typ = atom_type::octet;
            auto len = static_cast<takatori::type::octet const&>(type).length(); //NOLINT
            auto varying = static_cast<takatori::type::octet const&>(type).varying(); //NOLINT
            if (len.has_value()) {
                c.length_ = static_cast<std::uint32_t>(len.value());
            } else {
                c.length_ = true;
            }
            c.varying_ = varying;
            break;
        }
        case k::bit: typ = atom_type::bit; break;
        case k::date: typ = atom_type::date; break;
        case k::time_of_day: {
            if(static_cast<takatori::type::time_of_day const&>(type).with_time_zone()) { //NOLINT
                typ = atom_type::time_of_day_with_time_zone;
                break;
            }
            typ = atom_type::time_of_day;
            break;
        }
        case k::time_point: {
            if(static_cast<takatori::type::time_point const&>(type).with_time_zone()) { //NOLINT
                typ = atom_type::time_point_with_time_zone;
                break;
            }
            typ = atom_type::time_point;
            break;
        }
        case k::blob: typ = atom_type::blob; break;
        case k::clob: typ = atom_type::clob; break;
        case k::datetime_interval: typ = atom_type::datetime_interval; break;
        default:
            break;
    }
    c.atom_type_ = typ;
}

static void fill_from_provider(
    yugawara::storage::table const* tbl,
    std::shared_ptr<yugawara::storage::configurable_provider> provider,  //NOLINT(performance-unnecessary-value-param)
    dto::describe_table& out
) {
    BOOST_ASSERT(tbl != nullptr); //NOLINT
    out.table_name_ = tbl->simple_name();
    out.schema_name_ = "";  //TODO schema resolution
    out.database_name_ = "";   //TODO database name resolution
    if (! tbl->description().empty()) {
        out.description_ = tbl->description();
    }
    out.columns_.reserve(tbl->columns().size());
    for(auto&& col : tbl->columns()) {
        if(utils::is_prefix(col.simple_name(), generated_pkey_column_prefix)) {
            continue;
        }
        auto&& c = out.columns_.emplace_back();
        c.name_ = col.simple_name();
        set_column_type(col.type(), c);
        c.nullable_ = col.criteria().nullity().nullable();
        if (! col.description().empty()) {
            c.description_ = col.description();
        }
    }
    auto pi = provider->find_primary_index(*tbl);
    if(pi) {
        out.primary_key_.reserve(pi->keys().size());
        for(auto&& k : pi->keys()) {
            if(! utils::is_prefix(k.column().simple_name(), generated_pkey_column_prefix)) {
                out.primary_key_.emplace_back(k.column().simple_name());
            }
        }
    }
}

status describe(
    std::string_view table_name,
    dto::describe_table& out,
    std::shared_ptr<error::error_info>& error,
    request_info const& req_info
) {
    (void) req_info;
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::describe_table);
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);

    auto table = global::database_impl()->find_table(table_name);
    if(! table || utils::is_prefix(table_name, system_identifier_prefix)) {
        VLOG_LP(log_error) << "table not found : " << table_name;
        auto st = status::err_not_found;
        error = create_error_info(
            error_code::target_not_found_exception,
            string_builder{} << "Target table \"" << table_name << "\" is not found." << string_builder::to_string,
            st
        );
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        return st;
    }
    fill_from_provider(table.get(), global::database_impl()->tables(), out);
    req->status(scheduler::request_detail_status::finishing);
    log_request(*req);
    return status::ok;
}

}