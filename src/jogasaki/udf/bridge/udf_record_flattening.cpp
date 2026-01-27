/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "udf_record_flattening.h"

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/udf/bridge/udf_semantic_mappings.h>
#include <jogasaki/udf/bridge/udf_special_records.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
#include <jogasaki/udf/plugin_api.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::udf::bridge {
bool is_special_nested_record(std::string_view rn) {
    return rn == DECIMAL_RECORD || rn == DATE_RECORD || rn == LOCALTIME_RECORD ||
           rn == LOCALDATETIME_RECORD || rn == OFFSETDATETIME_RECORD || rn == BLOB_RECORD ||
           rn == CLOB_RECORD;
}
namespace {
void append_column_names(
    std::vector<jogasaki::executor::function::table_valued_function_column>& out,
    std::vector<plugin::udf::column_descriptor*> const& cols, std::string const& prefix = {}) {
    for (auto* col : cols) {
        if (!col) continue;
        auto name = std::string{col->column_name()};

        std::string full;
        if (prefix.empty()) {
            full = std::move(name);
        } else {
            full.reserve(prefix.size() + 1 + name.size());
            full.append(prefix);
            full.push_back('_');
            full.append(name);
        }

        if (auto* nested = col->nested()) {
            auto rn = nested->record_name();

            if (is_special_nested_record(rn)) {
                out.emplace_back(full);
            } else {
                append_column_names(out, nested->columns(), full);
            }
            continue;
        }

        out.emplace_back(full);
    }
}
jogasaki::udf::data::udf_wire_kind to_wire_kind_from_nested_record(std::string_view rn) {
    using k = jogasaki::udf::data::udf_wire_kind;

    if (rn == DECIMAL_RECORD) return k::decimal;
    if (rn == DATE_RECORD) return k::date;
    if (rn == LOCALTIME_RECORD) return k::time_of_day;
    if (rn == LOCALDATETIME_RECORD) return k::time_point;
    if (rn == OFFSETDATETIME_RECORD) return k::time_point_with_time_zone;
    if (rn == BLOB_RECORD) return k::blob;
    if (rn == CLOB_RECORD) return k::clob;

    fail_with_exception_msg("unknown special nested record_name in UDF schema");
}
jogasaki::udf::data::udf_wire_kind to_wire_kind_from_column(
    plugin::udf::column_descriptor const& col) {
    using wk = jogasaki::udf::data::udf_wire_kind;
    using tk = plugin::udf::type_kind;

    switch (col.type_kind()) {
        case tk::boolean: return wk::boolean;
        case tk::int4:
        case tk::sfixed4:
        case tk::sint4:
        case tk::uint4:
        case tk::fixed4: return wk::int4;

        case tk::int8:
        case tk::sfixed8:
        case tk::sint8:
        case tk::uint8:
        case tk::fixed8: return wk::int8;

        case tk::float4: return wk::float4;
        case tk::float8: return wk::float8;
        case tk::string: return wk::character;
        case tk::bytes: return wk::octet;
        default: fail_with_exception_msg("unsupported column type_kind for udf_wire_kind mapping");
    }
}
void append_wire_kinds(std::vector<jogasaki::udf::data::udf_wire_kind>& out,
    std::vector<plugin::udf::column_descriptor*> const& cols) {
    for (auto* col : cols) {
        if (!col) continue;

        if (auto* nested = col->nested()) {
            auto rn = nested->record_name();
            if (is_special_nested_record(rn)) {
                out.emplace_back(to_wire_kind_from_nested_record(rn));
            } else {
                append_wire_kinds(out, nested->columns());
            }
            continue;
        }
        out.emplace_back(to_wire_kind_from_column(*col));
    }
}
} // namespace anonymous
jogasaki::executor::function::table_valued_function_info::columns_type build_tvf_columns(
    plugin::udf::function_descriptor const& fn) {
    jogasaki::executor::function::table_valued_function_info::columns_type cols;
    cols.reserve(jogasaki::udf::bridge::count_effective_columns(fn.output_record()));
    append_column_names(cols, fn.output_record().columns());
    return cols;
}
std::size_t count_effective_columns(plugin::udf::record_descriptor const& rec) {
    std::size_t total = 0;

    for (auto* col : rec.columns()) {
        if (!col) continue;

        auto* nested = col->nested();
        if (nested) {
            total += count_effective_columns(*nested);
        } else {
            total += 1;
        }
    }
    return total;
}
std::vector<jogasaki::udf::data::udf_wire_kind>
build_output_wire_kinds(plugin::udf::function_descriptor const& fn) {
    std::vector<jogasaki::udf::data::udf_wire_kind> out;
    out.reserve(count_effective_columns(fn.output_record()));
    append_wire_kinds(out, fn.output_record().columns());
    return out;
}
} // namespace jogasaki::udf::bridge