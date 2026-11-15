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
#include "result_formatter.h"

#include <cstdio>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <variant>

#include <decimal.hh>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace tateyama::service_cli {

using jogasaki::executor::dto::common_column;
using kind = jogasaki::meta::field_type_kind;
using atom_type = common_column::atom_type;

namespace {

void append_length(std::ostream& out, std::optional<std::variant<std::uint32_t, bool>> const& v) {
    if (! v.has_value()) {
        return;
    }
    if (std::holds_alternative<bool>(*v)) {
        out << "(*)";
        return;
    }
    out << "(" << std::get<std::uint32_t>(*v) << ")";
}

void append_value(std::ostream& out, std::optional<std::variant<std::uint32_t, bool>> const& v) {
    if (std::holds_alternative<bool>(*v)) {
        out << "*";
        return;
    }
    out << std::get<std::uint32_t>(*v);
}

std::string format_date(takatori::datetime::date d) {
    std::ostringstream ss{};
    ss << std::setfill('0') << std::setw(4) << d.year() << '-' << std::setw(2) << d.month() << '-' << std::setw(2) << d.day();
    return ss.str();
}

std::string format_time(takatori::datetime::time_of_day t) {
    std::ostringstream ss{};
    ss << std::setfill('0') << std::setw(2) << t.hour() << ':' << std::setw(2) << t.minute() << ':' << std::setw(2) << t.second()
       << '.' << std::setw(9) << t.subsecond().count();
    return ss.str();
}

}  // namespace

std::string format_type(common_column const& column) {
    std::ostringstream ss{};
    switch(column.atom_type_) {
        case atom_type::boolean: ss << "BOOLEAN"; break;
        case atom_type::int4: ss << "INT"; break;
        case atom_type::int8: ss << "BIGINT"; break;
        case atom_type::float4: ss << "REAL"; break;
        case atom_type::float8: ss << "DOUBLE"; break;
        case atom_type::decimal:
            ss << "DECIMAL";
            if (column.precision_.has_value()) {
                ss << "(";
                append_value(ss, column.precision_);
                if (column.scale_.has_value()) {
                    ss << ", ";
                    append_value(ss, column.scale_);
                }
                ss << ")";
            }
            break;
        case atom_type::character:
            ss << (column.varying_.value_or(false) ? "VARCHAR" : "CHAR");
            append_length(ss, column.length_);
            break;
        case atom_type::octet:
            ss << (column.varying_.value_or(false) ? "VARBINARY" : "BINARY");
            append_length(ss, column.length_);
            break;
        case atom_type::date: ss << "DATE"; break;
        case atom_type::time_of_day: ss << "TIME"; break;
        case atom_type::time_of_day_with_time_zone: ss << "TIME WITH TIME ZONE"; break;
        case atom_type::time_point: ss << "TIMESTAMP"; break;
        case atom_type::time_point_with_time_zone: ss << "TIMESTAMP WITH TIME ZONE"; break;
        case atom_type::blob: ss << "BLOB"; break;
        case atom_type::clob: ss << "CLOB"; break;
        default: ss << common_column::to_string_view(column.atom_type_); break;
    }
    return ss.str();
}

std::string format_header(std::vector<common_column> const& columns) {
    std::ostringstream ss{};
    ss << "[";
    for(std::size_t i = 0; i < columns.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        auto& c = columns[i];
        if (c.name_.empty()) {
            ss << "@#" << i;
        } else {
            ss << c.name_;
        }
        ss << ": " << format_type(c);
    }
    ss << "]";
    return ss.str();
}

std::string format_record(jogasaki::mock::basic_record const& record) {
    std::ostringstream ss{};
    auto& meta = *record.record_meta();
    ss << "[";
    for(std::size_t i = 0, n = meta.field_count(); i < n; ++i) {
        if (i != 0) {
            ss << ", ";
        }
        if (record.is_null(i)) {
            ss << "null";
            continue;
        }
        auto& type = meta.at(i);
        switch(type.kind()) {
            case kind::boolean: ss << (record.get_value<std::int8_t>(i) != 0 ? "true" : "false"); break;
            case kind::int4: ss << record.get_value<std::int32_t>(i); break;
            case kind::int8: ss << record.get_value<std::int64_t>(i); break;
            case kind::float4: ss << record.get_value<float>(i); break;
            case kind::float8: ss << record.get_value<double>(i); break;
            case kind::decimal: {
                decimal::Decimal d{record.get_value<takatori::decimal::triple>(i)};
                ss << d.format("f");
                break;
            }
            case kind::character: {
                auto t = record.get_value<jogasaki::accessor::text>(i);
                ss << static_cast<std::string_view>(t);
                break;
            }
            case kind::octet: {
                auto b = record.get_value<jogasaki::accessor::binary>(i);
                for(auto c : static_cast<std::string_view>(b)) {
                    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(c)) << std::dec;
                }
                break;
            }
            case kind::date: ss << format_date(record.get_value<takatori::datetime::date>(i)); break;
            case kind::time_of_day: {
                ss << format_time(record.get_value<takatori::datetime::time_of_day>(i));
                if (type.option_unsafe<kind::time_of_day>()->with_offset_) {
                    ss << "Z";
                }
                break;
            }
            case kind::time_point: {
                auto tp = record.get_value<takatori::datetime::time_point>(i);
                ss << format_date(tp.date()) << ' ' << format_time(tp.time());
                if (type.option_unsafe<kind::time_point>()->with_offset_) {
                    ss << "Z";
                }
                break;
            }
            case kind::blob: ss << "<blob>"; break;
            case kind::clob: ss << "<clob>"; break;
            default: ss << "<" << type.kind() << ">"; break;
        }
    }
    ss << "]";
    return ss.str();
}

std::string format_row_count(std::size_t count) {
    std::ostringstream ss{};
    ss << "(" << count << (count == 1 ? " row)" : " rows)");
    return ss.str();
}

std::string format_statement_result(std::shared_ptr<jogasaki::request_statistics> const& stats) {
    if (! stats) {
        return "execute succeeded";
    }
    std::ostringstream ss{};
    bool first = true;
    stats->each_counter([&](jogasaki::counter_kind k, jogasaki::request_execution_counter const& c) {
        if (! c.has_value()) {
            return;
        }
        auto count = c.count().value_or(0);
        ss << (first ? "(" : ", ") << count << (count == 1 ? " row " : " rows ");
        switch(k) {
            case jogasaki::counter_kind::inserted: ss << "inserted"; break;
            case jogasaki::counter_kind::updated: ss << "updated"; break;
            case jogasaki::counter_kind::merged: ss << "merged"; break;
            case jogasaki::counter_kind::deleted: ss << "deleted"; break;
            default: ss << k; break;
        }
        first = false;
    });
    if (first) {
        return "execute succeeded";
    }
    ss << ")";
    return ss.str();
}

}  // namespace tateyama::service_cli
