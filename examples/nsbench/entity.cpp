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
#include "entity.h"

#include <cstddef>

namespace tateyama::nsbench {

namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;

namespace {

// sbench fills c/pad with random digits and '-' at every 12th character, leaving the last character of the column unused
constexpr std::size_t c_value_size = 119;
constexpr std::size_t pad_value_size = 59;

std::string random_string(std::size_t size, random_engine& rnd) {
    std::string ret(size, '0');
    for(std::size_t i = 1; i <= size; ++i) {
        ret[i-1] = (i % 12 == 0) ? '-' : static_cast<char>('0' + rnd() % 10);
    }
    return ret;
}

}  // namespace

std::int64_t random_between(std::int64_t origin, std::int64_t bound, random_engine& rnd) {
    if (bound <= origin) {
        return origin;
    }
    return origin + static_cast<std::int64_t>(rnd() % static_cast<std::uint64_t>(bound - origin));
}

std::string_view entity::prefix() const noexcept {
    return kind_ == kind::lkv ? "lkv" : "sbtest";
}

std::string_view entity::pk_column() const noexcept {
    return kind_ == kind::lkv ? "k" : "id";
}

std::string entity::table_name(std::int64_t table_id) const {
    return std::string{prefix()} + std::to_string(table_id);
}

std::string entity::drop_table_sql(std::int64_t table_id) const {
    return "drop table if exists " + table_name(table_id);
}

std::string entity::create_table_sql(std::int64_t table_id, bool primary_key) const {
    std::string pk = primary_key ? "PRIMARY KEY" : "";
    if (kind_ == kind::lkv) {
        return "create table " + table_name(table_id) + "(k bigint " + pk + ",v bigint not null)";
    }
    return "create table " + table_name(table_id) + "(id int " + pk + ", k int not null, c char(120) not null, pad char(60) not null)";
}

std::string entity::create_index_sql(std::int64_t table_id, std::string_view column) const {
    auto t = table_name(table_id);
    return "create index " + t + "_idx_" + std::string{column} + " on " + t + "(" + std::string{column} + ")";
}

std::string entity::insert_sql(std::int64_t table_id, bool upsert) const {
    std::string head = upsert ? "insert or replace into " : "insert into ";
    if (kind_ == kind::lkv) {
        return head + table_name(table_id) + "(k, v) values(:k, :v)";
    }
    return head + table_name(table_id) + "(id, k, c, pad) values(:id, :k, :c, :pad)";
}

std::string entity::update_sql(std::int64_t table_id) const {
    if (kind_ == kind::lkv) {
        return "update " + table_name(table_id) + " set v = :v where k = :k";
    }
    return "update " + table_name(table_id) + " set k = :k, c = :c, pad = :pad where id = :id";
}

std::string entity::delete_all_sql(std::int64_t table_id) const {
    return "delete from " + table_name(table_id);
}

std::string entity::delete_point_sql(std::int64_t table_id) const {
    return delete_all_sql(table_id) + " where " + std::string{pk_column()} + " = :value";
}

std::string entity::delete_scan_sql(std::int64_t table_id) const {
    auto pk = std::string{pk_column()};
    return delete_all_sql(table_id) + " where " + pk + " >= :from and " + pk + " < :to";
}

std::string entity::select_full_scan_sql(std::int64_t table_id, std::string_view columns) const {
    return "select " + std::string{columns} + " from " + table_name(table_id);
}

std::string entity::select_point_sql(
    std::int64_t table_id,
    std::string_view columns,
    std::string_view condition_column
) const {
    auto c = std::string{condition_column.empty() ? pk_column() : condition_column};
    return select_full_scan_sql(table_id, columns) + " where " + c + " = :value";
}

std::string entity::select_range_sql(
    std::int64_t table_id,
    std::string_view columns,
    std::string_view condition_column
) const {
    auto c = std::string{condition_column.empty() ? pk_column() : condition_column};
    return select_full_scan_sql(table_id, columns) + " where " + c + " >= :from and " + c + " < :to";
}

placeholders entity::row_placeholders() const {
    if (kind_ == kind::lkv) {
        return {
            {"k", sql::common::AtomType::INT8},
            {"v", sql::common::AtomType::INT8},
        };
    }
    return {
        {"id", sql::common::AtomType::INT4},
        {"k", sql::common::AtomType::INT4},
        {"c", sql::common::AtomType::CHARACTER},
        {"pad", sql::common::AtomType::CHARACTER},
    };
}

placeholders entity::point_placeholders() const {
    auto t = kind_ == kind::lkv ? sql::common::AtomType::INT8 : sql::common::AtomType::INT4;
    return {{"value", t}};
}

placeholders entity::range_placeholders() const {
    auto t = kind_ == kind::lkv ? sql::common::AtomType::INT8 : sql::common::AtomType::INT4;
    return {{"from", t}, {"to", t}};
}

jogasaki::utils::parameter entity::key_parameter(std::string name, std::int64_t value) const {
    if (kind_ == kind::lkv) {
        return {std::move(name), ValueCase::kInt8Value, value};
    }
    return {std::move(name), ValueCase::kInt4Value, static_cast<std::int32_t>(value)};
}

parameters entity::row(std::int64_t id, std::int64_t origin, std::int64_t bound, random_engine& rnd) const {
    if (kind_ == kind::lkv) {
        return {
            {"k", ValueCase::kInt8Value, id},
            {"v", ValueCase::kInt8Value, random_between(origin, bound, rnd)},
        };
    }
    return {
        {"id", ValueCase::kInt4Value, static_cast<std::int32_t>(id)},
        {"k", ValueCase::kInt4Value, static_cast<std::int32_t>(random_between(origin, bound, rnd))},
        {"c", ValueCase::kCharacterValue, random_string(c_value_size, rnd)},
        {"pad", ValueCase::kCharacterValue, random_string(pad_value_size, rnd)},
    };
}

parameters entity::point(std::int64_t value) const {
    return {key_parameter("value", value)};
}

parameters entity::range(std::int64_t from, std::int64_t to) const {
    return {key_parameter("from", from), key_parameter("to", to)};
}

}  // namespace tateyama::nsbench
