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
#pragma once

#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/utils/command_utils.h>

namespace tateyama::nsbench {

using placeholders = std::unordered_map<std::string, jogasaki::proto::sql::common::AtomType>;
using parameters = std::vector<jogasaki::utils::parameter>;
using random_engine = std::mt19937_64;

/**
 * @brief random value in [origin, bound), or origin if the range is empty
 */
std::int64_t random_between(std::int64_t origin, std::int64_t bound, random_engine& rnd);

/**
 * @brief table definition and statements, same as simple-benchmark (sbench) entity factories
 * @details lkv: `lkv<N>(k bigint, v bigint)`, sysbench: `sbtest<N>(id int, k int, c char(120), pad char(60))`
 */
class entity {
public:
    enum class kind {
        lkv,
        sysbench,
    };

    entity() = default;

    explicit entity(kind k) noexcept :
        kind_(k)
    {}

    [[nodiscard]] kind get_kind() const noexcept {
        return kind_;
    }

    [[nodiscard]] std::string table_name(std::int64_t table_id) const;

    [[nodiscard]] std::string drop_table_sql(std::int64_t table_id) const;

    [[nodiscard]] std::string create_table_sql(std::int64_t table_id, bool primary_key) const;

    [[nodiscard]] std::string create_index_sql(std::int64_t table_id, std::string_view column) const;

    [[nodiscard]] std::string insert_sql(std::int64_t table_id, bool upsert) const;

    [[nodiscard]] std::string update_sql(std::int64_t table_id) const;

    [[nodiscard]] std::string delete_all_sql(std::int64_t table_id) const;

    [[nodiscard]] std::string delete_point_sql(std::int64_t table_id) const;

    [[nodiscard]] std::string delete_scan_sql(std::int64_t table_id) const;

    /**
     * @brief select statements
     * @param condition_column the column for the condition, or empty to use the primary key column
     */
    [[nodiscard]] std::string select_full_scan_sql(std::int64_t table_id, std::string_view columns) const;

    [[nodiscard]] std::string select_point_sql(
        std::int64_t table_id,
        std::string_view columns,
        std::string_view condition_column
    ) const;

    [[nodiscard]] std::string select_range_sql(
        std::int64_t table_id,
        std::string_view columns,
        std::string_view condition_column
    ) const;

    /**
     * @brief placeholders for the row values (used by insert/update)
     */
    [[nodiscard]] placeholders row_placeholders() const;

    /**
     * @brief placeholders for the key value(s) used by point read/delete (`value`) and scan (`from`, `to`)
     */
    [[nodiscard]] placeholders point_placeholders() const;

    [[nodiscard]] placeholders range_placeholders() const;

    /**
     * @brief create the row values with the primary key `id`
     * @details non-key values are generated randomly as sbench does. Integer values are taken from [origin, bound).
     */
    [[nodiscard]] parameters row(std::int64_t id, std::int64_t origin, std::int64_t bound, random_engine& rnd) const;

    [[nodiscard]] parameters point(std::int64_t value) const;

    [[nodiscard]] parameters range(std::int64_t from, std::int64_t to) const;

private:
    kind kind_{kind::lkv};

    [[nodiscard]] std::string_view prefix() const noexcept;

    [[nodiscard]] std::string_view pk_column() const noexcept;

    [[nodiscard]] jogasaki::utils::parameter key_parameter(std::string name, std::int64_t value) const;
};

}  // namespace tateyama::nsbench
