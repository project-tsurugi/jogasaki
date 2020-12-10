/*
 * Copyright 2018-2020 tsurugi project.
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

#include <cassert>

#include <takatori/descriptor/variable.h>
#include <takatori/util/fail.h>

#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::meta {

using takatori::util::fail;
using takatori::descriptor::variable;

enum class variable_ordering_kind {
    ///@brief flat record (for forward exchange)
    flat_record,

    ///@brief construct record from keys and values (scan operator)
    flat_record_from_keys_values,

    ///@brief construct group meta from keys selected from values (group exchange)
    group_from_keys,
};

template<auto Kind>
struct variable_ordering_enum_tag_t {
    explicit variable_ordering_enum_tag_t() = default;
};

template<auto Kind>
inline constexpr variable_ordering_enum_tag_t<Kind> variable_ordering_enum_tag{};

/**
 * @brief represents variables order in a relation (e.g. exchange, index, or table)
 *
 * @details This is the central place to collectively handle all rules to determine the columns order.
 * The engine defines variables order based on ordering kind provided to the constructor.
 * This class objects represents the order by providing map from variables to 0-origin index.
 */
class variable_order {
public:
    using variable_index_type = std::size_t;
    using entity_type = std::unordered_map<variable, variable_index_type>;
    using key_bool_type = std::unordered_map<variable, bool>;

    variable_order() = default;

    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record>,
        takatori::util::sequence_view<variable const> columns
    );

    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record_from_keys_values>,
        takatori::util::sequence_view<variable const> keys,
        takatori::util::sequence_view<variable const> values
    );

    /**
     * @brief create variable order specifying grouping keys
     * @details this defines order of keys and values columns. with key columns first, and non-key columns follow.
     * @param columns the columns where order is defined. This may be part of the columns in the relation (e.g. exchange
     * can expose only a part of its columns.)
     * @param group_keys the key columns to define groups. The group key column may or may not appear in columns.
     */
    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::group_from_keys>,
        takatori::util::sequence_view<variable const> columns,
        takatori::util::sequence_view<variable const> group_keys
    );

    [[nodiscard]] variable_index_type index(variable const& var) const;

    [[nodiscard]] std::pair<variable_index_type, bool> key_value_index(variable const& var) const;

    [[nodiscard]] bool for_group() const noexcept;

    [[nodiscard]] bool is_key(variable const& var) const;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] std::size_t key_count() const noexcept;
private:
    entity_type entity_;
    key_bool_type key_bool_{};
    bool for_group_{false};

    void fill_flat_record(
        entity_type& entity,
        takatori::util::sequence_view<variable const> columns,
        std::size_t begin_offset = 0
    );
};

}

