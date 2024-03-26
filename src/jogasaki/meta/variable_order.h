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

#include <cassert>
#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/utils/field_types.h>

namespace jogasaki::meta {

using takatori::descriptor::variable;
using takatori::util::sequence_view;

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
 * @details This is the central place to collectively handle all rules to determine the columns order.
 * The engine defines variables order based on ordering kind provided to the constructor.
 * This class objects represents the order by providing map from variables to 0-origin index.
 * The order can be defined for group (i.e. key/value fields are separately ordered) or flat record (i.e. all fields
 * are ordered as if they are on the flat record).
 */
class variable_order {
public:
    using index_type = std::size_t;
    using entity_type = std::unordered_map<variable, index_type>;
    using key_bool_type = std::unordered_map<variable, bool>;
    using const_iterator = std::vector<variable>::const_iterator;

    /**
     * @brief create empty object
     */
    variable_order() = default;

    /**
     * @brief create new object from flat record
     * @param columns ordered list of flat record columns
     */
    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record>,
        sequence_view<variable const> columns
    );

    /**
     * @brief create new object from key and value fields
     * @details the keys and values are concatenated sequentially to form a order
     * @param keys ordered list of key fields
     * @param values ordered list of value fields
     */
    variable_order(
        variable_ordering_enum_tag_t<variable_ordering_kind::flat_record_from_keys_values>,
        sequence_view<variable const> keys,
        sequence_view<variable const> values
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
        sequence_view<variable const> columns,
        sequence_view<variable const> group_keys
    );

    /**
     * @brief return the index of the variable
     * @param var the variable to query the index with
     * @return the index of the variable. If this object is for group ordering (for_group is true), index for key
     * fields and one for value fields are 0-origin respectively.
     */
    [[nodiscard]] index_type index(variable const& var) const;

    /**
     * @brief query the variable index together with the flag whether its for key
     * @param var the variable to query
     * @return pair of index and the boolean flag showing the variable is part of the key
     * @note this can be used only for the group ordering (i.e. for_group() returns true)
     */
    [[nodiscard]] std::pair<index_type, bool> key_value_index(variable const& var) const;

    /**
     * @brief return whether the variable order is for group, i.e. key/value fields are ordered separately
     */
    [[nodiscard]] bool for_group() const noexcept;

    /**
     * @brief return whether the variable is for key fields of the group
     * @param var the variable to query
     * @note this can be used only for the group ordering (i.e. for_group() returns true)
     */
    [[nodiscard]] bool is_key(variable const& var) const;

    /**
     * @brief return the number of variables registered to this object
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief return the number of key fields
     * @note this can be used only for the group ordering (i.e. for_group() returns true)
     */
    [[nodiscard]] std::size_t key_count() const noexcept;

    /**
     * @brief return the begin iterator of the key fields (for group ordering) or record fields (for flat record ordering)
     */
    [[nodiscard]] const_iterator begin() const noexcept;

    /**
     * @brief return the end iterator of the key fields (for group ordering) or record fields (for flat record ordering)
     */
    [[nodiscard]] const_iterator end() const noexcept;

    /**
     * @brief return the begin iterator to iterate value fields
     * @note this can be used only for the group ordering (i.e. for_group() returns true)
     */
    [[nodiscard]] const_iterator value_begin() const noexcept;

    /**
     * @brief return the end iterator of the value fields
     * @note this can be used only for the group ordering (i.e. for_group() returns true)
     */
    [[nodiscard]] const_iterator value_end() const noexcept;

private:
    entity_type entity_;
    key_bool_type key_bool_{};
    bool for_group_{false};
    std::vector<variable> record_or_key_{};
    std::vector<variable> value_{};

    void fill_flat_record(
        entity_type& entity,
        sequence_view<variable const> columns,
        std::size_t begin_offset = 0
    );
};

}

