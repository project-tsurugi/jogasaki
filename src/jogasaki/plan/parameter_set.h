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

#include <unordered_map>

#include <takatori/util/optional_ptr.h>

#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/plan/parameter_entry.h>

namespace jogasaki::plan {

using takatori::util::optional_ptr;

/**
 * @brief parameters for place holders
 */
class parameter_set {
public:
    using kind = meta::field_type_kind;

    /// @brief the entry type.
    using entry_type = parameter_entry;

    /// @brief the entity type.
    using entity_type = std::unordered_map<std::string, entry_type>;

    /// @brief the iterator type.
    using iterator = entity_type::const_iterator;

    /**
     * @brief create new object
     */
    parameter_set() = default;

    /**
     * @brief destruct the object
     */
    ~parameter_set() = default;

    parameter_set(parameter_set const& other) = default;
    parameter_set(parameter_set&& other) noexcept = default;  //NOLINT(performance-noexcept-move-constructor,hicpp-noexcept-move)
    parameter_set& operator=(parameter_set const& other) = default;
    parameter_set& operator=(parameter_set&& other) noexcept = default;

    /**
     * @brief setter functions for placeholder values
     * @param name the place-holder name without colon at the beginning
     * @param value the value to assign for the placeholder
     */
    void set_int4(std::string_view name, runtime_t<kind::int4> value);
    void set_int8(std::string_view name, runtime_t<kind::int8> value);
    void set_float4(std::string_view name, runtime_t<kind::float4> value);
    void set_float8(std::string_view name, runtime_t<kind::float8> value);
    void set_character(std::string_view name, runtime_t<kind::character> value);
    void set_decimal(std::string_view name, runtime_t<kind::decimal> value);
    void set_date(std::string_view name, runtime_t<kind::date> value);
    void set_time_of_day(std::string_view name, runtime_t<kind::time_of_day> value);
    void set_time_point(std::string_view name, runtime_t<kind::time_point> value);

    void set_reference_column(std::string_view name, std::size_t position);
    void set_reference_column(std::string_view name, std::string_view column_name);
    /**
     * @brief nullity setter functions for placeholder
     * @param name the place-holder name without colon at the beginning
     */
    void set_null(std::string_view name);

    /**
     * @brief return the number of entries held by this object
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief find the entry by name
     * @param name the name used to search
     * @return the found entry, or null
     */
    [[nodiscard]] optional_ptr<entry_type const> find(std::string_view name) const;

    /**
     * @brief return begin iterator
     */
    [[nodiscard]] iterator begin() const noexcept;

    /**
     * @brief return end iterator
     */
    [[nodiscard]] iterator end() const noexcept;

private:
    entity_type map_{};

    void add(std::string name, entry_type entry);
};

}
