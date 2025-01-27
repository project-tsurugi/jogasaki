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

#include <string_view>
#include <memory>

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/field_type_traits.h>

namespace jogasaki::api {

using kind = field_type_kind;

/**
 * @brief parameter set interface
 * @details this is used to assign values to parameter (a.k.a placeholder) in the sql statement
 */
class parameter_set {
public:
    parameter_set() = default;
    virtual ~parameter_set() = default;
    parameter_set(parameter_set const& other) = delete;
    parameter_set& operator=(parameter_set const& other) = delete;
    parameter_set(parameter_set&& other) noexcept = delete;
    parameter_set& operator=(parameter_set&& other) noexcept = delete;

    /**
     * @brief setter for the placeholder
     * @param name the name of the placeholder without colon
     * @param value the value assigned to the placeholder
     */
    virtual void set_boolean(std::string_view name, field_type_traits<kind::boolean>::parameter_type value) = 0;
    virtual void set_int4(std::string_view name, field_type_traits<kind::int4>::parameter_type value) = 0;
    virtual void set_int8(std::string_view name, field_type_traits<kind::int8>::parameter_type value) = 0;
    virtual void set_float4(std::string_view name, field_type_traits<kind::float4>::parameter_type value) = 0;
    virtual void set_float8(std::string_view name, field_type_traits<kind::float8>::parameter_type value) = 0;
    virtual void set_character(std::string_view name, field_type_traits<kind::character>::parameter_type value) = 0;
    virtual void set_octet(std::string_view name, field_type_traits<kind::octet>::parameter_type value) = 0;
    virtual void set_decimal(std::string_view name, field_type_traits<kind::decimal>::parameter_type value) = 0;
    virtual void set_date(std::string_view name, field_type_traits<kind::date>::parameter_type value) = 0;
    virtual void set_time_of_day(std::string_view name, field_type_traits<kind::time_of_day>::parameter_type value) = 0;
    virtual void set_time_point(std::string_view name, field_type_traits<kind::time_point>::parameter_type value) = 0;

    virtual void set_reference_column(std::string_view name, std::size_t position) = 0;
    virtual void set_reference_column(std::string_view name, std::string_view column_name) = 0;

    /**
     * @brief setter for the nullity of the placeholder
     * @param name the name of the placeholder without colon
     */
    virtual void set_null(std::string_view name) = 0;

    template<std::size_t N>
    void set_character(std::string_view name, char const (&value)[N]) {  //NOLINT
        set_character(name, &value[0]);  // NOLINT
    }

    /**
     * @brief clone the parameter set
     * @return the cloned object
     */
    [[nodiscard]] virtual parameter_set* clone() const& = 0;
    [[nodiscard]] virtual parameter_set* clone() && = 0;
};

/**
 * @brief factory method to get the new parameter set
 * @return new parameter set
 */
std::unique_ptr<parameter_set> create_parameter_set();

}
