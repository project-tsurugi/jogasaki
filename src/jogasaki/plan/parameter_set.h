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

#include <mizugaki/placeholder_map.h>

#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::plan {

/**
 * @brief parameters for place holders
 */
class parameter_set {
public:
    using kind = meta::field_type_kind;

    template<kind Kind>
    using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;

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
    void set_int4(std::string_view name, runtime_type<kind::int4> value);
    void set_int8(std::string_view name, runtime_type<kind::int8> value);
    void set_float4(std::string_view name, runtime_type<kind::float4> value);
    void set_float8(std::string_view name, runtime_type<kind::float8> value);
    void set_character(std::string_view name, runtime_type<kind::character> value);

    /**
     * @brief nullity setter functions for placeholder
     * @param name the place-holder name without colon at the beginning
     */
    void set_null(std::string_view name);

    /**
     * @brief accessor to the placeholder map
     * @return the placeholder map held by this object
     */
    [[nodiscard]] mizugaki::placeholder_map const& map() const noexcept;

private:
    mizugaki::placeholder_map map_{};
};

}
