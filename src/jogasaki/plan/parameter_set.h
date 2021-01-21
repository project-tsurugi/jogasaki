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
#include <mizugaki/placeholder_entry.h>

#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::plan {

/**
 * @brief parameter for place holders
 */
class parameter_set {
public:
    using kind = meta::field_type_kind;

    parameter_set() = default;
    ~parameter_set() = default;
    parameter_set(parameter_set const& other) = default;
    parameter_set(parameter_set&& other) = default;
    parameter_set& operator=(parameter_set const& other) = default;
    parameter_set& operator=(parameter_set&& other) = default;

    void set_int4(std::string_view name, meta::field_type_traits<kind::int4>::runtime_type value);
    void set_int8(std::string_view name, meta::field_type_traits<kind::int8>::runtime_type value);
    void set_float4(std::string_view name, meta::field_type_traits<kind::float4>::runtime_type value);
    void set_float8(std::string_view name, meta::field_type_traits<kind::float8>::runtime_type value);
    void set_character(std::string_view name, meta::field_type_traits<kind::character>::runtime_type value);
    [[nodiscard]] mizugaki::placeholder_map const& map() const noexcept;
private:
    mizugaki::placeholder_map map_{};
};

}
