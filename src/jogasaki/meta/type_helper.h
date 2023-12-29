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

#include <cstddef>
#include <type_traits>
#include <variant>
#include <ostream>
#include <memory>

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_option.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>

namespace jogasaki::meta {

field_type int1_type() {
    return field_type{field_enum_tag<field_type_kind::int1>};
}

field_type int2_type() {
    return field_type{field_enum_tag<field_type_kind::int2>};
}

field_type int4_type() {
    return field_type{field_enum_tag<field_type_kind::int4>};
}

field_type int8_type() {
    return field_type{field_enum_tag<field_type_kind::int8>};
}

field_type float4_type() {
    return field_type{field_enum_tag<field_type_kind::float4>};
}

field_type float8_type() {
    return field_type{field_enum_tag<field_type_kind::float8>};
}

field_type decimal_type(std::optional<std::size_t> precision = std::nullopt, std::optional<std::size_t> scale = std::nullopt) {
    return field_type{std::make_shared<decimal_field_option>(precision, scale)};
}

field_type character_type(bool varying = true, std::optional<std::size_t> length = std::nullopt) {
    return field_type{std::make_shared<character_field_option>(varying, length)};
}

field_type octet_type(bool varying = true, std::optional<std::size_t> length = std::nullopt) {
    return field_type{field_enum_tag<field_type_kind::octet>};
}

field_type date_type() {
    return field_type{field_enum_tag<field_type_kind::date>};
}

field_type time_of_day_type(bool with_offset = false) {
    return field_type{std::make_shared<time_of_day_field_option>(with_offset)};
}

field_type time_point_type(bool with_offset = false) {
    return field_type{std::make_shared<time_point_field_option>(with_offset)};
}

}  // namespace jogasaki::meta
