/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <string_view>
#include <type_traits>

#include <jogasaki/lob/blob_locator.h>
#include <jogasaki/lob/blob_reference.h>
#include <jogasaki/lob/clob_locator.h>
#include <jogasaki/lob/clob_reference.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/api/field_type_kind.h>

namespace jogasaki::api {

/**
 * @brief field type traits providing field type related information
 * @tparam Kind kind of the field type
 */
template <field_type_kind Kind>
struct field_type_traits;

template <class T1, class T2 = void, class T3 = T1>
struct simple_field_type_traits {
    using runtime_type = T1;
    using option_type = T2;
    using parameter_type = T3;
    static constexpr std::size_t size = sizeof(runtime_type);
    static constexpr std::size_t alignment = alignof(runtime_type);
};

template <>
struct field_type_traits<field_type_kind::boolean> : simple_field_type_traits<std::int8_t> {};

template <>
struct field_type_traits<field_type_kind::int4> : simple_field_type_traits<std::int32_t> {};

template <>
struct field_type_traits<field_type_kind::int1> : field_type_traits<field_type_kind::int4> {};

template <>
struct field_type_traits<field_type_kind::int2> : field_type_traits<field_type_kind::int4> {};

template <>
struct field_type_traits<field_type_kind::int8> : simple_field_type_traits<std::int64_t> {};

template <>
struct field_type_traits<field_type_kind::float4> : simple_field_type_traits<float> {};

template <>
struct field_type_traits<field_type_kind::float8> : simple_field_type_traits<double> {};

template <>
struct field_type_traits<field_type_kind::decimal> : simple_field_type_traits<takatori::decimal::triple> {};

template <>
struct field_type_traits<field_type_kind::character> : simple_field_type_traits<std::string_view> {}; // use string_view for simplicity

template <>
struct field_type_traits<field_type_kind::octet> : simple_field_type_traits<std::string_view> {}; // use string_view for simplicity

template <>
struct field_type_traits<field_type_kind::date> : simple_field_type_traits<takatori::datetime::date> {};

template <>
struct field_type_traits<field_type_kind::time_of_day> : simple_field_type_traits<takatori::datetime::time_of_day> {};

template <>
struct field_type_traits<field_type_kind::time_point> : simple_field_type_traits<takatori::datetime::time_point> {};

template <>
struct field_type_traits<field_type_kind::blob> : simple_field_type_traits<lob::blob_reference, void, lob::blob_locator> {};

template <>
struct field_type_traits<field_type_kind::clob> : simple_field_type_traits<lob::clob_reference, void, lob::clob_locator> {};

template <>
struct field_type_traits<field_type_kind::pointer> : simple_field_type_traits<void*> {};

//TODO add specialization for other types

} // namespace

