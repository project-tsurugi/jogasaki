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

#include <cstdint>
#include <type_traits>

#include <fpdecimal/decimal.h>

#include <takatori/util/enum_tag.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_option.h>

namespace jogasaki::meta {

/**
 * @brief field type traits providing field type related information
 * @tparam Kind kind of the field type
 */
template <field_type_kind Kind>
struct field_type_traits;

template <class T1, class T2 = void>
struct simple_field_type_traits {
    using runtime_type = T1;
    using option_type = T2;
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
struct field_type_traits<field_type_kind::decimal> : simple_field_type_traits<fpdecimal::Decimal> {};

template <>
struct field_type_traits<field_type_kind::character> : simple_field_type_traits<accessor::text> {};

template <>
struct field_type_traits<field_type_kind::date> : simple_field_type_traits<takatori::datetime::date, date_field_option> {};

template <>
struct field_type_traits<field_type_kind::time_of_day> : simple_field_type_traits<takatori::datetime::time_of_day> {};

template <>
struct field_type_traits<field_type_kind::time_point> : simple_field_type_traits<takatori::datetime::time_point, time_point_field_option> {};

template <>
struct field_type_traits<field_type_kind::pointer> : simple_field_type_traits<void*> {};

//TODO add specialization for other types

template <meta::field_type_kind Kind>
using runtime_t = typename meta::field_type_traits<Kind>::runtime_type;

} // namespace

