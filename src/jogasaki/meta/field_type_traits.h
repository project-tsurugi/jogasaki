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

#include <cstdint>
#include <type_traits>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/lob/blob_locator.h>
#include <jogasaki/lob/blob_reference.h>
#include <jogasaki/lob/clob_locator.h>
#include <jogasaki/lob/clob_reference.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_option.h>
#include <jogasaki/meta/octet_field_option.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>

namespace jogasaki::meta {

/**
 * @brief field type traits providing field type related information
 * @tparam Kind kind of the field type
 */
template <field_type_kind Kind>
struct field_type_traits;

template <class T1, class T2 = void, class T3 = T1, class T4 = T1>
struct simple_field_type_traits {
    using runtime_type = T1;
    using option_type = T2;
    using value_range = T3;
    using parameter_type = T4;
    static constexpr std::size_t size = sizeof(runtime_type);
    static constexpr std::size_t alignment = alignof(runtime_type);
};

template <>
struct field_type_traits<field_type_kind::boolean> : simple_field_type_traits<std::int8_t> {};

template <>
struct field_type_traits<field_type_kind::int4> : simple_field_type_traits<std::int32_t> {};

template <>
struct field_type_traits<field_type_kind::int1> : simple_field_type_traits<std::int32_t, void, std::int8_t> {};

template <>
struct field_type_traits<field_type_kind::int2> : simple_field_type_traits<std::int32_t, void, std::int16_t> {};

template <>
struct field_type_traits<field_type_kind::int8> : simple_field_type_traits<std::int64_t> {};

template <>
struct field_type_traits<field_type_kind::float4> : simple_field_type_traits<float> {};

template <>
struct field_type_traits<field_type_kind::float8> : simple_field_type_traits<double> {};

template <>
struct field_type_traits<field_type_kind::decimal> : simple_field_type_traits<takatori::decimal::triple, decimal_field_option> {};

template <>
struct field_type_traits<field_type_kind::character> : simple_field_type_traits<accessor::text, character_field_option> {};

template <>
struct field_type_traits<field_type_kind::octet> : simple_field_type_traits<accessor::binary, octet_field_option> {};

template <>
struct field_type_traits<field_type_kind::date> : simple_field_type_traits<takatori::datetime::date> {};

template <>
struct field_type_traits<field_type_kind::time_of_day> : simple_field_type_traits<takatori::datetime::time_of_day, time_of_day_field_option> {};

template <>
struct field_type_traits<field_type_kind::time_point> : simple_field_type_traits<takatori::datetime::time_point, time_point_field_option> {};

template <>
struct field_type_traits<field_type_kind::blob> : simple_field_type_traits<lob::blob_reference, void, lob::blob_reference, lob::blob_locator> {};

template <>
struct field_type_traits<field_type_kind::clob> : simple_field_type_traits<lob::clob_reference, void, lob::clob_reference, lob::clob_locator> {};

template <>
struct field_type_traits<field_type_kind::pointer> : simple_field_type_traits<void*> {};

template <>
struct field_type_traits<field_type_kind::unknown> {
    // unknown is the field type for null literals
    // This type actually doesn't store real value, but the trait is defined for compatibility.
    // Treat this type as if it's 0 length character string.
    using runtime_type = char;
    static constexpr std::size_t size = 0;
    static constexpr std::size_t alignment = 1;
};

template <>
struct field_type_traits<field_type_kind::undefined> {
    // undefined is the field type that is not supported (e.g. load file contains unsupported type)
    // This type actually doesn't store real value, but the trait is defined for compatibility.
    // Treat this type as if it's 0 length character string.
    using runtime_type = char;
    using option_type = void;
    static constexpr std::size_t size = 0;
    static constexpr std::size_t alignment = 1;
};

//TODO add specialization for other types

} // namespace

namespace jogasaki {

///@brief short name for runtime type
template <meta::field_type_kind Kind>
using runtime_t = typename meta::field_type_traits<Kind>::runtime_type;

///@brief short name for parameter type
template <meta::field_type_kind Kind>
using parameter_t = typename meta::field_type_traits<Kind>::parameter_type;

}

