/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <functional>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor::expr::details {

constexpr std::size_t max_triple_digits = decimal_max_precision;

constexpr std::int64_t decimal_context_emax = 24576;

constexpr std::int64_t decimal_context_emin = -24575;

constexpr std::int64_t decimal_context_etiny = -24612;

constexpr std::int64_t max_triple_exponent = decimal_context_emax - static_cast<std::int64_t>(max_triple_digits - 1);

using kind = meta::field_type_kind;

template <kind Kind>
using runtime_type = typename meta::field_type_traits<Kind>::runtime_type;

template <kind Int, kind Float>
constexpr runtime_type<Int> max_integral_float_convertible_to_int_source;

template <kind Int, kind Float>
constexpr runtime_type<Float> max_integral_float_convertible_to_int =  //NOLINT(google-readability-casting) false positive
    static_cast<runtime_type<Float>>(max_integral_float_convertible_to_int_source<Int, Float>);  //NOLINT(google-readability-casting) false positive

// from float4
template <>
inline constexpr runtime_type<kind::int1> max_integral_float_convertible_to_int_source<kind::int1, kind::float4> =
    static_cast<runtime_type<kind::int1>>(std::numeric_limits<std::int8_t>::max());

template <>
inline constexpr runtime_type<kind::int2> max_integral_float_convertible_to_int_source<kind::int2, kind::float4> =
    static_cast<runtime_type<kind::int2>>(std::numeric_limits<std::int16_t>::max());

// see expression_constants_test.cpp for the details of the magic numbers
template <>
inline constexpr runtime_type<kind::int4> max_integral_float_convertible_to_int_source<kind::int4, kind::float4> =
    static_cast<runtime_type<kind::int4>>(std::numeric_limits<std::int32_t>::max() - 127);

// see expression_constants_test.cpp for the details of the magic numbers
template <>
inline constexpr runtime_type<kind::int8> max_integral_float_convertible_to_int_source<kind::int8, kind::float4> =
    static_cast<runtime_type<kind::int8>>(std::numeric_limits<std::int64_t>::max() - (512L*1024L*1024L*1024L - 1));

// from float8
template <>
inline constexpr runtime_type<kind::int1> max_integral_float_convertible_to_int_source<kind::int1, kind::float8> =
    static_cast<runtime_type<kind::int1>>(std::numeric_limits<std::int8_t>::max());

template <>
inline constexpr runtime_type<kind::int2> max_integral_float_convertible_to_int_source<kind::int2, kind::float8> =
    static_cast<runtime_type<kind::int2>>(std::numeric_limits<std::int16_t>::max());

template <>
inline constexpr runtime_type<kind::int4> max_integral_float_convertible_to_int_source<kind::int4, kind::float8> =
    static_cast<runtime_type<kind::int4>>(std::numeric_limits<std::int32_t>::max());

// see expression_constants_test.cpp for the details of the magic numbers
template <>
inline constexpr runtime_type<kind::int8> max_integral_float_convertible_to_int_source<kind::int8, kind::float8> =
    static_cast<runtime_type<kind::int8>>(std::numeric_limits<std::int64_t>::max() - 1023);

template <kind Int, kind Float>
constexpr runtime_type<Int> min_integral_float_convertible_to_int_source;

template <kind Int, kind Float>
constexpr runtime_type<Float> min_integral_float_convertible_to_int =  //NOLINT(google-readability-casting) false positive
    static_cast<runtime_type<Float>>(min_integral_float_convertible_to_int_source<Int, Float>);  //NOLINT(google-readability-casting) false positive

// from float4
template <>
inline constexpr runtime_type<kind::int1> min_integral_float_convertible_to_int_source<kind::int1, kind::float4> =
    static_cast<runtime_type<kind::int1>>(std::numeric_limits<std::int8_t>::min());  //NOLINT(bugprone-signed-char-misuse,cert-str34-c)

template <>
inline constexpr runtime_type<kind::int2> min_integral_float_convertible_to_int_source<kind::int2, kind::float4> =
    static_cast<runtime_type<kind::int2>>(std::numeric_limits<std::int16_t>::min());

template <>
inline constexpr runtime_type<kind::int4> min_integral_float_convertible_to_int_source<kind::int4, kind::float4> =
    static_cast<runtime_type<kind::int4>>(std::numeric_limits<std::int32_t>::min());

template <>
inline constexpr runtime_type<kind::int8> min_integral_float_convertible_to_int_source<kind::int8, kind::float4> =
    static_cast<runtime_type<kind::int8>>(std::numeric_limits<std::int64_t>::min());

// from float8
template <>
inline constexpr runtime_type<kind::int1> min_integral_float_convertible_to_int_source<kind::int1, kind::float8> =
    static_cast<runtime_type<kind::int1>>(std::numeric_limits<std::int8_t>::min());  //NOLINT(bugprone-signed-char-misuse,cert-str34-c)

template <>
inline constexpr runtime_type<kind::int2> min_integral_float_convertible_to_int_source<kind::int2, kind::float8> =
    static_cast<runtime_type<kind::int2>>(std::numeric_limits<std::int16_t>::min());

template <>
inline constexpr runtime_type<kind::int4> min_integral_float_convertible_to_int_source<kind::int4, kind::float8> =
    static_cast<runtime_type<kind::int4>>(std::numeric_limits<std::int32_t>::min());

template <>
inline constexpr runtime_type<kind::int8> min_integral_float_convertible_to_int_source<kind::int8, kind::float8> =
    static_cast<runtime_type<kind::int8>>(std::numeric_limits<std::int64_t>::min());

inline constexpr auto triple_max_of_decimal_38_0 =
    takatori::decimal::triple{1, 5421010862427522170, 687399551400673279, 0};

inline constexpr auto triple_max_of_decimal_38_0_plus_one =
    takatori::decimal::triple{1, 5421010862427522170, 687399551400673280, 0};

inline constexpr auto triple_max_of_decimal_38_0_plus_two =
    takatori::decimal::triple{1, 5421010862427522170, 687399551400673281, 0};

inline constexpr auto triple_min_of_decimal_38_0 =
    takatori::decimal::triple{-1, 5421010862427522170, 687399551400673279, 0};

inline constexpr auto triple_min_of_decimal_38_0_minus_one =
    takatori::decimal::triple{-1, 5421010862427522170, 687399551400673280, 0};

inline constexpr auto triple_min_of_decimal_38_0_minus_two =
    takatori::decimal::triple{-1, 5421010862427522170, 687399551400673281, 0};

inline constexpr auto triple_max =
    takatori::decimal::triple{1, 5421010862427522170, 687399551400673279, decimal_context_emax - 37};

inline constexpr auto triple_min =
    takatori::decimal::triple{-1, 5421010862427522170, 687399551400673279, decimal_context_emax - 37};

inline constexpr std::string_view string_positive_nan = "NaN";

inline constexpr std::string_view string_negative_nan = "-NaN";

inline constexpr std::string_view string_positive_infinity = "Infinity";

inline constexpr std::string_view string_negative_infinity = "-Infinity";

inline constexpr std::string_view string_positive_inf = "Inf";

inline constexpr std::string_view string_negative_inf = "-Inf";

}  // namespace jogasaki::executor::expr::details
