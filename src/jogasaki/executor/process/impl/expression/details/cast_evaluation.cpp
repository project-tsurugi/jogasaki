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
#include "cast_evaluation.h"

#include <cstddef>
#include <functional>
#include <charconv>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/walk.h>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/data/any.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/variant.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/utils/checkpoint_holder.h>

#include "common.h"

namespace jogasaki::executor::process::impl::expression::details {

using jogasaki::data::any;
using takatori::decimal::triple;
using takatori::util::fail;

template <class T>
inline decimal::Decimal int_max{std::numeric_limits<T>::max()};

template <class T>
inline decimal::Decimal int_min{std::numeric_limits<T>::min()};

template <class T>
T to(decimal::Decimal const& d) {
    if constexpr (std::is_same_v<T, std::int32_t>) {
        return d.i32();
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
        return d.i64();
    }
    std::abort();
}

template <class T>
any to_int(std::string_view s) {
    auto a = to_decimal(s);
    if(! a) {
        return a;
    }
    decimal::Decimal d{a.to<triple>()};
    decimal::context.clear_status();
    d = d.rescale(0);
    if((decimal::context.status() & MPD_Inexact) != 0) {
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    if(d < int_min<T> || int_max<T> < d) {
        return any{std::in_place_type<error>, error(error_kind::overflow)};
    }
    return any{std::in_place_type<T>, to<T>(d)};
}

// string to numeric conversion should be done via decimal,
// but in order to to support float representation outside decimal range such as 1E100, we use stof/stod for now TODO
any to_float4(std::string_view s) {
    float value{};
    try {
        // std::from_chars for float/double is not available until gcc 11
        value = std::stof(std::string{s});
    } catch (std::exception& e) {
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    return any{std::in_place_type<float>, value};
}

any to_float8(std::string_view s) {
    double value{};
    try {
        // std::from_chars for float/double is not available until gcc 11
        value = std::stod(std::string{s});
    } catch (std::exception& e) {
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    return any{std::in_place_type<double>, value};
}

any to_decimal(std::string_view s) {
    decimal::context.clear_status();
    decimal::Decimal value{std::string{s}};
    if((decimal::context.status() & MPD_Inexact) != 0) {
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    return any{std::in_place_type<triple>, value};
}

any to_boolean(std::string_view s) {
    runtime_t<meta::field_type_kind::boolean> value{};
    if(is_prefix_of_case_insensitive(s, "true")) {
        value = 1;
    } else if (is_prefix_of_case_insensitive(s, "false")) {
        value = 0;
    } else {
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    return any{std::in_place_type<runtime_t<meta::field_type_kind::boolean>>, value};
}



any to_int1(std::string_view s) {
    return to_int<std::int32_t>(s);
}

any to_int2(std::string_view s) {
    return to_int<std::int32_t>(s);
}

any to_int4(std::string_view s) {
    return to_int<std::int32_t>(s);
}

any to_int8(std::string_view s) {
    return to_int<std::int64_t>(s);
}

any from_character(
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    auto txt = a.to<runtime_t<meta::field_type_kind::character>>();
    auto sv = trim_spaces(static_cast<std::string_view>(txt));
    switch(tgt.kind()) {
        case k::boolean: return to_boolean(sv);
        case k::int1: return to_int1(sv);
        case k::int2: return to_int2(sv);
        case k::int4: return to_int4(sv);
        case k::int8: return to_int8(sv);
        case k::float4: return to_float4(sv);
        case k::float8: return to_float8(sv);
        case k::decimal: return to_decimal(sv);
        case k::character: return a;
        case k::octet: break;
        case k::bit: break;
        case k::date: break;
        case k::time_of_day: break;
        case k::time_point: break;
        case k::datetime_interval: break;
        case k::array: break;
        case k::record: break;
        case k::unknown: break;
        case k::row_reference: break;
        case k::row_id: break;
        case k::declared: break;
        case k::extension: break;
    }
    return return_unsupported();
}

any conduct_cast(
    ::takatori::type::data const& src,
    ::takatori::type::data const& tgt,
    any const& a
    ) {
    using k = takatori::type::type_kind;
    switch(src.kind()) {
        case k::boolean: break;
        case k::int1: break;
        case k::int2: break;
        case k::int4: break;
        case k::int8: break;
        case k::float4: break;
        case k::float8: break;
        case k::decimal: break;
        case k::character: return from_character(tgt, a);
        case k::octet: break;
        case k::bit: break;
        case k::date: break;
        case k::time_of_day: break;
        case k::time_point: break;
        case k::datetime_interval: break;
        case k::array: break;
        case k::record: break;
        case k::unknown: break;
        case k::row_reference: break;
        case k::row_id: break;
        case k::declared: break;
        case k::extension: break;
    }
    return return_unsupported();
}

} // namespace
