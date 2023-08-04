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
#include <takatori/util/string_builder.h>
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
#include "jogasaki/executor/process/impl/expression/evaluator_context.h"

namespace jogasaki::executor::process::impl::expression::details {

using takatori::decimal::triple;
using takatori::util::string_builder;

template <class T>
inline const decimal::Decimal int_max{std::numeric_limits<T>::max()};

template <class T>
inline const decimal::Decimal int_min{std::numeric_limits<T>::min()};

template <class T>
T to(decimal::Decimal const& d) {
    if constexpr (std::is_same_v<T, std::int32_t>) {
        return d.i32();
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
        return d.i64();
    }
    std::abort();
}

any handle_inexact_conversion(evaluator_context& ctx, decimal::Decimal const& d, decimal::Decimal const& dd, bool& error_return) {
    error_return = false;
    if((decimal::context.status() & MPD_Inexact) != 0) {
        // inexact operation
        switch(ctx.get_cast_loss_policy()) {
            case cast_loss_policy::ignore: break;
            case cast_loss_policy::floor: error_return = true; return {std::in_place_type<error>, error(error_kind::unsupported)};
            case cast_loss_policy::ceil: error_return = true; return {std::in_place_type<error>, error(error_kind::unsupported)};
            case cast_loss_policy::unknown: error_return = true; return {};
            case cast_loss_policy::warn: {
                ctx.add_error({error_kind::cast_failure, string_builder{} << "cast warning src:" << d << " dest:" << dd << string_builder::to_string});
                break;
            }
            case cast_loss_policy::error: error_return = true; return {std::in_place_type<error>, error(error_kind::cast_failure)};
        }
    }
    return {};
}

/**
 * @tparam T type used to validate the value range
 * @tparam E type used to store in any
 */
template <class T, class E = T>
any to_int(std::string_view s, evaluator_context& ctx) {
    auto a = to_decimal(s, ctx);
    if(! a) {
        return a;
    }
    decimal::Decimal d{a.to<triple>()};
    decimal::context.clear_status();
    auto dd = d.rescale(0);
    bool error_return{false};
    if(auto a = handle_inexact_conversion(ctx, d, dd, error_return); error_return) {
        return a;
    }
    if(dd < int_min<T> || int_max<T> < dd) {
        return {std::in_place_type<error>, error(error_kind::overflow)};
    }
    return {std::in_place_type<E>, to<E>(dd)};
}

any to_float4(std::string_view s, evaluator_context& ctx) {
    (void) ctx;
    auto a = to_decimal(s, ctx);
    if(! a) {
        return a;
    }
    decimal::Decimal d{a.to<triple>()};
    float value{};
    try {
        // std::from_chars is preferable, but it is not available for float/double until gcc 11
        value = std::stof(d.to_sci());
    } catch (std::exception& e) {
        return any{std::in_place_type<error>, error(error_kind::overflow)};
    }
    return any{std::in_place_type<float>, value};
}

any to_float8(std::string_view s, evaluator_context& ctx) {
    (void) ctx;
    auto a = to_decimal(s, ctx);
    if(! a) {
        return a;
    }
    decimal::Decimal d{a.to<triple>()};
    double value{};
    try {
        // std::from_chars is preferable, but it is not available for float/double until gcc 11
        value = std::stod(d.to_sci());
    } catch (std::exception& e) {
        return any{std::in_place_type<error>, error(error_kind::overflow)};
    }
    return any{std::in_place_type<double>, value};
}

any to_decimal(std::string_view s, evaluator_context& ctx) {
    (void) ctx;
    decimal::context = decimal::IEEEContext(128);
    decimal::Decimal value{std::string{s}};
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }

    auto tri = value.as_uint128_triple();
    if(tri.tag != MPD_TRIPLE_NORMAL) {
        // out of the range that triple can handle
        return any{std::in_place_type<error>, error(error_kind::overflow)};
    }
    return any{std::in_place_type<triple>, value};
}

any to_decimal(takatori::decimal::triple dec, evaluator_context& ctx, std::optional<std::size_t> precision, std::optional<std::size_t> scale) {
    (void) ctx;
    (void) precision;
    decimal::context = decimal::IEEEContext(128);
    decimal::Decimal value{dec};
    if(scale.has_value()) {
        decimal::context.clear_status();
        auto y = value.rescale(-static_cast<std::int64_t>(*scale));
        bool error_return{false};
        if(auto a = handle_inexact_conversion(ctx, value, y, error_return); error_return) {
            return a;
        }
        return any{std::in_place_type<triple>, y};
    }
    return any{std::in_place_type<triple>, value};
}

any to_boolean(std::string_view s, evaluator_context& ctx) {
    (void) ctx;
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

any to_int1(std::string_view s, evaluator_context& ctx) {
    return to_int<std::int8_t, std::int32_t>(s, ctx);
}

any to_int2(std::string_view s, evaluator_context& ctx) {
    return to_int<std::int16_t, std::int32_t>(s, ctx);
}

any to_int4(std::string_view s, evaluator_context& ctx) {
    return to_int<std::int32_t>(s, ctx);
}

any to_int8(std::string_view s, evaluator_context& ctx) {
    return to_int<std::int64_t>(s, ctx);
}

any from_character(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    auto txt = a.to<runtime_t<meta::field_type_kind::character>>();
    auto sv = trim_spaces(static_cast<std::string_view>(txt));
    switch(tgt.kind()) {
        case k::boolean: return to_boolean(sv, ctx);
        case k::int1: return to_int1(sv, ctx);
        case k::int2: return to_int2(sv, ctx);
        case k::int4: return to_int4(sv, ctx);
        case k::int8: return to_int8(sv, ctx);
        case k::float4: return to_float4(sv, ctx);
        case k::float8: return to_float8(sv, ctx);
        case k::decimal: return to_decimal(sv, ctx);
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

any from_decimal(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    auto dec = a.to<runtime_t<meta::field_type_kind::decimal>>();
    switch(tgt.kind()) {
        case k::boolean: break;
        case k::int1: break;
        case k::int2: break;
        case k::int4: break;
        case k::int8: break;
        case k::float4: break;
        case k::float8: break;
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return to_decimal(dec, ctx, t.precision(), t.scale());
        }
        case k::character: break;
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
    evaluator_context& ctx,
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
        case k::decimal: return from_decimal(ctx, tgt, a);
        case k::character: return from_character(ctx, tgt, a);
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
