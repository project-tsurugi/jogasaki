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
#include "cast_evaluation.h"

#include <charconv>
#include <cstddef>
#include <functional>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/walk.h>
#include <takatori/type/character.h>
#include <takatori/type/float.h>
#include <takatori/type/int.h>
#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context_guard.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/variant.h>

#include "common.h"
#include "constants.h"

namespace jogasaki::executor::process::impl::expression::details {

using takatori::decimal::triple;
using takatori::util::string_builder;
using takatori::util::unsafe_downcast;
using takatori::util::throw_exception;

any supports_small_integers() {
    if(global::config_pool()->support_smallint()) {
        return {};
    }
    return {std::in_place_type<error>, error(error_kind::unsupported)};
}

/**
 * @brief validate integer is in the valid range
 * @tparam Target target type to validate the src against
 * @tparam TargetEffective target type used to store the result in any
 * @tparam Source source integer type
 * @param src the source integer to validate
 * @param ctx evaluator context
 * @return validated integer or error in any
 */
template <class Target, class TargetEffective = Target, class Source>
any handle_precision_lost(Source src, Target modified, evaluator_context& ctx) {
    switch(ctx.get_loss_precision_policy()) {
        case loss_precision_policy::ignore: break;
        case loss_precision_policy::floor: return {std::in_place_type<error>, error(error_kind::unsupported)};
        case loss_precision_policy::ceil: return {std::in_place_type<error>, error(error_kind::unsupported)};
        case loss_precision_policy::unknown: return {};  // null to indicate inexact conversion
        case loss_precision_policy::warn: {
            ctx.add_error(
                {error_kind::lost_precision,
                 string_builder{} << "value loses precision src:" << src << " modified:" << modified << string_builder::to_string}
            );
            break;
        }
        case loss_precision_policy::implicit: //fallthrough
        case loss_precision_policy::error: {
            ctx.add_error(
                {error_kind::lost_precision,
                 string_builder{} << "value loses precision src:" << src << " modified:" << modified << string_builder::to_string}
            );
            return {std::in_place_type<error>, error(error_kind::lost_precision)};
        }
    }
    return any{std::in_place_type<TargetEffective>, static_cast<TargetEffective>(modified)};
}

/**
 * @brief validate integer is in the valid range
 * @tparam Target target type to validate the src against
 * @tparam TargetEffective target type used to store the result in any
 * @tparam Source source integer type
 * @param src the source integer to validate
 * @param ctx evaluator context
 * @return validated integer or error in any
 */
template <class Target, class TargetEffective = Target, class Source>
any validate_integer_range_from_integer(Source src, evaluator_context& ctx) {
    static const Source maxTgt{std::numeric_limits<Target>::max()};
    static const Source minTgt{std::numeric_limits<Target>::min()};
    if(maxTgt < src) {
        return handle_precision_lost<Target, TargetEffective>(src, std::numeric_limits<Target>::max(), ctx);
    }
    if(src < minTgt) {
        return handle_precision_lost<Target, TargetEffective>(src, std::numeric_limits<Target>::min(), ctx);
    }
    return any{std::in_place_type<TargetEffective>, static_cast<TargetEffective>(src)};
}

/**
 * @brief validate integer is in the valid range
 * @tparam Target target type to validate the src against
 * @tparam TargetEffective target type used to store the result in any
 * @tparam Source source integer type
 * @param src the source integer to validate
 * @param ctx evaluator context
 * @return validated integer or error in any
 */
template <class Target, class TargetEffective = Target>
any validate_integer_range_from_decimal(decimal::Decimal const& src, evaluator_context& ctx) {
    if(src.isnan()) {
        auto& e = ctx.add_error({error_kind::arithmetic_error, "NaN is not supported for integer conversion"});
        e.new_argument() << src;
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    decimal::context.clear_status();
    decimal::Decimal rounded{};
    {
        decimal_context_guard guard{};
        guard.round(MPD_ROUND_DOWN);
        decimal::context.clear_status();
        rounded = src.to_integral();
        if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
            auto& e = ctx.add_error({error_kind::undefined, "unexpected error in converting decimal to integer"});
            e.new_argument() << src;
            e.new_argument() << rounded;
            return any{std::in_place_type<error>, error(error_kind::undefined)};
        }
    }
    static const decimal::Decimal maxTgt{std::numeric_limits<Target>::max()};
    static const decimal::Decimal minTgt{std::numeric_limits<Target>::min()};
    // src can be +INF/-INF
    if(maxTgt < src) {
        return handle_precision_lost<Target, TargetEffective>(src, std::numeric_limits<Target>::max(), ctx);
    }
    if(src < minTgt) {
        return handle_precision_lost<Target, TargetEffective>(src, std::numeric_limits<Target>::min(), ctx);
    }
    if(! src.isinteger()) {
        return handle_precision_lost<Target, TargetEffective>(src, static_cast<Target>(rounded.i64()), ctx);
    }
    return any{std::in_place_type<TargetEffective>, static_cast<TargetEffective>(rounded.i64())};
}

template <kind SrcKind, kind TargetKind, class Target, class TargetEffective = Target, class Source>
any validate_integer_range_from_float(Source const& src, evaluator_context& ctx) {
    if(std::isnan(src)) {
        auto& e = ctx.add_error({error_kind::arithmetic_error, "NaN is not supported for integer conversion"});
        e.new_argument() << src;
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    auto maxTgt = max_integral_float_convertible_to_int<TargetKind, SrcKind>;
    auto minTgt = min_integral_float_convertible_to_int<TargetKind, SrcKind>;
    if(maxTgt < src) {
        auto tgt = std::numeric_limits<Target>::max();
        return handle_precision_lost<Target, TargetEffective>(src, tgt, ctx);
    }
    if(src < minTgt) {
        auto tgt = std::numeric_limits<Target>::min();
        return handle_precision_lost<Target, TargetEffective>(src, tgt, ctx);
    }
    auto truncated = std::trunc(src);
    if(src != truncated) {
        return handle_precision_lost<Target, TargetEffective>(src, static_cast<Target>(truncated), ctx);
    }
    return any{std::in_place_type<TargetEffective>, static_cast<TargetEffective>(src)};
}

any handle_inexact_conversion(
    evaluator_context& ctx,
    decimal::Decimal const& d,
    decimal::Decimal const& dd,
    bool& error_return
) {
    error_return = false;
    if((decimal::context.status() & MPD_Inexact) != 0) {
        // inexact operation
        switch(ctx.get_loss_precision_policy()) {
            case loss_precision_policy::ignore: break;
            case loss_precision_policy::floor:
                error_return = true;
                return {std::in_place_type<error>, error(error_kind::unsupported)};
            case loss_precision_policy::ceil:
                error_return = true;
                return {std::in_place_type<error>, error(error_kind::unsupported)};
            case loss_precision_policy::unknown: error_return = true; return {};  // null to indicate inexact conversion
            case loss_precision_policy::warn: {
                auto& e = ctx.add_error({error_kind::lost_precision, "warning: conversion loses precision"});
                e.new_argument() << d;
                e.new_argument() << dd;
                break;
            }
            case loss_precision_policy::implicit: //fallthrough
            case loss_precision_policy::error:
                error_return = true;
                auto& e = ctx.add_error({error_kind::lost_precision, "conversion loses precision"});
                e.new_argument() << d;
                e.new_argument() << dd;
                return {std::in_place_type<error>, error(error_kind::lost_precision)};
        }
    }
    return {};
}

any create_max_decimal(evaluator_context& ctx, std::size_t precision, std::size_t scale, decimal::Decimal& out) {
    decimal::context.clear_status();
    decimal::Decimal dec{triple{1, 0, 1, static_cast<std::int32_t>(precision)}};
    dec = dec - 1;
    auto ret = dec.scaleb(-static_cast<std::int64_t>(scale));
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error in creating max decimal value prec:" << precision
                              << " scale:" << scale << string_builder::to_string}
        );
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    out = ret;
    return {};
}

any reduce_decimal(decimal::Decimal const& value, decimal::Decimal& out, evaluator_context& ctx) {
    decimal::context.clear_status();
    auto d = value.reduce();
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        auto& e = ctx.add_error({error_kind::undefined, "unknown error in reducing decimal value"});
        e.new_argument() << value;
        e.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    out = d;
    return {};
}

any handle_ps(
    decimal::Decimal d,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    if(! d.isfinite()) {
        throw_exception(std::logic_error{"special value is not supported"});
    }
    if(precision.has_value() && ! scale.has_value()) {
        ctx.add_error(
            {error_kind::unsupported,
             string_builder{} << "unsupported decimal conversion: scale:* precision:" << *precision
                              << string_builder::to_string}
        );
        return any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    if(! precision.has_value() && ! scale.has_value()) {
        return as_triple(d, ctx);
    }
    // scale has_value
    if(! precision.has_value()) {
        precision = decimal_default_precision;
    }

    // handle precision
    if(*precision < *scale) {
        throw_exception(std::logic_error{"precision must be greater than or equal to scale"});
    }
    if(auto a = reduce_decimal(d, d, ctx); a.error()) {
        return a;
    }
    if(d.exponent() > 0) {
        // extend integral part to full digits
        // e.g. 1.0E3 -> 1000
        decimal::context.clear_status();
        auto newd = d.rescale(0);
        if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
            auto& e = ctx.add_error({error_kind::undefined, "unexpected error in rescaling decimal value"});
            e.new_argument() << d;
            e.new_argument() << newd;
            return any{std::in_place_type<error>, error(error_kind::undefined)};
        }
        d = newd;
    }
    auto digits = d.coeff().adjexp() + 1;
    auto digits_prec = -d.exponent();

    if(static_cast<std::int64_t>(*precision-*scale) < digits-digits_prec) {
        decimal::Decimal mx{};
        if(auto a = create_max_decimal(ctx, *precision, *scale, mx); a.error()) {
            return a;
        }
        return as_triple(mx.copy_sign(d), ctx);
    }

    // handle scale
    decimal::context.clear_status();
    decimal::Decimal rescaled{};
    {
        decimal_context_guard guard{};
        guard.round(MPD_ROUND_DOWN);

        decimal::context.clear_status();
        rescaled = d.rescale(-static_cast<std::int64_t>(*scale));
        if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
            auto& e = ctx.add_error({error_kind::undefined, "unexpected error in rescaling decimal value"});
            e.new_argument() << d;
            e.new_argument() << rescaled;
            return any{std::in_place_type<error>, error(error_kind::undefined)};
        }
    }
    bool error_return{false};
    if(auto a = handle_inexact_conversion(ctx, d, rescaled, error_return); error_return) {
        return a;
    }
    d = rescaled;
    return as_triple(d, ctx);
}

template <class T>
inline const decimal::Decimal int_max{std::numeric_limits<T>::max()};

template <class T>
inline const decimal::Decimal int_min{std::numeric_limits<T>::min()};

any handle_length(
    std::string_view src,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding,
    bool lenient_remove_padding
);

any as_triple(
    decimal::Decimal const& d,
    evaluator_context& ctx
) {
    decimal::context.clear_status();
    decimal::Decimal r{};
    if(auto a = reduce_decimal(d, r, ctx); a.error()) {
        return a;
    }
    // TODO validate r is in the valid range
    return any{std::in_place_type<triple>, static_cast<triple>(r)};
}

namespace from_decimal {

template <class T>
T to(decimal::Decimal const& d) {
    if constexpr (std::is_same_v<T, std::int32_t>) {
        return d.i32();
    } else if constexpr (std::is_same_v<T, std::int64_t>) {
        return d.i64();
    }
    std::abort();
}

any to_decimal(
    triple dec,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    decimal::Decimal value{dec};
    return handle_ps(value, ctx, precision, scale);
}

any to_character(
    triple dec,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding
) {
    decimal::Decimal value{dec};
    auto s = value.to_sci();
    return handle_length(s, ctx, len, add_padding, false);
}

any to_int1(triple src, evaluator_context& ctx) {
    decimal::Decimal value{src};
    return validate_integer_range_from_decimal<std::int8_t, std::int32_t>(value, ctx);
}

any to_int2(triple src, evaluator_context& ctx) {
    decimal::Decimal value{src};
    return validate_integer_range_from_decimal<std::int16_t, std::int32_t>(value, ctx);
}

any to_int4(triple src, evaluator_context& ctx) {
    decimal::Decimal value{src};
    return validate_integer_range_from_decimal<std::int32_t, std::int32_t>(value, ctx);
}

any to_int8(triple src, evaluator_context& ctx) {
    decimal::Decimal value{src};
    return validate_integer_range_from_decimal<std::int64_t, std::int64_t>(value, ctx);
}

any decimal_to_float4(decimal::Decimal const& d, evaluator_context& ctx) {
    (void) ctx;
    float value{};
    try {
        // std::from_chars is preferable, but it is not available for float/double until gcc 11
        value = std::stof(d.to_sci());
    } catch (std::out_of_range& e) {
        // overflow
        if(d > 1) {
            return any{std::in_place_type<float>, std::numeric_limits<float>::infinity()};
        }
        if(d < -1) {
            return any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()};
        }
        // underflow
        return any{std::in_place_type<float>, (d.sign() > 0 ? 0.0F : -0.0F)};
    } catch (std::invalid_argument& e) {
        // unexpected error
        auto& err = ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error in converting decimal to float4:" << e.what()
                              << string_builder::to_string}
        );
        err.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    if(std::isnan(value)) {
        value = std::numeric_limits<float>::quiet_NaN();
    }
    return any{std::in_place_type<float>, value};
}

any to_float4(triple src, evaluator_context& ctx) {
    decimal::context.clear_status();
    decimal::Decimal value{src};
    return decimal_to_float4(value, ctx);
}

any decimal_to_float8(decimal::Decimal const& d, evaluator_context& ctx) {
    (void) ctx;
    double value{};
    try {
        // std::from_chars is preferable, but it is not available for float/double until gcc 11
        value = std::stod(d.to_sci());
    } catch (std::out_of_range& e) {
        // overflow
        if(d > 1) {
            return any{std::in_place_type<double>, std::numeric_limits<double>::infinity()};
        }
        if(d < -1) {
            return any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()};
        }
        // underflow
        return any{std::in_place_type<double>, (d.sign() > 0 ? 0.0 : -0.0)};
    } catch (std::invalid_argument& e) {
        // unexpected error
        auto& err = ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error in converting decimal to float8:" << e.what()
                              << string_builder::to_string}
        );
        err.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    if(std::isnan(value)) {
        value = std::numeric_limits<double>::quiet_NaN();
    }
    return any{std::in_place_type<double>, value};
}

any to_float8(triple src, evaluator_context& ctx) {
    decimal::context.clear_status();
    decimal::Decimal value{src};
    return decimal_to_float8(value, ctx);
}

}  // namespace from_decimal

namespace from_character {

bool is_valid_nan(std::string_view s) {
    // sign for nan is not meaningful, but we accept it for usability
    return
        equals_case_insensitive(s, "NaN") ||
        equals_case_insensitive(s, "+NaN") ||
        equals_case_insensitive(s, "-NaN");
}

/**
 * @brief convert string to decimal
 * This function is used internally to convert from string, to decimal, and then to target type.
 * @return error if the string is not a valid decimal
 * @return empty any if the conversion is successful (converted values is available in `out`, and `out` can be a special value)
*/
any to_decimal_internal(
    std::string_view s,
    evaluator_context& ctx,
    decimal::Decimal& out
) {
    decimal::context.clear_status();
    decimal::Decimal value{std::string{s}};
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        auto& e = ctx.add_error({error_kind::format_error, "invalid string passed for conversion"});
        e.new_argument() << s;
        e.new_argument() << value;
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    // do the digits validation first, otherwise, the exponent is too large and any operation can silently truncate the digits
    if(value.isspecial()) {
        out = value;
        return {};
    }
    // validate the value is in the valid triple (with digits checked) range
    // otherwise, truncate the coefficient and increase the exponent
    if(static_cast<std::int64_t>(max_triple_digits) < value.coeff().adjexp() + 1) {
        auto diff = value.coeff().adjexp() + 1 - static_cast<std::int64_t>(max_triple_digits);
        auto exp = value.exponent();
        {
            decimal_context_guard guard{};
            guard.round(MPD_ROUND_DOWN);
            value = value.rescale(exp+diff);
        }
    }
    if(value.isspecial()) {
        out = value;
        return {};
    }
    if(decimal_context_emax < value.adjexp() || value.adjexp() < decimal_context_emin) {
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    if(auto a = reduce_decimal(value, value, ctx); a.error()) {
        return a;
    }
    out = value;
    return {};
}

/**
 * @tparam T type used to validate the value range
 * @tparam E type used to store in any
 */
template <class T, class E = T>
any to_int(std::string_view s, evaluator_context& ctx) {
    decimal::Decimal d{};
    auto a = to_decimal_internal(s, ctx, d);
    if(a.error()) {
        return a;
    }
    if(d.isspecial()) {
        auto& e = ctx.add_error({error_kind::format_error, "special value passed for conversion to integral type"});
        e.new_argument() << s;
        e.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    decimal::context.clear_status();
    auto dd = d.rescale(0);
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        auto& e = ctx.add_error({error_kind::undefined, "unexpected error in rescaling decimal value"});
        e.new_argument() << d;
        e.new_argument() << dd;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }

    bool error_return{false};
    if(auto a = handle_inexact_conversion(ctx, d, dd, error_return); error_return) {
        return a;
    }
    if(dd < int_min<T>) {
        return handle_precision_lost<T, E>(s, std::numeric_limits<T>::min(), ctx);
    }
    if(int_max<T> < dd) {
        return handle_precision_lost<T, E>(s, std::numeric_limits<T>::max(), ctx);
    }
    return {std::in_place_type<E>, from_decimal::to<E>(dd)};
}

any to_float4(std::string_view s, evaluator_context& ctx) {
    decimal::Decimal d{};
    auto a = to_decimal_internal(s, ctx, d);
    if(a.error()) {
        return a;
    }
    if(d.isnan() && ! is_valid_nan(s)) {
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    return from_decimal::decimal_to_float4(d, ctx);
}

any to_float8(std::string_view s, evaluator_context& ctx) {
    decimal::Decimal d{};
    auto a = to_decimal_internal(s, ctx, d);
    if(a.error()) {
        return a;
    }
    if(d.isnan() && ! is_valid_nan(s)) {
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    return from_decimal::decimal_to_float8(d, ctx);
}

any to_decimal(
    std::string_view s,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    decimal::Decimal dec{};
    auto a = to_decimal_internal(s, ctx, dec);
    if(a.error()) {
        return a;
    }
    if(dec.isspecial()) {
        auto& e = ctx.add_error(
            {error_kind::format_error,
             "invalid input since conversion generated special value that is not convertible to decimal"}
        );
        e.new_argument() << s;
        e.new_argument() << dec;
        return any{std::in_place_type<error>, error(error_kind::format_error)};
    }
    return handle_ps(dec, ctx, precision, scale);
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

any to_character(
    std::string_view s,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding,
    bool src_padded
) {
    return handle_length(s, ctx, len, add_padding, src_padded);
}

}  // namespace from_character

any cast_from_character(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a,
    bool src_padded // whether src is char column
) {
    using k = takatori::type::type_kind;
    auto txt = a.to<runtime_t<meta::field_type_kind::character>>();
    auto sv = static_cast<std::string_view>(txt);
    auto trimmed = trim_spaces(sv);
    switch(tgt.kind()) {
        case k::boolean: return from_character::to_boolean(trimmed, ctx);
        case k::int1: return from_character::to_int1(trimmed, ctx);
        case k::int2: return from_character::to_int2(trimmed, ctx);
        case k::int4: return from_character::to_int4(trimmed, ctx);
        case k::int8: return from_character::to_int8(trimmed, ctx);
        case k::float4: return from_character::to_float4(trimmed, ctx);
        case k::float8: return from_character::to_float8(trimmed, ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_character::to_decimal(trimmed, ctx, t.precision(), t.scale());
        }
        case k::character: {
            auto& typ = unsafe_downcast<takatori::type::character>(tgt);
            return from_character::to_character(sv, ctx, typ.length(), ! typ.varying(), src_padded);
        }
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

any cast_from_decimal(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    auto dec = a.to<runtime_t<meta::field_type_kind::decimal>>();
    switch(tgt.kind()) {
        case k::boolean: break; // not supported
        case k::int1: return from_decimal::to_int1(dec, ctx);
        case k::int2: return from_decimal::to_int2(dec, ctx);
        case k::int4: return from_decimal::to_int4(dec, ctx);
        case k::int8: return from_decimal::to_int8(dec, ctx);
        case k::float4: return from_decimal::to_float4(dec, ctx);
        case k::float8: return from_decimal::to_float8(dec, ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_decimal::to_decimal(dec, ctx, t.precision(), t.scale());
        }
        case k::character: {
            auto& t = static_cast<takatori::type::character const&>(tgt);  //NOLINT
            return from_decimal::to_character(dec, ctx, t.length(), ! t.varying());
        }
        case k::octet: break; // not supported
        case k::bit: break; // not supported
        case k::date: break; // not supported
        case k::time_of_day: break; // not supported
        case k::time_point: break; // not supported
        case k::datetime_interval: break; // not supported
        case k::array: break; // not supported
        case k::record: break; // not supported
        case k::unknown: break; // not supported
        case k::row_reference: break; // not supported
        case k::row_id: break; // not supported
        case k::declared: break; // not supported
        case k::extension: break; // not supported
    }
    return return_unsupported();
}

any truncate_or_pad_if_needed(
    evaluator_context& ctx,
    std::string_view src,
    std::size_t dlen,
    bool add_padding,
    bool lenient_remove_padding,
    bool& lost_precision
) {
    lost_precision = false;
    auto slen = src.length();
    if(dlen == slen) {
        return any{std::in_place_type<accessor::text>, accessor::text{ctx.resource(), src}};
    }
    if(dlen < src.length()) {
        if(lenient_remove_padding) {
            // check if truncation occurs only for padding or not
            if(! std::all_of(src.begin()+dlen, src.end(), [](auto c) { return c == ' '; })) {
                lost_precision = true;
            }
        } else {
            lost_precision = true;
        }
        return any{
            std::in_place_type<accessor::text>,
            accessor::text{ctx.resource(), std::string_view{src.data(), dlen}}
        };
    }
    // dlen > src.length()
    if(add_padding) {
        std::string tmp(dlen, ' ');
        std::memcpy(tmp.data(), src.data(), src.size());
        return any{std::in_place_type<accessor::text>, accessor::text{ctx.resource(), tmp}};
    }
    return any{std::in_place_type<accessor::text>, accessor::text{ctx.resource(), src}};
}

any handle_length(
    std::string_view src,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding,
    bool lenient_remove_padding
) {
    if(len.has_value()) {
        auto dlen = len.value();
        bool lost_precision = false;
        auto ret = truncate_or_pad_if_needed(ctx, src, dlen, add_padding, lenient_remove_padding, lost_precision);
        if(lost_precision) {
            // inexact operation
            switch(ctx.get_loss_precision_policy()) {
                case loss_precision_policy::ignore: break;
                case loss_precision_policy::floor: return {std::in_place_type<error>, error(error_kind::unsupported)};
                case loss_precision_policy::ceil: return {std::in_place_type<error>, error(error_kind::unsupported)};
                case loss_precision_policy::unknown: return {};  // null to indicate truncation issue
                case loss_precision_policy::warn: {
                    ctx.add_error(
                        {error_kind::lost_precision,
                         string_builder{} << "cast warning src length:" << src.size() << " dest length:" << dlen
                                          << string_builder::to_string}
                    );
                    break;
                }
                case loss_precision_policy::implicit: //fallthrough
                case loss_precision_policy::error: {
                    return {std::in_place_type<error>, error(error_kind::lost_precision)};
                }
            }
        }
        return ret;
    }
    return any{std::in_place_type<accessor::text>, accessor::text{ctx.resource(), src}};
}

template<class T>
any int_to_decimal(
    T src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    decimal::context.clear_status();
    decimal::Decimal d{src};
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        // TODO this can happen?
        auto& err = ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error in converting int to decimal status:" << decimal::context.status()
                              << string_builder::to_string}
        );
        err.new_argument() << src;
        err.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    return handle_ps(d, ctx, precision, scale);
}

template<class T>
any float_to_decimal(
    T src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    if(std::isnan(src)) {
        return any{std::in_place_type<error>, error(error_kind::arithmetic_error)};
    }
    if(std::isinf(src)) {
        return any{std::in_place_type<triple>, (std::signbit(src) ? triple_min : triple_max)};
    }
    auto str = std::to_string(src);
    decimal::context.clear_status();
    decimal::Decimal d{str};
    if((decimal::context.status() & MPD_IEEE_Invalid_operation) != 0) {
        auto& e = ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error in converting float value to decimal status:"
                              << decimal::context.status() << string_builder::to_string}
        );
        e.new_argument() << src;
        e.new_argument() << d;
        return any{std::in_place_type<error>, error(error_kind::undefined)};
    }
    return handle_ps(d, ctx, precision, scale);
}

namespace from_int4 {

any to_character(std::int32_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding) {
    auto res = std::to_string(src);
    return handle_length(res, ctx, len, add_padding, false);
}

any to_int1(std::int32_t src, evaluator_context& ctx) {
    return validate_integer_range_from_integer<std::int8_t, std::int32_t>(src, ctx);
}
any to_int2(std::int32_t src, evaluator_context& ctx) {
    return validate_integer_range_from_integer<std::int16_t, std::int32_t>(src, ctx);
}

any to_int8(std::int32_t src, evaluator_context& ctx) {
    (void) ctx;
    // no validation needed since int4 -> int8 is widening
    return any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(src)};
}

any to_float4(std::int32_t src, evaluator_context& ctx) {
    (void) ctx;
    return any{std::in_place_type<float>, static_cast<float>(src)};
}

any to_float8(std::int32_t src, evaluator_context& ctx) {
    (void) ctx;
    return any{std::in_place_type<double>, static_cast<double>(src)};
}

any to_decimal(
    std::int32_t src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    return int_to_decimal(src, ctx, precision, scale);
}

}  // namespace from_int4

any cast_from_int4(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    switch(tgt.kind()) {
        case k::boolean: break; // not supported
        case k::int1: return from_int4::to_int1(a.to<std::int32_t>(), ctx);
        case k::int2: return from_int4::to_int2(a.to<std::int32_t>(), ctx);
        case k::int4: return a;
        case k::int8: return from_int4::to_int8(a.to<std::int32_t>(), ctx);
        case k::float4: return from_int4::to_float4(a.to<std::int32_t>(), ctx);
        case k::float8: return from_int4::to_float8(a.to<std::int32_t>(), ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_int4::to_decimal(a.to<std::int32_t>(), ctx, t.precision(), t.scale());
        }
        case k::character:
            return from_int4::to_character(
                a.to<std::int32_t>(),
                ctx,
                unsafe_downcast<takatori::type::character const>(tgt).length(),
                ! unsafe_downcast<takatori::type::character const>(tgt).varying()
            );
        case k::octet: break;  // not supported
        case k::bit: break;  // not supported
        case k::date: break;  // not supported
        case k::time_of_day: break;  // not supported
        case k::time_point: break;  // not supported
        case k::datetime_interval: break;  // not supported
        case k::array: break;  // not supported
        case k::record: break;  // not supported
        case k::unknown: break;  // not supported
        case k::row_reference: break;  // not supported
        case k::row_id: break;  // not supported
        case k::declared: break;  // not supported
        case k::extension: break;  // not supported
    }
    return return_unsupported();
}

namespace from_int8 {

any to_character(std::int64_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding) {
    auto res = std::to_string(src);
    return handle_length(res, ctx, len, add_padding, false);
}

any to_int1(std::int64_t src, evaluator_context& ctx) {
    return validate_integer_range_from_integer<std::int8_t, std::int32_t>(src, ctx);
}

any to_int2(std::int64_t src, evaluator_context& ctx) {
    return validate_integer_range_from_integer<std::int16_t, std::int32_t>(src, ctx);
}

any to_int4(std::int64_t src, evaluator_context& ctx) {
    return validate_integer_range_from_integer<std::int32_t>(src, ctx);
}

any to_float4(std::int64_t src, evaluator_context& ctx) {
    (void) ctx;
    return any{std::in_place_type<float>, static_cast<float>(src)};
}

any to_float8(std::int64_t src, evaluator_context& ctx) {
    (void) ctx;
    return any{std::in_place_type<double>, static_cast<double>(src)};
}

any to_decimal(
    std::int64_t src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    return int_to_decimal(src, ctx, precision, scale);
}

}  // namespace from_int8

any cast_from_int8(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    switch(tgt.kind()) {
        case k::boolean: break; // not supported
        case k::int1: return from_int8::to_int1(a.to<std::int64_t>(), ctx);
        case k::int2: return from_int8::to_int2(a.to<std::int64_t>(), ctx);
        case k::int4: return from_int8::to_int4(a.to<std::int64_t>(), ctx);
        case k::int8: return a;
        case k::float4: return from_int8::to_float4(a.to<std::int64_t>(), ctx);
        case k::float8: return from_int8::to_float8(a.to<std::int64_t>(), ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_int8::to_decimal(a.to<std::int64_t>(), ctx, t.precision(), t.scale());
        }
        case k::character:
            return from_int8::to_character(
                a.to<std::int64_t>(),
                ctx,
                unsafe_downcast<takatori::type::character const>(tgt).length(),
                ! unsafe_downcast<takatori::type::character const>(tgt).varying()
            );
        case k::octet: break; // not supported
        case k::bit: break; // not supported
        case k::date: break; // not supported
        case k::time_of_day: break; // not supported
        case k::time_point: break; // not supported
        case k::datetime_interval: break; // not supported
        case k::array: break; // not supported
        case k::record: break; // not supported
        case k::unknown: break; // not supported
        case k::row_reference: break; // not supported
        case k::row_id: break; // not supported
        case k::declared: break; // not supported
        case k::extension: break; // not supported
    }
    return return_unsupported();
}

namespace from_boolean {

any to_character(std::int8_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding) {
    std::string_view res{};
    if(src == 0) {
        res = "false";
    } else {
        res = "true";
    }
    return handle_length(res, ctx, len, add_padding, false);
}

}  // namespace from_boolean

any cast_from_boolean(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    switch(tgt.kind()) {
        case k::boolean: return a;
        case k::int1: break;  // not supported
        case k::int2: break;  // not supported
        case k::int4: break;  // not supported
        case k::int8: break;  // not supported
        case k::float4: break;  // not supported
        case k::float8: break;  // not supported
        case k::decimal: break;  // not supported
        case k::character:
            return from_boolean::to_character(
                a.to<std::int8_t>(),
                ctx,
                unsafe_downcast<takatori::type::character const>(tgt).length(),
                ! unsafe_downcast<takatori::type::character const>(tgt).varying()
            );
        case k::octet: break;  // not supported
        case k::bit: break;  // not supported
        case k::date: break;  // not supported
        case k::time_of_day: break;  // not supported
        case k::time_point: break;  // not supported
        case k::datetime_interval: break;  // not supported
        case k::array: break;  // not supported
        case k::record: break;  // not supported
        case k::unknown: break;  // not supported
        case k::row_reference: break;  // not supported
        case k::row_id: break;  // not supported
        case k::declared: break;  // not supported
        case k::extension: break;  // not supported
    }
    return return_unsupported();
}

namespace from_float4 {

any to_character(float src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding) {
    if(std::isnan(src)) {
        // avoid printing "-NaN"
        return handle_length(string_positive_nan, ctx, len, add_padding, false);
    }
    if(std::isinf(src)) {
        auto str = std::signbit(src) ? string_negative_infinity : string_positive_infinity;
        return handle_length(str, ctx, len, add_padding, false);
    }
    auto res = std::to_string(src);
    return handle_length(res, ctx, len, add_padding, false);
}

any to_int1(float src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float4, kind::int1, std::int8_t, std::int32_t>(src, ctx);
}

any to_int2(float src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float4, kind::int2, std::int16_t, std::int32_t>(src, ctx);
}

any to_int4(float src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float4, kind::int4, std::int32_t, std::int32_t>(src, ctx);
}

any to_int8(float src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float4, kind::int8, std::int64_t, std::int64_t>(src, ctx);
}

any to_float4(float src, evaluator_context& ctx) {
    // this function almost does nothing, but to standardize nan
    (void) ctx;
    if(std::isnan(src)) {
        src = std::numeric_limits<float>::quiet_NaN();
    }
    return any{std::in_place_type<float>, src};
}

any to_float8(float src, evaluator_context& ctx) {
    (void) ctx;
    if(std::isnan(src)) {
        return any{std::in_place_type<double>, std::numeric_limits<double>::quiet_NaN()};
    }
    return any{std::in_place_type<double>, static_cast<double>(src)};
}

any to_decimal(
    float src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    return float_to_decimal(src, ctx, precision, scale);
}

}  // namespace from_float4

any cast_from_float4(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    switch(tgt.kind()) {
        case k::boolean: break; // not supported
        case k::int1: return from_float4::to_int1(a.to<float>(), ctx);
        case k::int2: return from_float4::to_int2(a.to<float>(), ctx);
        case k::int4: return from_float4::to_int4(a.to<float>(), ctx);
        case k::int8: return from_float4::to_int8(a.to<float>(), ctx);
        case k::float4: return from_float4::to_float4(a.to<float>(), ctx);
        case k::float8: return from_float4::to_float8(a.to<float>(), ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_float4::to_decimal(a.to<float>(), ctx, t.precision(), t.scale());
        }
        case k::character:
            return from_float4::to_character(
                a.to<float>(),
                ctx,
                unsafe_downcast<takatori::type::character const>(tgt).length(),
                ! unsafe_downcast<takatori::type::character const>(tgt).varying()
            );
        case k::octet: break; // not supported
        case k::bit: break; // not supported
        case k::date: break; // not supported
        case k::time_of_day: break; // not supported
        case k::time_point: break; // not supported
        case k::datetime_interval: break; // not supported
        case k::array: break; // not supported
        case k::record: break; // not supported
        case k::unknown: break; // not supported
        case k::row_reference: break; // not supported
        case k::row_id: break; // not supported
        case k::declared: break; // not supported
        case k::extension: break; // not supported
    }
    return return_unsupported();
}

namespace from_float8 {

any to_character(double src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding) {
    if(std::isnan(src)) {
        // avoid printing "-NaN"
        return handle_length(string_positive_nan, ctx, len, add_padding, false);
    }
    if(std::isinf(src)) {
        auto str = std::signbit(src) ? string_negative_infinity : string_positive_infinity;
        return handle_length(str, ctx, len, add_padding, false);
    }
    auto res = std::to_string(src);
    return handle_length(res, ctx, len, add_padding, false);
}

any to_int1(double src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float8, kind::int1, std::int8_t, std::int32_t>(src, ctx);
}

any to_int2(double src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float8, kind::int2, std::int16_t, std::int32_t>(src, ctx);
}

any to_int4(double src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float8, kind::int4, std::int32_t, std::int32_t>(src, ctx);
}

any to_int8(double src, evaluator_context& ctx) {
    return validate_integer_range_from_float<kind::float8, kind::int8, std::int64_t, std::int64_t>(src, ctx);
}

any to_float4(double src, evaluator_context& ctx) {
    if(std::isnan(src)) {
        return any{std::in_place_type<float>, std::numeric_limits<float>::quiet_NaN()};
    }
    if(! std::isinf(src)) {
        // check range only when it's not a special values
        static const float maxTgt{std::numeric_limits<float>::max()};
        static const float minTgt{std::numeric_limits<float>::lowest()};
        if(maxTgt < src) {
            return handle_precision_lost(src, maxTgt, ctx);
        }
        if(src < minTgt) {
            return handle_precision_lost(src, minTgt, ctx);
        }

        // treat underflow as zero/-zero
        static const float low_bound{std::numeric_limits<float>::min()};
        if(src < low_bound && -low_bound < src) {
            if(std::signbit(src)) {
                return handle_precision_lost(src, -0.0F, ctx);
            }
            return handle_precision_lost(src, 0.0F, ctx);
        }
    }
    return any{std::in_place_type<float>, static_cast<float>(src)};
}

any to_float8(double src, evaluator_context& ctx) {
    (void) ctx;
    // this function almost does nothing, but to standardize nan
    if(std::isnan(src)) {
        src = std::numeric_limits<double>::quiet_NaN();
    }
    return any{std::in_place_type<double>, src};
}

any to_decimal(
    double src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
) {
    return float_to_decimal(src, ctx, precision, scale);
}

}  // namespace from_float8

any cast_from_float8(evaluator_context& ctx,
    ::takatori::type::data const& tgt,
    any const& a
) {
    using k = takatori::type::type_kind;
    switch(tgt.kind()) {
        case k::boolean: break; // not supported
        case k::int1: return from_float8::to_int1(a.to<double>(), ctx);
        case k::int2: return from_float8::to_int2(a.to<double>(), ctx);
        case k::int4: return from_float8::to_int4(a.to<double>(), ctx);
        case k::int8: return from_float8::to_int8(a.to<double>(), ctx);
        case k::float4: return from_float8::to_float4(a.to<double>(), ctx);
        case k::float8: return from_float8::to_float8(a.to<double>(), ctx);
        case k::decimal: {
            auto& t = static_cast<takatori::type::decimal const&>(tgt);  //NOLINT
            return from_float8::to_decimal(a.to<double>(), ctx, t.precision(), t.scale());
        }
        case k::character:
            return from_float8::to_character(
                a.to<double>(),
                ctx,
                unsafe_downcast<takatori::type::character const>(tgt).length(),
                ! unsafe_downcast<takatori::type::character const>(tgt).varying()
            );
        case k::octet: break; // not supported
        case k::bit: break; // not supported
        case k::date: break; // not supported
        case k::time_of_day: break; // not supported
        case k::time_point: break; // not supported
        case k::datetime_interval: break; // not supported
        case k::array: break; // not supported
        case k::record: break; // not supported
        case k::unknown: break; // not supported
        case k::row_reference: break; // not supported
        case k::row_id: break; // not supported
        case k::declared: break; // not supported
        case k::extension: break; // not supported
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
    // until we officially support boolean and small integers, these types are only available for testing
    if(src.kind() == k::boolean || src.kind() == k::int1 || src.kind() == k::int2 ||
       tgt.kind() == k::boolean || tgt.kind() == k::int1 || tgt.kind() == k::int2) {
        if(auto a = supports_small_integers(); a.error()) {
            return a;
        }
    }
    switch(src.kind()) {
        case k::boolean: return cast_from_boolean(ctx, tgt, a);
        case k::int1: return cast_from_int4(ctx, tgt, a);
        case k::int2: return cast_from_int4(ctx, tgt, a);
        case k::int4: return cast_from_int4(ctx, tgt, a);
        case k::int8: return cast_from_int8(ctx, tgt, a);
        case k::float4: return cast_from_float4(ctx, tgt, a);
        case k::float8: return cast_from_float8(ctx, tgt, a);
        case k::decimal: return cast_from_decimal(ctx, tgt, a);
        case k::character:
            return cast_from_character(ctx, tgt, a, ! unsafe_downcast<takatori::type::character>(src).varying());
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

}  // namespace jogasaki::executor::process::impl::expression::details
