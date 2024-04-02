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
#include <cstdint>
#include <decimal.hh>
#include <functional>
#include <optional>
#include <string_view>

#include <takatori/decimal/triple.h>
#include <takatori/type/data.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>

namespace jogasaki::executor::process::impl::expression::details {

using any = jogasaki::data::any;

/**
 * @brief cast function from src to target type
 * @param ctx evaluator context holding resource, warnings
 * @param src source type
 * @param tgt target type
 * @param a value to be casted
 * @return casted value or error if any
 */
any conduct_cast(
    evaluator_context& ctx,
    ::takatori::type::data const& src,
    ::takatori::type::data const& tgt,
    any const& a
);

/// following functions are private, left for testing

namespace from_boolean {

any to_character(std::int8_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding);

}  // namespace from_boolean

namespace from_int4 {

any to_int1(std::int32_t src, evaluator_context& ctx);
any to_int2(std::int32_t src, evaluator_context& ctx);
any to_int8(std::int32_t src, evaluator_context& ctx);
any to_float4(std::int32_t src, evaluator_context& ctx);
any to_float8(std::int32_t src, evaluator_context& ctx);
any to_character(std::int32_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding);
any to_decimal(
    std::int32_t src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace from_int4

namespace from_int8 {

any to_int1(std::int64_t src, evaluator_context& ctx);
any to_int2(std::int64_t src, evaluator_context& ctx);
any to_int4(std::int64_t src, evaluator_context& ctx);
any to_float4(std::int64_t src, evaluator_context& ctx);
any to_float8(std::int64_t src, evaluator_context& ctx);
any to_character(std::int64_t src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding);
any to_decimal(
    std::int64_t src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace from_int8

namespace from_float4 {

any to_int1(float src, evaluator_context& ctx);
any to_int2(float src, evaluator_context& ctx);
any to_int4(float src, evaluator_context& ctx);
any to_int8(float src, evaluator_context& ctx);
any to_float8(float src, evaluator_context& ctx);
any to_character(float src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding);
any to_decimal(
    float src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace from_float4

namespace from_float8 {

any to_int1(double src, evaluator_context& ctx);
any to_int2(double src, evaluator_context& ctx);
any to_int4(double src, evaluator_context& ctx);
any to_int8(double src, evaluator_context& ctx);
any to_float4(double src, evaluator_context& ctx);
any to_character(double src, evaluator_context& ctx, std::optional<std::size_t> len, bool add_padding);
any to_decimal(
    double src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace from_float8

namespace from_character {

any to_boolean(std::string_view s, evaluator_context& ctx);
any to_int1(std::string_view s, evaluator_context& ctx);
any to_int2(std::string_view s, evaluator_context& ctx);
any to_int4(std::string_view s, evaluator_context& ctx);
any to_int8(std::string_view s, evaluator_context& ctx);
any to_float4(std::string_view s, evaluator_context& ctx);
any to_float8(std::string_view s, evaluator_context& ctx);
any to_character(
    std::string_view s,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding,
    bool src_padded
);
any to_decimal(
    std::string_view s,
    evaluator_context& ctx,
    std::optional<std::size_t> precision = {},
    std::optional<std::size_t> scale = {}
);

}  // namespace from_character

namespace from_decimal {

any to_int1(takatori::decimal::triple src, evaluator_context& ctx);
any to_int2(takatori::decimal::triple src, evaluator_context& ctx);
any to_int4(takatori::decimal::triple src, evaluator_context& ctx);
any to_int8(takatori::decimal::triple src, evaluator_context& ctx);
any to_float4(takatori::decimal::triple src, evaluator_context& ctx);
any to_float8(takatori::decimal::triple src, evaluator_context& ctx);
any to_character(
    takatori::decimal::triple dec,
    evaluator_context& ctx,
    std::optional<std::size_t> len,
    bool add_padding
);
any to_decimal(
    takatori::decimal::triple dec,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace from_decimal

any truncate_or_pad_if_needed(
    evaluator_context& ctx,
    std::string_view src,
    std::size_t dlen,
    bool add_padding,
    bool lenient_remove_padding,
    bool& lost_precision);

any as_triple(
    decimal::Decimal const& d,
    evaluator_context& ctx
);

/**
 * @brief validate, modify and return triple that fits into sql decimal(p,s)
 * @param src decimal to be handled this must be finite value (i.e. not NaN or Inf/-Inf) otherwise the behavior is undefined
 * @param ctx the evaluator context
 * @param precision the precision of the target decimal
 * @param scale the scale of the target decimal
 * @return data::any with reduced (i.e. no trailing zeros in coefficient) triple that fits with the given precision and scale
 * @return data::any with error_kind::unsupported if the given `scale` is nullopt while `precision` is not
 * @note this is private functionality, accessible from outside just for testing
 *
*/
any handle_ps(
    decimal::Decimal const& src,
    evaluator_context& ctx,
    std::optional<std::size_t> precision,
    std::optional<std::size_t> scale
);

}  // namespace jogasaki::executor::process::impl::expression::details
