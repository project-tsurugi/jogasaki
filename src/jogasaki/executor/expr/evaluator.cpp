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
#include "evaluator.h"

#include <cstdlib>
#include <decimal.hh>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/cast_loss_policy.h>
#include <takatori/scalar/dispatch.h>
#include <takatori/scalar/expression.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/type/data.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/datastore/assign_lob_id.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/equal_to.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/less.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/utf8_utils.h>

#include "details/cast_evaluation.h"
#include "details/common.h"
#include "details/decimal_context.h"

namespace jogasaki::executor::expr {

using jogasaki::data::any;
using takatori::decimal::triple;
using takatori::util::string_builder;
using takatori::util::throw_exception;

namespace details {

engine::engine(
    evaluator_context& ctx,
    executor::process::impl::variable_table& variables,
    yugawara::compiled_info const& info,
    executor::process::impl::variable_table const* host_variables,
    engine::memory_resource* resource
) noexcept:
    ctx_(ctx),
    variables_(variables),
    info_(info),
    host_variables_(host_variables),
    resource_(resource)
{}

template <class T, class U = T>
any add(T const& l, U const& r) {
    return any{std::in_place_type<T>, l+r};
}

template <>
any add<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)+static_cast<decimal::Decimal>(r)};
}

triple triple_from_int(std::int64_t arg) {
    decimal::Decimal dec{arg};
    return triple{dec};
}

double triple_to_double(triple arg) {
    decimal::Decimal dec{arg};
    return std::stod(dec.to_eng());
}

any promote_binary_numeric_left(any const& l, any const& r) {
    switch(l.type_index()) {
        case any::index<std::int32_t>: {
            using L = std::int32_t;
            switch(r.type_index()) {
                case any::index<std::int32_t>: return l;
                case any::index<std::int64_t>: return any{std::in_place_type<std::int64_t>, l.to<L>()};
                case any::index<float>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<double>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<triple>: return any{std::in_place_type<triple>, triple_from_int(l.to<L>())};
                default: return return_unsupported();
            }
            break;
        }
        case any::index<std::int64_t>: {
            using L = std::int64_t;
            switch(r.type_index()) {
                case any::index<std::int32_t>: return l;
                case any::index<std::int64_t>: return l;
                case any::index<float>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<double>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<triple>: return any{std::in_place_type<triple>, triple_from_int(l.to<L>())};
                default: return return_unsupported();
            }
            break;
        }
        case any::index<float>: {
            using L = float;
            switch(r.type_index()) {
                case any::index<std::int32_t>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<std::int64_t>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<float>: return any{std::in_place_type<double>, l.to<L>()}; // float v.s. float becomes double
                case any::index<double>: return any{std::in_place_type<double>, l.to<L>()};
                case any::index<triple>: return any{std::in_place_type<double>, l.to<L>()};
                default: return return_unsupported();
            }
            break;
        }
        case any::index<double>: {
            using L = double;
            switch(r.type_index()) {
                case any::index<std::int32_t>: return l;
                case any::index<std::int64_t>: return l;
                case any::index<float>: return l;
                case any::index<double>: return l;
                case any::index<triple>: return any{std::in_place_type<double>, l.to<L>()};
                default: return return_unsupported();
            }
            break;
        }
        case any::index<triple>: {
            using L = triple;
            switch(r.type_index()) {
                case any::index<std::int32_t>: return l;
                case any::index<std::int64_t>: return l;
                case any::index<float>: return any{std::in_place_type<double>, triple_to_double(l.to<L>())};
                case any::index<double>: return any{std::in_place_type<double>, triple_to_double(l.to<L>())};
                case any::index<triple>: return l;
                default: return return_unsupported();
            }
            break;
        }
        case any::index<accessor::text>:  // fall-thru
        case any::index<accessor::binary>:  // fall-thru
        case any::index<takatori::datetime::date>:  // fall-thru
        case any::index<takatori::datetime::time_of_day>:  // fall-thru
        case any::index<takatori::datetime::time_point>:  // fall-thru
        {
            if(l.type_index() != r.type_index()) {
                return return_unsupported();
            }
            return l;
        }
        default: return return_unsupported();
    }
    return return_unsupported();
}

std::pair<any, any> promote_binary_numeric(any const& l, any const& r) {
    return {
        promote_binary_numeric_left(l,r),
        promote_binary_numeric_left(r,l)
    };
}

template <class T, class U = T>
any subtract(T const& l, U const& r) {
    return any{std::in_place_type<T>, l-r};
}

template <>
any subtract<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)-static_cast<decimal::Decimal>(r)};
}

template <class T, class U>
any engine::concat(T const& l, U const& r) {
    return any{std::in_place_type<T>, accessor::text{resource_, l, r}};
}
any engine::concat_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    switch(left.type_index()) {
        case any::index<accessor::text>: return concat(left.to<accessor::text>(), right.to<accessor::text>());
        default: return return_unsupported();
    }
}

template <class T, class U = T>
any multiply(T const& l, U const& r) {
    return any{std::in_place_type<T>, l*r};
}

template <>
any multiply<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)*static_cast<decimal::Decimal>(r)};
}

template <class T, class U = T>
any divide(T const& l, U const& r) {
    if (r == 0) {
        return any{std::in_place_type<class error>, error_kind::arithmetic_error};
    }
    return any{std::in_place_type<T>, l/r};
}

template <>
any divide<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    // TODO check context status
    if (r == 0) {
        return any{std::in_place_type<class error>, error_kind::arithmetic_error};
    }
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)/static_cast<decimal::Decimal>(r)};
}

template <class T, class U = T>
any remainder(T const& l, U const& r) {
    if (r == 0) {
        return any{std::in_place_type<class error>, error_kind::arithmetic_error};
    }
    return any{std::in_place_type<T>, l%r};
}

template <>
any remainder<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    // TODO check context status
    if (r == 0) {
        return any{std::in_place_type<class error>, error_kind::arithmetic_error};
    }
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)%static_cast<decimal::Decimal>(r)};
}



any engine::conditional_and_any(any const& left, any const& right) {
    // first, check if either of operands is false because then
    // the result is false regardless of the other operand being true or null
    if(left) {
        if(left.type_index() != any::index<bool>) {
            return return_unsupported();
        }
        if(! left.to<bool>()) {
            return left;
        }
    }
    if(right) {
        if(right.type_index() != any::index<bool>) {
            return return_unsupported();
        }
        if(! right.to<bool>()) {
            return right;
        }
    }
    // left/right are either true or null
    if(! left || ! right) {
        return {};
    }
    return any{std::in_place_type<bool>, true};
}

any engine::conditional_or_any(any const& left, any const& right) {
    // first, check if either of operands is true because then
    // the result is true regardless of the other operand being false or null
    if(left) {
        if(left.type_index() != any::index<bool>) {
            return return_unsupported();
        }
        if(left.to<bool>()) {
            return left;
        }
    }
    if(right) {
        if(right.type_index() != any::index<bool>) {
            return return_unsupported();
        }
        if(right.to<bool>()) {
            return right;
        }
    }
    // left/right are either false or null
    if(! left || ! right) {
        return {};
    }
    return any{std::in_place_type<bool>, false};
}

any engine::operator()(takatori::scalar::binary const& exp) {
    using optype = takatori::scalar::binary_operator;
    auto l = dispatch(*this, exp.left());
    auto r = dispatch(*this, exp.right());
    if (l.error()) return l;
    if (r.error()) return r;
    if (exp.operator_kind() != optype::conditional_and && exp.operator_kind() != optype::conditional_or) {
        // except AND/OR, if either of operands is null, the result is null
        if (! l) return l;
        if (! r) return r;
    }
    switch(exp.operator_kind()) {
        case optype::add: return add_any(l, r);
        case optype::concat: return concat_any(l, r);
        case optype::subtract: return subtract_any(l, r);
        case optype::divide: return divide_any(l, r);
        case optype::multiply: return multiply_any(l, r);
        case optype::remainder: return remainder_any(l, r);
        case optype::conditional_and: return conditional_and_any(l, r);
        case optype::conditional_or: return conditional_or_any(l, r);
        default: return return_unsupported();
    }
}

template <typename T, typename E = T>
std::enable_if_t<! (std::is_same_v<T, lob::clob_reference> || std::is_same_v<T, lob::blob_reference>), any>
create_any(accessor::record_ref ref, executor::process::impl::value_info const& info) {
    return {std::in_place_type<T>, ref.get_value<E>(info.value_offset())};
}

template <typename T, typename E = T>
std::enable_if_t<std::is_same_v<T, lob::clob_reference> || std::is_same_v<T, lob::blob_reference>, any>
create_any(accessor::record_ref ref, executor::process::impl::value_info const& info, evaluator_context& ctx) {
    // evaluating lob value resolves `provided` reference by registering to datastore

    auto& var = ref.get_reference<E>(info.value_offset());
    if (var.kind() != lob::lob_reference_kind::provided) {
        return {std::in_place_type<T>, var};
    }
    lob::lob_id_type id{};
    std::shared_ptr<jogasaki::error::error_info> error{};
    if (auto st = datastore::assign_lob_id(var, ctx.transaction(), id, error); st != status::ok) {
        ctx.set_error_info(std::move(error));
        return any{std::in_place_type<class error>, error_kind::error_info_provided};
    }
    return {std::in_place_type<T>, T{id, lob::lob_data_provider::datastore}};
}

any engine::operator()(takatori::scalar::variable_reference const& exp) {
    auto b = variables_ && variables_.info().exists(exp.variable());
    auto h = host_variables_ != nullptr && *host_variables_ && host_variables_->info().exists(exp.variable());
    BOOST_ASSERT(b || h); //NOLINT
    (void)h;
    auto& info = b ? variables_.info().at(exp.variable()) : host_variables_->info().at(exp.variable());
    auto ref = b ? variables_.store().ref() : host_variables_->store().ref();
    auto is_null = ref.is_null(info.nullity_offset());
    if (is_null) {
        return {};
    }
    using t = takatori::type::type_kind;
    auto& type = info_.type_of(exp);
    switch(type.kind()) {
        case t::boolean: return create_any<bool, std::int8_t>(ref, info);
        case t::int1: return create_any<runtime_t<meta::field_type_kind::int1>>(ref, info);
        case t::int2: return create_any<runtime_t<meta::field_type_kind::int2>>(ref, info);
        case t::int4: return create_any<runtime_t<meta::field_type_kind::int4>>(ref, info);
        case t::int8: return create_any<runtime_t<meta::field_type_kind::int8>>(ref, info);
        case t::float4: return create_any<runtime_t<meta::field_type_kind::float4>>(ref, info);
        case t::float8: return create_any<runtime_t<meta::field_type_kind::float8>>(ref, info);
        case t::decimal: return create_any<runtime_t<meta::field_type_kind::decimal>>(ref, info);
        case t::character: return create_any<runtime_t<meta::field_type_kind::character>>(ref, info);
        case t::octet: return create_any<runtime_t<meta::field_type_kind::octet>>(ref, info);
        case t::date: return create_any<runtime_t<meta::field_type_kind::date>>(ref, info);
        case t::time_of_day: return create_any<runtime_t<meta::field_type_kind::time_of_day>>(ref, info);
        case t::time_point: return create_any<runtime_t<meta::field_type_kind::time_point>>(ref, info);
        case t::blob: return create_any<runtime_t<meta::field_type_kind::blob>>(ref, info, ctx_);
        case t::clob: return create_any<runtime_t<meta::field_type_kind::clob>>(ref, info, ctx_);
        default: return return_unsupported();
    }
}

template <class T>
any sign_inversion(T const& l) {
    return any{std::in_place_type<T>, -l};
}

template <>
any sign_inversion<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l) {
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, -static_cast<decimal::Decimal>(l)};
}

any engine::sign_inversion_any(any const& exp) {
    BOOST_ASSERT(exp);  //NOLINT
    switch(exp.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return sign_inversion(exp.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return sign_inversion(exp.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return sign_inversion(exp.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return sign_inversion(exp.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return sign_inversion(exp.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
}

template <class T>
any conditional_not(T const& e) {
    return any{std::in_place_type<T>, ! e};
}

any engine::conditional_not_any(any const& exp) {
    BOOST_ASSERT(exp);  //NOLINT
    switch(exp.type_index()) {
        case any::index<bool>: return conditional_not(exp.to<bool>());
        default: return return_unsupported();
    }
}

template <class T>
any length(T const& e) {
    return any{std::in_place_type<std::int32_t>, static_cast<std::string_view>(e).size()};
}

any engine::length_any(any const& exp) {
    BOOST_ASSERT(exp);  //NOLINT
    switch(exp.type_index()) {
        case any::index<accessor::text>: return length(exp.to<accessor::text>());
        default: return return_unsupported();
    }
}

any engine::is_null(any const& exp) {
    return any{std::in_place_type<bool>, exp.empty()};
}

any engine::is_true(any const& exp) {
    // exp is null or bool
    if(exp.empty()) {
        return any{std::in_place_type<bool>, false};
    }
    return exp;
}

any engine::is_false(any const& exp) {
    // exp is null or bool
    if(exp.empty()) {
        return any{std::in_place_type<bool>, false};
    }
    return conditional_not(exp.to<bool>());
}

any engine::is_unknown(any const& exp) {
    return is_null(exp);
}

any engine::operator()(takatori::scalar::unary const& exp) {
    using optype = takatori::scalar::unary::operator_kind_type;
    auto v = dispatch(*this, exp.operand());
    if(v.error()) {
        return v;
    }
    if(! v && exp.operator_kind() != takatori::scalar::unary_operator::is_null &&
       exp.operator_kind() != takatori::scalar::unary_operator::is_true &&
       exp.operator_kind() != takatori::scalar::unary_operator::is_false &&
       exp.operator_kind() != takatori::scalar::unary_operator::is_unknown) {
        // except for is_xxx predicate, return null if input is null
        return v;
    }
    switch(exp.operator_kind()) {
        case optype::plus:
            // no-op - pass current value upward
            return v;
        case optype::sign_inversion:
            return sign_inversion_any(v);
        case optype::conditional_not:
            return conditional_not_any(v);
        case optype::length:
            return length_any(v);
        case optype::is_null:
            return is_null(v);
        case optype::is_true:
            return is_true(v);
        case optype::is_false:
            return is_false(v);
        case optype::is_unknown:
            return is_unknown(v);
        default: return return_unsupported();
    }
}

template <class T, class U = T>
any compare(takatori::scalar::comparison_operator op, T const& l, U const& r) {
    using optype = takatori::scalar::comparison_operator;
    bool result = false;
    switch(op) {
        case optype::equal: result = equal_to(l, r); break;
        case optype::not_equal: result = ! equal_to(l, r); break;
        case optype::greater: result = less(r, l); break;
        case optype::greater_equal: result = ! less(l, r); break;
        case optype::less: result = less(l, r); break;
        case optype::less_equal: result = ! less(r, l); break;
        default: return return_unsupported();
    }
    return any{std::in_place_type<bool>, result};
}

any engine::operator()(takatori::scalar::immediate const& exp) {
    auto& type = info_.type_of(exp);
    return utils::as_any(exp.value(), type, resource_);
}

loss_precision_policy from(takatori::scalar::cast_loss_policy t) {
    switch(t) {
        case takatori::scalar::cast_loss_policy::ignore: return loss_precision_policy::ignore;
        case takatori::scalar::cast_loss_policy::floor: return loss_precision_policy::floor;
        case takatori::scalar::cast_loss_policy::ceil: return loss_precision_policy::ceil;
        case takatori::scalar::cast_loss_policy::unknown: return loss_precision_policy::unknown;
        case takatori::scalar::cast_loss_policy::warn: return loss_precision_policy::warn;
        case takatori::scalar::cast_loss_policy::error: return loss_precision_policy::error;
    }
    std::abort();
}

any engine::operator()(takatori::scalar::cast const& exp) {
    auto v = dispatch(*this, exp.operand());
    if (! v) return v;
    auto& src_type = info_.type_of(exp.operand());
    auto& tgt_type = exp.type();

    auto original = ctx_.get_loss_precision_policy();
    ctx_.set_loss_precision_policy(from(exp.loss_policy()));
    auto ret = details::conduct_cast(ctx_, src_type, tgt_type, v);
    ctx_.set_loss_precision_policy(original);
    return ret;
}

any engine::operator()(takatori::scalar::compare const& exp) {
    auto l = dispatch(*this, exp.left());
    auto r = dispatch(*this, exp.right());
    if (l.error()) return l;
    if (r.error()) return r;
    if (! l) return l;
    if (! r) return r;
    return compare_any(exp.operator_kind(), l, r);
}

struct token {
    enum class kind {
        /// A literal string
        literal,
        /// A wildcard (`%`) that matches zero or more characters (greedy).
        wildcard_any,
        /// A wildcard (`_`) that matches exactly one character.
        wildcard_one
    };
    token() = default;
    token(kind k, std::string v) : kind_(k), value_(std::move(v)) {}
    [[nodiscard]] kind get_kind() const { return kind_; }
    [[nodiscard]] std::string const& get_value() const { return value_; }

  private:
    kind kind_{kind::literal};
    /// The literal value to match (used only if kind == literal).
    std::string value_{};
};

bool is_single_utf8_character(std::string_view view) noexcept {
    const std::size_t char_size = utils::get_byte(utils::detect_next_encoding(view, 0));
    return (char_size != 0 && char_size == view.size());
}

// Check if the escape sequence at the end of the pattern is unescaped
bool has_unescaped_trailing_escape(std::string_view pattern, std::string_view escape) {
    // escape must be non-empty and shorter than pattern
    if (escape.empty() || pattern.size() < escape.size()) { return false; }
    // Check if the pattern ends with the escape sequence
    if (!std::equal(escape.begin(), escape.end(), pattern.end() - escape.size())) { return false; }

    std::size_t count           = 0;
    std::size_t last_escape_pos = pattern.size() - escape.size();
    // Count how many times the escape sequence appears at the end of the pattern
    while (last_escape_pos >= escape.size()) {
        last_escape_pos -= escape.size();
        if (std::equal(escape.begin(), escape.end(), pattern.begin() + last_escape_pos)) {
            ++count;
        } else {
            break;
        }
    }
    // If the count of escape sequences is odd, it means the last escape is unescaped
    // If the count is even, it means the last escape is escaped
    return (count % 2 == 0);
}
inline bool is_escape_sequence(
    std::string_view pattern, size_t i, std::string_view escape) noexcept {
    return !escape.empty() && i + escape.size() <= pattern.size() &&
           pattern.substr(i, escape.size()) == escape;
}
std::vector<token> tokenize_like_pattern(std::string_view pattern, std::string_view escape) {
    std::vector<token> tokens;
    // Reserve space for tokens to avoid multiple reallocations
    tokens.reserve(pattern.size());
    std::string buffer;
    size_t i{0};
    while (i < pattern.size()) {
        // match escape
        if (is_escape_sequence(pattern, i, escape)) {
            i += escape.size();
            size_t char_size = utils::get_byte(utils::detect_next_encoding(pattern, i));
            for (size_t j = 0; j < char_size && i < pattern.size(); ++j) {
                buffer += pattern[i++];
            }
        } else if (pattern[i] == '%') {
            if (!buffer.empty()) {
                tokens.emplace_back(token::kind::literal, std::move(buffer));
                buffer.clear();
            }
            // Avoid adding multiple consecutive wildcard_any tokens
            if (tokens.empty() || tokens.back().get_kind() != token::kind::wildcard_any) {
                tokens.emplace_back(token::kind::wildcard_any, "");
            }
            ++i;
        } else if (pattern[i] == '_') {
            if (!buffer.empty()) {
                tokens.emplace_back(token::kind::literal, std::move(buffer));
                buffer.clear();
            }
            tokens.emplace_back(token::kind::wildcard_one, "");
            ++i;
        } else {
            size_t char_size = utils::get_byte(utils::detect_next_encoding(pattern, i));
            for (size_t j = 0; j < char_size && i < pattern.size(); ++j) {
                buffer += pattern[i++];
            }
        }
    }
    if (!buffer.empty()) { tokens.emplace_back(token::kind::literal, std::move(buffer)); }
    return tokens;
}
bool starts_with(std::string_view str, std::string_view prefix) {
    return str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix;
}

[[nodiscard]] bool match_literal_token(std::string_view input, std::size_t& input_index,
    token const& tok, std::size_t& pattern_index, std::size_t& backtrack_input_index,
    std::size_t& backtrack_pattern_index) {
    if (starts_with(input.substr(input_index), tok.get_value())) {
        input_index += tok.get_value().size();
        ++pattern_index;
        return true;
    }
    if (backtrack_pattern_index != std::string::npos) {
        ++backtrack_input_index;
        input_index   = backtrack_input_index;
        pattern_index = backtrack_pattern_index + 1;
        return true;
    }
    return false;
}
[[nodiscard]] bool match_wildcard_one_token(std::string_view input, std::size_t& input_index,
    std::size_t& pattern_index, std::size_t& backtrack_input_index,
    std::size_t& backtrack_pattern_index) {
    if (input_index < input.size()) {
        std::size_t char_size = utils::get_byte(utils::detect_next_encoding(input, input_index));
        if (input_index + char_size <= input.size()) {
            input_index += char_size;
            ++pattern_index;
            return true;
        }
        return false;
    }
    if (backtrack_pattern_index != std::string::npos) {
        ++backtrack_input_index;
        input_index   = backtrack_input_index;
        pattern_index = backtrack_pattern_index + 1;
        return true;
    }
    return false;
}
void match_wildcard_any_token(std::size_t& pattern_index, std::size_t& input_index,
    std::size_t& backtrack_pattern_index, std::size_t& backtrack_input_index) {
    backtrack_pattern_index = pattern_index;
    backtrack_input_index   = input_index;
    ++pattern_index;
}
/**
 * @brief Match the input string with the given LIKE-style pattern.
 *
 * This function checks whether the given input string matches a pattern
 * expressed as a sequence of tokens.
 * The pattern can include:
 *
 *   - literal tokens: must match exactly.
 *   - wildcard_one (`_`): matches exactly one character (UTF-8 aware).
 *   - wildcard_any (`%`): matches zero or more characters (greedy).
 *
 * Matching is **greedy with backtracking**, meaning:
 *
 *   - When encountering a `wildcard_any` token (`%`), the algorithm
 *     initially assumes it matches zero characters (non-consuming match).
 *   - If later matching fails, the algorithm *backtracks* to this
 *     `wildcard_any` position and attempts to consume one more character,
 *     retrying the rest of the pattern.
 *   - This continues until a successful match is found or all possibilities
 *     are exhausted.
 *
 *   Example:
 *  - Input:   "abcde"
 *  - Pattern: "a%de"
 *
 *  a -> matches 'a' (literal)
 *       input_index += tok.value.size()(1)
 *       ++pattern_index(1)
 *  % -> (wildcard_any)
 *       backtrack_pattern_index = pattern_index(1);
 *       backtrack_input_index   = input_index(1);
 *       ++pattern_i(2);
 *  d -> not match  "bcde"
 *        ++backtrack_input_index(2);
 *        input_index   = backtrack_input_index(1)
 *        pattern_index = backtrack_pattern_index(1) + 1;
 *  d -> not match  "cde"
 *        ++backtrack_input_index(3);
 *        input_index   = backtrack_input_index(3)
 *        pattern_index = backtrack_pattern_index(1) + 1;
 *  d  -> match  "de"
 *        input_index += tok.value.size()(4);
 *        ++pattern_index(3);
 *  e  -> match  "e"
 *        input_index += tok.value.size()(5)
 *        ++pattern_index(4)
 *  input_index(5) == input.size(5) -> true
 *
 *
 * @param input   The UTF-8 encoded input string to be matched.
 * @param pattern A sequence of token objects representing the pattern.
 * @return true if the input matches the pattern; false otherwise.
 */
bool match_like_pattern(std::string_view input, std::vector<token> const& pattern) {
    std::size_t pattern_index = 0;
    std::size_t input_index   = 0;

    std::size_t backtrack_pattern_index = std::string::npos;
    std::size_t backtrack_input_index   = std::string::npos;

    while (input_index <= input.size()) {
        if (pattern_index < pattern.size()) {
            token const& tok = pattern[pattern_index];
            switch (tok.get_kind()) {
                case token::kind::literal:
                    if (!match_literal_token(input, input_index, tok, pattern_index,
                            backtrack_input_index, backtrack_pattern_index)) {
                        return false;
                    }
                    break;
                case token::kind::wildcard_one:
                    if (!match_wildcard_one_token(input, input_index, pattern_index,
                            backtrack_input_index, backtrack_pattern_index)) {
                        return false;
                    }
                    break;
                case token::kind::wildcard_any:
                    match_wildcard_any_token(
                        pattern_index, input_index, backtrack_pattern_index, backtrack_input_index);
                    break;
            }
            // Digest all patterns
        } else {
            if (input_index == input.size()) { return true; }
            // input 'a' and pattern '%' reaches here
            if (backtrack_pattern_index != std::string::npos) {
                ++backtrack_input_index;
                input_index   = backtrack_input_index;
                pattern_index = backtrack_pattern_index + 1;
                // input 'abcde' and pattern 'abc' reaches here
            } else {
                // not match and no backtrace
                return false;
            }
        }
    }
    // input 'abc' and pattern 'abc%%%' reaches here
    while (pattern_index < pattern.size() &&
           pattern[pattern_index].get_kind() == token::kind::wildcard_any) {
        ++pattern_index;
    }
    return pattern_index == pattern.size();
}

any engine::operator()(takatori::scalar::match const& match) {
    auto escape_val  = dispatch(*this, match.escape());
    auto input_val   = dispatch(*this, match.input());
    auto pattern_val = dispatch(*this, match.pattern());
    if (escape_val.error()) return escape_val;
    if (input_val.error()) return input_val;
    if (pattern_val.error()) return pattern_val;
    if (escape_val.empty() || input_val.empty() || pattern_val.empty()) return {};
    constexpr auto char_type = any::index<runtime_t<meta::field_type_kind::character>>;
    if (escape_val.type_index() != char_type || input_val.type_index() != char_type ||
        pattern_val.type_index() != char_type) {
        return return_unsupported();
    }
    auto kind = match.operator_kind();
    if (kind == takatori::scalar::match::operator_kind_type::like) {
        auto escape_text = escape_val.to<runtime_t<kind::character>>();
        auto escape_str  = static_cast<std::string_view>(escape_text);
        if (!escape_str.empty() && !is_single_utf8_character(escape_str)) {
            return return_invalid_input_value();
        }
        auto pattern_text = pattern_val.to<runtime_t<kind::character>>();
        auto input_text   = input_val.to<runtime_t<kind::character>>();
        if (pattern_text.empty()) {
            if (input_text.empty()) { return any{std::in_place_type<bool>, true}; }
            return any{std::in_place_type<bool>, false};
        }
        auto pattern_str = static_cast<std::string_view>(pattern_text);
        if (!utils::is_valid_utf8(pattern_str)) { return {}; }
        if (escape_str == pattern_str) { return return_invalid_input_value(); }
        if (has_unescaped_trailing_escape(pattern_str, escape_str)) {
            return return_invalid_input_value();
        }
        std::vector<token> token = tokenize_like_pattern(pattern_str, escape_str);
        auto input_str           = static_cast<std::string_view>(input_text);
        if (!utils::is_valid_utf8(input_str)) { return {}; }
        auto res = match_like_pattern(input_str, token);
        return any{std::in_place_type<bool>, res};
    }
    // kind == takatori::scalar::match::operator_kind_type::similar
    return return_unsupported();
}

any convert_return_type_if_needed(
    takatori::scalar::expression const& e,
    yugawara::compiled_info const& info,
    takatori::type::data const& dest_type,
    any const& v,
    engine::memory_resource* resource
) {
    auto& src_type = info.type_of(e);
    if(conv::to_require_conversion(src_type, dest_type)) {
        data::any converted{};
        if(auto res = conv::conduct_unifying_conversion(src_type, dest_type, v, converted, resource);
            res != status::ok) {
            VLOG_LP(log_error) << "unexpected error occurred during conversion";
        }
        return converted;
    }
    return v;
}

any engine::operator()(takatori::scalar::conditional const& arg) {
    auto& dest_type = info_.type_of(arg);
    for(auto const& e : arg.alternatives()) {
        auto b = dispatch(*this, e.condition());
        if(b.error()) {
            return b;
        }
        if(b.empty() || ! b.to<bool>()) {
            continue;
        }
        auto v = dispatch(*this, e.body());
        return convert_return_type_if_needed(e.body(), info_, dest_type, v, resource_);
    }
    if(arg.default_expression().has_value()) {
        auto v = dispatch(*this, arg.default_expression().value());
        return convert_return_type_if_needed(arg.default_expression().value(), info_, dest_type, v, resource_);
    }
    // no matching condition, no default clause - return null
    return {};
}

any engine::operator()(takatori::scalar::coalesce const& arg) {
    auto& dest_type = info_.type_of(arg);
    for(auto const& e : arg.alternatives()) {
        auto v = dispatch(*this, e);
        if(v.error()) {
            return v;
        }
        if(v.empty()) {
            continue;
        }
        return convert_return_type_if_needed(e, info_, dest_type, v, resource_);
    }
    return {};
}

any engine::operator()(takatori::scalar::let const&) {
    return return_unsupported();
}

any engine::operator()(takatori::scalar::function_call const& arg) {
    std::vector<any> inputs{};
    inputs.reserve(arg.arguments().size());
    for(auto const& e : arg.arguments()) {
        auto v = dispatch(*this, e);
        if(v.error()) {
            return v;
        }
        inputs.emplace_back(v);
    }
    if(auto f = yugawara::binding::extract_if<yugawara::function::declaration>(arg.function()); f.has_value()) {
        if(auto info = global::scalar_function_repository().find(f->definition_id()); info != nullptr) {
            if(! ctx_.transaction()) {
                throw_exception(std::logic_error{""});
            }
            return info->function_body()(ctx_, inputs);
        }
    }
    throw_exception(std::logic_error{""});
}

any engine::operator()(takatori::scalar::extension const&) {
    return return_unsupported();
}

evaluator_context& engine::context() noexcept {
    return ctx_;
}

}  // namespace details

evaluator::evaluator(
    takatori::scalar::expression const& expression,
    yugawara::compiled_info const& info,
    executor::process::impl::variable_table const* host_variables
) noexcept:
    expression_(std::addressof(expression)),
    info_(std::addressof(info)),
    host_variables_(host_variables)
{}

any evaluator::operator()(
    evaluator_context& ctx,
    executor::process::impl::variable_table& variables,
    evaluator::memory_resource* resource
) const {
    try {
        details::ensure_decimal_context();
        details::engine e{ctx, variables, *info_, host_variables_, resource};
        return takatori::scalar::dispatch(e, *expression_);
    } catch (std::exception const& e) {
        // catch unexpected error during mpdecimal operation such as MallocError or ValueError
        // this should not happen normally, even but in that case we stop the evaluation simply stops
        ctx.add_error(
            {error_kind::undefined,
             string_builder{} << "unexpected error occurred during expression evaluation:" << e.what()
                              << string_builder::to_string}
        );
        return any{std::in_place_type<class error>, error_kind::undefined};
    }
}

any evaluate_bool(
    evaluator_context& ctx,
    evaluator& eval,
    executor::process::impl::variable_table& variables,
    memory::lifo_paged_memory_resource* resource
) {
    utils::checkpoint_holder h{resource};
    auto a = eval(ctx, variables, resource);
    if (a.error()) {
        return a;
    }
    return any{std::in_place_type<bool>, a && a.to<bool>()};
}

any remainder_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right); // NOLINT
    auto [l, r] = details::promote_binary_numeric(left, right);
    switch (l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>:
            return details::remainder(l.to<runtime_t<meta::field_type_kind::int4>>(),
                r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>:
            return details::remainder(l.to<runtime_t<meta::field_type_kind::int8>>(),
                r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>:
            return details::remainder(l.to<runtime_t<meta::field_type_kind::decimal>>(),
                r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return details::return_unsupported();
    }
}
any add_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l,r] = details::promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return details::add(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return details::add(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return details::add(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return details::add(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return details::add(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return details::return_unsupported();
    }
}

any subtract_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = details::promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return details::subtract(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return details::subtract(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return details::subtract(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return details::subtract(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return details::subtract(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return details::return_unsupported();
    }
}

any compare_any(takatori::scalar::comparison_operator optype, any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = details::promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        case any::index<runtime_t<meta::field_type_kind::character>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::character>>(), r.to<runtime_t<meta::field_type_kind::character>>());
        case any::index<runtime_t<meta::field_type_kind::octet>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::octet>>(), r.to<runtime_t<meta::field_type_kind::octet>>());
        case any::index<runtime_t<meta::field_type_kind::date>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::date>>(), r.to<runtime_t<meta::field_type_kind::date>>());
        case any::index<runtime_t<meta::field_type_kind::time_of_day>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::time_of_day>>(), r.to<runtime_t<meta::field_type_kind::time_of_day>>());
        case any::index<runtime_t<meta::field_type_kind::time_point>>: return details::compare(optype, l.to<runtime_t<meta::field_type_kind::time_point>>(), r.to<runtime_t<meta::field_type_kind::time_point>>());
        default: return details::return_unsupported();
    }
}
any multiply_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = details::promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return details::multiply(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return details::multiply(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return details::multiply(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return details::multiply(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return details::multiply(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return details::return_unsupported();
    }
}
any divide_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = details::promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return details::divide(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return details::divide(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return details::divide(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return details::divide(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return details::divide(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return details::return_unsupported();
    }
}

}  // namespace jogasaki::executor::expr
