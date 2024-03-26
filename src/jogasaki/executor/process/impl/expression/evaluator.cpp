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
#include "evaluator.h"

#include <cstdlib>
#include <decimal.hh>
#include <exception>
#include <string>
#include <string_view>
#include <utility>
#include <boost/assert.hpp>

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
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/equal_to.h>
#include <jogasaki/executor/less.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>

#include "details/cast_evaluation.h"
#include "details/common.h"
#include "details/decimal_context.h"

namespace jogasaki::executor::process::impl::expression {

using jogasaki::data::any;
using takatori::decimal::triple;
using takatori::util::string_builder;

namespace details {

engine::engine(
    evaluator_context& ctx,
    variable_table& variables,
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

any engine::add_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l,r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return add(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return add(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return add(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return add(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return add(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
}

template <class T, class U = T>
any subtract(T const& l, U const& r) {
    return any{std::in_place_type<T>, l-r};
}

template <>
any subtract<runtime_t<meta::field_type_kind::decimal>>(runtime_t<meta::field_type_kind::decimal> const& l, runtime_t<meta::field_type_kind::decimal> const& r) {
    return any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, static_cast<decimal::Decimal>(l)-static_cast<decimal::Decimal>(r)};
}

any engine::subtract_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return subtract(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return subtract(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return subtract(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return subtract(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return subtract(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
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

any engine::multiply_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return multiply(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return multiply(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return multiply(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return multiply(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return multiply(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
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

any engine::divide_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return divide(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return divide(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return divide(l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return divide(l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return divide(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
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

any engine::remainder_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return remainder(l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return remainder(l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return remainder(l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        default: return return_unsupported();
    }
}

template <class T, class U = T>
any conditional_and(T const& l, U const& r) {
    return any{std::in_place_type<T>, l && r};
}

any engine::conditional_and_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    switch(left.type_index()) {
        case any::index<bool>: return conditional_and(left.to<bool>(), right.to<bool>());
        default: return return_unsupported();
    }
}

template <class T, class U = T>
any conditional_or(T const& l, U const& r) {
    return any{std::in_place_type<T>, l || r};
}

any engine::conditional_or_any(any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    switch(left.type_index()) {
        case any::index<bool>: return conditional_or(left.to<bool>(), right.to<bool>());
        default: return return_unsupported();
    }
}

any engine::operator()(takatori::scalar::binary const& exp) {
    using optype = takatori::scalar::binary_operator;
    auto l = dispatch(*this, exp.left());
    auto r = dispatch(*this, exp.right());
    if (l.error()) return l;
    if (r.error()) return r;
    if (! l) return l;
    if (! r) return r;
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
any create_any(accessor::record_ref ref, value_info const& info) {
    return {std::in_place_type<T>, ref.get_value<E>(info.value_offset())};
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
        case t::int4: return create_any<runtime_t<meta::field_type_kind::int4>>(ref, info);
        case t::int8: return create_any<runtime_t<meta::field_type_kind::int8>>(ref, info);
        case t::float4: return create_any<runtime_t<meta::field_type_kind::float4>>(ref, info);
        case t::float8: return create_any<runtime_t<meta::field_type_kind::float8>>(ref, info);
        case t::boolean: return create_any<bool, std::int8_t>(ref, info);
        case t::character: return create_any<runtime_t<meta::field_type_kind::character>>(ref, info);
        case t::decimal: return create_any<runtime_t<meta::field_type_kind::decimal>>(ref, info);
        case t::date: return create_any<runtime_t<meta::field_type_kind::date>>(ref, info);
        case t::time_of_day: return create_any<runtime_t<meta::field_type_kind::time_of_day>>(ref, info);
        case t::time_point: return create_any<runtime_t<meta::field_type_kind::time_point>>(ref, info);
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
    if(exp.error()) {
        return exp;
    }
    return any{std::in_place_type<bool>, exp.empty()};
}

any engine::operator()(takatori::scalar::unary const& exp) {
    using optype = takatori::scalar::unary::operator_kind_type;
    auto v = dispatch(*this, exp.operand());
    if (! v && exp.operator_kind() != takatori::scalar::unary_operator::is_null) {
        // except for is_null predicate, return null if input is null
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

any engine::compare_any(takatori::scalar::comparison_operator optype, any const& left, any const& right) {
    BOOST_ASSERT(left && right);  //NOLINT
    auto [l, r] = promote_binary_numeric(left, right);
    switch(l.type_index()) {
        case any::index<runtime_t<meta::field_type_kind::int4>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::int4>>(), r.to<runtime_t<meta::field_type_kind::int4>>());
        case any::index<runtime_t<meta::field_type_kind::int8>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::int8>>(), r.to<runtime_t<meta::field_type_kind::int8>>());
        case any::index<runtime_t<meta::field_type_kind::float4>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::float4>>(), r.to<runtime_t<meta::field_type_kind::float4>>());
        case any::index<runtime_t<meta::field_type_kind::float8>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::float8>>(), r.to<runtime_t<meta::field_type_kind::float8>>());
        case any::index<runtime_t<meta::field_type_kind::character>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::character>>(), r.to<runtime_t<meta::field_type_kind::character>>());
        case any::index<runtime_t<meta::field_type_kind::decimal>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::decimal>>(), r.to<runtime_t<meta::field_type_kind::decimal>>());
        case any::index<runtime_t<meta::field_type_kind::date>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::date>>(), r.to<runtime_t<meta::field_type_kind::date>>());
        case any::index<runtime_t<meta::field_type_kind::time_of_day>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::time_of_day>>(), r.to<runtime_t<meta::field_type_kind::time_of_day>>());
        case any::index<runtime_t<meta::field_type_kind::time_point>>: return compare(optype, l.to<runtime_t<meta::field_type_kind::time_point>>(), r.to<runtime_t<meta::field_type_kind::time_point>>());
        default: return return_unsupported();
    }
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

any engine::operator()(takatori::scalar::match const&) {
    return return_unsupported();
}

any engine::operator()(takatori::scalar::conditional const&) {
    return return_unsupported();
}

any engine::operator()(takatori::scalar::coalesce const&) {
    return return_unsupported();
}

any engine::operator()(takatori::scalar::let const&) {
    return return_unsupported();
}

any engine::operator()(takatori::scalar::function_call const&) {
    return return_unsupported();
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
    variable_table& variables,
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
    variable_table& variables,
    memory::lifo_paged_memory_resource* resource
) {
    utils::checkpoint_holder h{resource};
    auto a = eval(ctx, variables, resource);
    if (a.error()) {
        return a;
    }
    return any{std::in_place_type<bool>, a && a.to<bool>()};
}

}  // namespace jogasaki::executor::process::impl::expression
