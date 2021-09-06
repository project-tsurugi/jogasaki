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
#include "evaluator.h"

#include <cstddef>
#include <functional>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/walk.h>
#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/expression/any.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/variant.h>
#include <jogasaki/accessor/text.h>

namespace jogasaki::executor::process::impl::expression {

using takatori::util::fail;

namespace details {

engine::engine(
    variable_table& variables,
    yugawara::compiled_info const& info,
    executor::process::impl::variable_table const* host_variables,
    engine::memory_resource* resource
) noexcept:
    variables_(variables),
    info_(info),
    host_variables_(host_variables),
    resource_(resource)
{}

template <class T>
static constexpr std::size_t index = alternative_index<T, any::base_type>();

template <class T, class U = T>
any add(T const& l, U const& r) {
    return any{std::in_place_type<T>, l+r};
}

any engine::add_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return add(l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return add(l.to<std::int64_t>(), r.to<std::int64_t>());
        case index<float>: return add(l.to<float>(), r.to<float>());
        case index<double>: return add(l.to<double>(), r.to<double>());
        default:
            fail();
    }
}

template <class T, class U = T>
any subtract(T const& l, U const& r) {
    return any{std::in_place_type<T>, l-r};
}

any engine::subtract_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return subtract(l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return subtract(l.to<std::int64_t>(), r.to<std::int64_t>());
        case index<float>: return subtract(l.to<float>(), r.to<float>());
        case index<double>: return subtract(l.to<double>(), r.to<double>());
        default:
            fail();
    }
}

template <class T, class U>
any engine::concat(T const& l, U const& r) {
    return any{std::in_place_type<T>, accessor::text{resource_, l, r}};
}
any engine::concat_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<accessor::text>: return concat(l.to<accessor::text>(), r.to<accessor::text>());
        default:
            fail();
    }
}

template <class T, class U = T>
any multiply(T const& l, U const& r) {
    return any{std::in_place_type<T>, l*r};
}

any engine::multiply_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return multiply(l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return multiply(l.to<std::int64_t>(), r.to<std::int64_t>());
        case index<float>: return multiply(l.to<float>(), r.to<float>());
        case index<double>: return multiply(l.to<double>(), r.to<double>());
        default:
            fail();
    }
}

template <class T, class U = T>
any divide(T const& l, U const& r) {
    return any{std::in_place_type<T>, l/r};
}

any engine::divide_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return divide(l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return divide(l.to<std::int64_t>(), r.to<std::int64_t>());
        case index<float>: return divide(l.to<float>(), r.to<float>());
        case index<double>: return divide(l.to<double>(), r.to<double>());
        default:
            fail();
    }
}

template <class T, class U = T>
any remainder(T const& l, U const& r) {
    return any{std::in_place_type<T>, l%r};
}

any engine::remainder_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return remainder(l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return remainder(l.to<std::int64_t>(), r.to<std::int64_t>());
        default:
            fail();
    }
}

template <class T, class U = T>
any conditional_and(T const& l, U const& r) {
    return any{std::in_place_type<T>, l && r};
}

any engine::conditional_and_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<bool>: return conditional_and(l.to<bool>(), r.to<bool>());
        default:
            fail();
    }
}

template <class T, class U = T>
any conditional_or(T const& l, U const& r) {
    return any{std::in_place_type<T>, l || r};
}

any engine::conditional_or_any(any const& l, any const& r) {
    switch(l.type_index()) {
        case index<bool>: return conditional_or(l.to<bool>(), r.to<bool>());
        default:
            fail();
    }
}

any engine::operator()(takatori::scalar::binary const& exp) {
    using optype = takatori::scalar::binary_operator;
    auto l = dispatch(*this, exp.left());
    auto r = dispatch(*this, exp.right());
    switch(exp.operator_kind()) {
        case optype::add: return add_any(l, r);
        case optype::concat: return concat_any(l, r);
        case optype::subtract: return subtract_any(l, r);
        case optype::divide: return divide_any(l, r);
        case optype::multiply: return multiply_any(l, r);
        case optype::remainder: return remainder_any(l, r);
        case optype::conditional_and: return conditional_and_any(l, r);
        case optype::conditional_or: return conditional_or_any(l, r);
        default:
            fail();
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
        // TODO create and use traits for types
        case t::int4: return create_any<std::int32_t>(ref, info);
        case t::int8: return create_any<std::int64_t>(ref, info);
        case t::float4: return create_any<float>(ref, info);
        case t::float8: return create_any<double>(ref, info);
        case t::boolean: return create_any<bool, std::int8_t>(ref, info);
        case t::character: return create_any<accessor::text>(ref, info);
        default: fail();
    }
}

template <class T>
any sign_inversion(T const& l) {
    return any{std::in_place_type<T>, -l};
}

any engine::sign_inversion_any(any const& l) {
    switch(l.type_index()) {
        case index<std::int32_t>: return sign_inversion(l.to<std::int32_t>());
        case index<std::int64_t>: return sign_inversion(l.to<std::int64_t>());
        case index<float>: return sign_inversion(l.to<float>());
        case index<double>: return sign_inversion(l.to<double>());
        default:
            fail();
    }
}

template <class T>
any conditional_not(T const& l) {
    return any{std::in_place_type<T>, ! l};
}

any engine::conditional_not_any(any const& l) {
    switch(l.type_index()) {
        case index<bool>: return conditional_not(l.to<bool>());
        default:
            fail();
    }
}

template <class T>
any length(T const& l) {
    return any{std::in_place_type<std::int32_t>, static_cast<std::string_view>(l).size()};
}

any engine::length_any(any const& l) {
    switch(l.type_index()) {
        case index<accessor::text>: return length(l.to<accessor::text>());
        default:
            fail();
    }
}

any engine::operator()(takatori::scalar::unary const& exp) {
    using optype = takatori::scalar::unary::operator_kind_type;
    auto v = dispatch(*this, exp.operand());
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
        default:
            fail();
    }
}

template <class T, class U = T>
any compare(takatori::scalar::comparison_operator op, T const& l, U const& r) {
    using optype = takatori::scalar::comparison_operator;
    bool result = false;
    switch(op) {
        case optype::equal: result = l == r; break;
        case optype::not_equal: result = l != r; break;
        case optype::greater: result = l > r; break;
        case optype::greater_equal: result = l >= r; break;
        case optype::less: result = l < r; break;
        case optype::less_equal: result = l <= r; break;
        default:
            fail();
    }
    return any{std::in_place_type<bool>, result};
}

any engine::compare_any(takatori::scalar::comparison_operator optype, any const& l, any const& r) {
    switch(l.type_index()) {
        case index<std::int32_t>: return compare(optype, l.to<std::int32_t>(), r.to<std::int32_t>());
        case index<std::int64_t>: return compare(optype, l.to<std::int64_t>(), r.to<std::int64_t>());
        case index<float>: return compare(optype, l.to<float>(), r.to<float>());
        case index<double>: return compare(optype, l.to<double>(), r.to<double>());
        case index<accessor::text>: return compare(optype, l.to<accessor::text>(), r.to<accessor::text>());
        default:
            fail();
    }
}

any engine::operator()(takatori::scalar::immediate const& exp) {
    auto& type = info_.type_of(exp);
    return utils::as_any(exp.value(), type, resource_);
}

any engine::operator()(takatori::scalar::cast const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::compare const& exp) {
    auto l = dispatch(*this, exp.left());
    auto r = dispatch(*this, exp.right());
    return compare_any(exp.operator_kind(), l, r);
}

any engine::operator()(takatori::scalar::match const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::conditional const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::coalesce const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::let const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::function_call const&) {
    fail(); //TODO implement
}

any engine::operator()(takatori::scalar::extension const&) {
    fail();
}

}

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
    variable_table& variables,
    evaluator::memory_resource* resource
) const {
    details::engine c{variables, *info_, host_variables_, resource};
    return takatori::scalar::dispatch(c, *expression_);
}

bool evaluate_bool(
    evaluator& eval,
    variable_table& variables,
    memory::lifo_paged_memory_resource* resource
) {
    utils::checkpoint_holder h{resource};
    auto a = eval(variables, resource);
    return a.has_value() && a.to<bool>();
}
} // namespace
