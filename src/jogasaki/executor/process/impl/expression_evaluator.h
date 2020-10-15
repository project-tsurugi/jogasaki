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

#include <cstddef>
#include <functional>

#include <takatori/scalar/expression.h>
#include <takatori/scalar/walk.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/util/post_visit.h>
#include <takatori/util/fail.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>

#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/executor/process/impl/any.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::process::impl {

using takatori::util::fail;

namespace details {

template<class T>
inline static typename T::view_type value_of(takatori::scalar::expression const& expr) {
    if (auto e = takatori::util::dynamic_pointer_cast<takatori::scalar::immediate>(expr)) {
        if (auto v = takatori::util::dynamic_pointer_cast<T>(e->value())) {
            return v->get();
        }
        throw std::domain_error("inconsistent value type");
    }
    throw std::domain_error("unsupported expression");
}

class expression_callback {
public:
    using stack_type = std::vector<any>;

    using memory_resource = memory::paged_memory_resource;

    expression_callback(
        executor::process::impl::block_scope& scope,
        yugawara::compiled_info const& info
    ) noexcept :
        scope_(scope),
        info_(info)
    {}

    template <typename T, typename E = T>
    void push(stack_type& stack, accessor::record_ref ref, value_info const& info) {
        stack.emplace_back(std::in_place_type<T>, ref.get_value<E>(info.value_offset()));
    }

    template <typename T>
    void push(stack_type& stack, T val) {
        stack.emplace_back(std::in_place_type<T>, val);
    }

    template <typename T>
    T pop(stack_type& stack) {
        auto ret = stack.back().to<T>();
        stack.resize(stack.size() - 1);
        return ret;
    }

    // default handler
    bool operator()(takatori::scalar::expression const&, stack_type&, memory_resource*) {
        fail();
    }

    bool operator()(takatori::scalar::binary const&, stack_type&, memory_resource*) {
        return true;
    }

    template <typename T>
    void binary(takatori::scalar::binary_operator op, stack_type& stack, memory_resource*) {
        using k = takatori::scalar::binary_operator;
        auto right = pop<T>(stack);
        auto left = pop<T>(stack);
        T result = 0;
        switch(op) {
            case k::add: result = left+right; break;
            case k::subtract: result = left-right; break;
            case k::divide: result = left/right; break;
            case k::multiply:
                if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {  //NOLINT
                    result = left*right; break;
                }
                break;
            case k::concat:
                // specialized template should be called for T = accessor::text
                fail();
            case k::remainder:
                if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {  //NOLINT
                    result = left%right;
                }
                break;
            case k::conditional_and:
                if constexpr (std::is_same_v<T, bool>) {  //NOLINT
                    result = left && right;
                }
                break;
            case k::conditional_or:
                if constexpr (std::is_same_v<T, bool>) {  //NOLINT
                    result = left || right;
                }
                break;
            default:
                fail();
        }
        push<T>(stack, result);
    }

    void operator()(takatori::util::post_visit, takatori::scalar::binary const& arg, stack_type& stack, memory_resource* resource);

    bool operator()(takatori::scalar::immediate const& arg, stack_type& stack, memory_resource* resource) {
        using t = takatori::type::type_kind;
        auto& type = info_.type_of(arg);
        switch(type.kind()) {
            // TODO create and use traits for types
            case t::int4: push<std::int32_t>(stack, value_of<takatori::value::int4>(arg)); break;
            case t::int8: push<std::int64_t>(stack, value_of<takatori::value::int8>(arg)); break;
            case t::float4: push<float>(stack, value_of<takatori::value::float4>(arg)); break;
            case t::float8: push<double>(stack, value_of<takatori::value::float8>(arg)); break;
            case t::boolean: push<bool>(stack, value_of<takatori::value::boolean>(arg)); break;
            case t::character: {
                auto sv = value_of<takatori::value::character>(arg);
                push<accessor::text>(stack, accessor::text{resource, sv});
                break;
            }
            default: fail();
        }
        return false;
    }

    bool operator()(takatori::scalar::compare const&, stack_type&, memory_resource*) {
        return true;
    }

    template <typename T>
    void compare(takatori::scalar::comparison_operator op, stack_type& stack, memory_resource*) {
        using k = takatori::scalar::comparison_operator;
        auto right = pop<T>(stack);
        auto left = pop<T>(stack);
        bool result = false;
        switch(op) {
            case k::equal: result = left == right; break;
            case k::not_equal: result = left != right; break;
            case k::greater: result = left > right; break;
            case k::greater_equal: result = left >= right; break;
            case k::less: result = left < right; break;
            case k::less_equal: result = left <= right; break;
            default:
                fail();
        }
        push<bool>(stack, result);
    }

    void operator()(takatori::util::post_visit, takatori::scalar::compare const& arg, stack_type& stack, memory_resource* resource) {
        auto& type = info_.type_of(arg.left()); //TODO support cases where left/right types differ
        using t = takatori::type::type_kind;
        switch(type.kind()) {
            case t::int4: compare<std::int32_t>(arg.operator_kind(), stack, resource); break;
            case t::int8: compare<std::int64_t>(arg.operator_kind(), stack, resource); break;
            case t::float4: compare<float>(arg.operator_kind(), stack, resource); break;
            case t::float8: compare<double>(arg.operator_kind(), stack, resource); break;
            case t::boolean: compare<bool>(arg.operator_kind(), stack, resource); break;
            case t::character: compare<accessor::text>(arg.operator_kind(), stack, resource); break;
            default: fail();
        }
    }

    bool operator()(takatori::scalar::unary const&, stack_type&, memory_resource*) {
        return true;
    }

    void operator()(takatori::util::post_visit, takatori::scalar::unary const& arg, stack_type& stack, memory_resource*) {
        using k = takatori::scalar::unary::operator_kind_type;
        using t = takatori::type::type_kind;
        switch(arg.operator_kind()) {
            case k::plus:
                // no-op - pass current stack upward
                break;
            case k::sign_inversion: {
                auto& type = info_.type_of(arg);
                switch(type.kind()) {
                    case t::int4: push<std::int32_t>(stack, -pop<std::int32_t>(stack)); break;
                    case t::int8: push<std::int64_t>(stack, -pop<std::int64_t>(stack)); break;
                    case t::float4: push<float>(stack, -pop<float>(stack)); break;
                    case t::float8: push<double>(stack, -pop<double>(stack)); break;

                    default: fail();
                }
                break;
            }
            case k::conditional_not: {
                auto& type = info_.type_of(arg);
                switch(type.kind()) {
                    case t::boolean: push<bool>(stack, !pop<bool>(stack)); break;
                    default: fail();
                }
                break;
            }
            case k::length: {
                auto& type = info_.type_of(arg.operand());
                switch(type.kind()) {
                    case t::character: {
                        auto txt = pop<accessor::text>(stack);
                        push<std::int32_t>(stack, static_cast<std::string_view>(txt).size());
                        break;
                    }
                    default:
                        fail();
                }
                break;
            }
            default:
                fail();
        }
    }

    bool operator()(takatori::scalar::variable_reference const& arg, stack_type& stack, memory_resource*) {
        auto& info = scope_.value_map().at(arg.variable());
        auto ref = scope_.store().ref();
        using t = takatori::type::type_kind;
        auto& type = info_.type_of(arg);
        switch(type.kind()) {
            // TODO create and use traits for types
            case t::int4: push<std::int32_t>(stack, ref, info); break;
            case t::int8: push<std::int64_t>(stack, ref, info); break;
            case t::float4: push<float>(stack, ref, info); break;
            case t::float8: push<double>(stack, ref, info); break;
            case t::boolean: push<bool, std::int8_t>(stack, ref, info); break;
            case t::character: push<accessor::text>(stack, ref, info); break;
            default: fail();
        }
        return false;
    }
private:
    executor::process::impl::block_scope& scope_;
    yugawara::compiled_info const& info_{};
};

template <>
void expression_callback::binary<accessor::text>(takatori::scalar::binary_operator op, stack_type& stack, memory_resource* resource);

}

/**
 * @brief expression evaluator
 */
class expression_evaluator {
public:
    using memory_resource = details::expression_callback::memory_resource;
    /**
     * @brief construct empty object
     */
    expression_evaluator() = default;

    /**
     * @brief construct new object
     */
    explicit expression_evaluator(
        takatori::scalar::expression const& expression,
        yugawara::compiled_info const& info
    ) noexcept :
        expression_(std::addressof(expression)),
        info_(std::addressof(info))
    {}

    /**
     * @brief evaluate the expression
     * @details The required memory is allocated from the memory resource to calculate and store the result value.
     * Caller is responsible for release the allocated store after consuming the result. This can be typically done by
     * remembering checkpoint before this call and using memory_resource::deallocate_after() after consuming return value.
     * @param scope scope variables used to evaluate the expression
     * @param resource memory resource used to store generated value. Specify nullptr if the evaluation
     * never generate types whose values are stored via memory resource(e.g. accessor::text). Then UB if such type is processed.
     * @return the result of evaluation
     */
    [[nodiscard]] any operator()(
        executor::process::impl::block_scope& scope,
        memory_resource* resource = nullptr
    ) const {
        details::expression_callback c{scope, *info_};
        details::expression_callback::stack_type stack{};
        takatori::scalar::walk(c, *expression_, stack, resource);
        return stack.back();
    }

private:
    takatori::scalar::expression const* expression_{};
    yugawara::compiled_info const* info_{};
};

} // namespace
