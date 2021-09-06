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
#include <takatori/scalar/cast.h>
#include <takatori/scalar/match.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/coalesce.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/conditional.h>
#include <takatori/scalar/extension.h>
#include <takatori/scalar/function_call.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/let.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/expression/any.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/utils/checkpoint_holder.h>

namespace jogasaki::executor::process::impl::expression {

namespace details {

class engine {
public:
    using memory_resource = memory::paged_memory_resource;

    engine(
        executor::process::impl::variable_table& variables,
        yugawara::compiled_info const& info,
        executor::process::impl::variable_table const* host_variables,
        memory_resource* resource
    ) noexcept;

    any add_any(any const& l, any const& r);
    any subtract_any(any const& l, any const& r);
    any concat_any(any const& l, any const& r);
    any multiply_any(any const& l, any const& r);
    any divide_any(any const& l, any const& r);
    any remainder_any(any const& l, any const& r);
    any conditional_and_any(any const& l, any const& r);
    any conditional_or_any(any const& l, any const& r);
    any operator()(takatori::scalar::binary const& exp);
    any operator()(takatori::scalar::immediate const& exp);
    any operator()(takatori::scalar::variable_reference const& exp);
    any sign_inversion_any(any const& l);
    any conditional_not_any(any const& l);
    any length_any(any const& l);
    any operator()(takatori::scalar::unary const& exp);
    any operator()(takatori::scalar::cast const&);
    any compare_any(takatori::scalar::comparison_operator optype, any const& l, any const& r);
    any operator()(takatori::scalar::compare const& exp);
    any operator()(takatori::scalar::match const&);
    any operator()(takatori::scalar::conditional const&);
    any operator()(takatori::scalar::coalesce const&);
    any operator()(takatori::scalar::let const&);
    any operator()(takatori::scalar::function_call const&);
    any operator()(takatori::scalar::extension const&);

private:
    executor::process::impl::variable_table& variables_;
    yugawara::compiled_info const& info_{};
    executor::process::impl::variable_table const* host_variables_{};
    memory_resource* resource_{};

    template <class T, class U = T>
    any concat(T const& l, U const& r);
};

}

/**
 * @brief expression evaluator
 */
class evaluator {
public:
    using memory_resource = details::engine::memory_resource;
    /**
     * @brief construct empty object
     */
    evaluator() = default;

    /**
     * @brief construct new object
     * @param expression the expression to be evaluated
     * @param info compiled info associated with the expression
     * @param host_variables the host variable table to be used to resolve reference variable expression
     * using host variable. Pass nullptr if the evaluator never evaluates variable reference.
     */
    explicit evaluator(
        takatori::scalar::expression const& expression,
        yugawara::compiled_info const& info,
        executor::process::impl::variable_table const* host_variables = nullptr
    ) noexcept;

    /**
     * @brief evaluate the expression
     * @details The required memory is allocated from the memory resource to calculate and store the result value.
     * Caller is responsible for release the allocated store after consuming the result. This can be typically done by
     * remembering checkpoint before this call and using memory_resource::deallocate_after() after
     * consuming return value.
     * @param variables variables table used to evaluate the expression
     * @param resource memory resource used to store generated value. Specify nullptr if the evaluation
     * never generate types whose values are stored via memory resource(e.g. accessor::text).
     * Then UB if such type is processed.
     * @return the result of evaluation
     */
    [[nodiscard]] any operator()(
        executor::process::impl::variable_table& variables,
        memory_resource* resource = nullptr
    ) const;

private:
    takatori::scalar::expression const* expression_{};
    yugawara::compiled_info const* info_{};
    executor::process::impl::variable_table const* host_variables_{};
};

/**
 * @brief utility function to evaluate the expression as bool
 * @details This is same as evaluator::operator() except that this function also handles rewinding lifo memory
 * resource used for evaluation.
 * @param eval the evaluator to conduct the evaluation
 * @param variables variables table used to evaluate the expression
 * @param resource memory resource used to store generated value. Specify nullptr if the evaluation
 * never generate types whose values are stored via memory resource(e.g. accessor::text).
 * Then UB if such type is processed.
 * @return the result of evaluation. If the result is empty (i.e. null value), false is returned.
 */
[[nodiscard]] bool evaluate_bool(
    evaluator& eval,
    executor::process::impl::variable_table& variables,
    memory::lifo_paged_memory_resource* resource = nullptr
);

} // namespace
