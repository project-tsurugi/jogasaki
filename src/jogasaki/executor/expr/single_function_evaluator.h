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
#include <functional>

#include <takatori/decimal/triple.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/coalesce.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/conditional.h>
#include <takatori/scalar/expression.h>
#include <takatori/scalar/extension.h>
#include <takatori/scalar/function_call.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/let.h>
#include <takatori/scalar/match.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/compiled_info.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/paged_memory_resource.h>

#include "evaluator_context.h"

namespace jogasaki::executor::expr {

using any = jogasaki::data::any;

/**
 * @brief expression evaluator
 */
class single_function_evaluator {
public:
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief construct empty object
     */
    single_function_evaluator() = default;
    /**
     * @brief construct new object
     */
    single_function_evaluator(
        std::size_t function_def_id,
        yugawara::function::configurable_provider const& functions
    ) noexcept;

    /**
     * @brief evaluate the expression
     * @return the result of evaluation
     */
    [[nodiscard]] data::any operator()(evaluator_context& ctx) const;

private:
    std::shared_ptr<takatori::scalar::expression const> expression_{};
    executor::expr::evaluator evaluator_{};
};

}  // namespace jogasaki::executor::expr
