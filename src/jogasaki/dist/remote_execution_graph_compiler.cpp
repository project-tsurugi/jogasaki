/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "remote_execution_graph_compiler.h"

#include <exception>
#include <utility>

#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <yugawara/analyzer/expression_analyzer.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>

#include <jogasaki/executor/process/process_preparation.h>

namespace jogasaki::dist {

[[nodiscard]] static bool unsupported_remote_expression(takatori::scalar::expression const& expression,
                                                        takatori::type::type_kind kind,
                                                        yugawara::analyzer::expression_mapping const& expressions) {
    using k = takatori::type::type_kind;

    if (kind == k::int1 || kind == k::int2) {
        return true;
    }
    if (expression.kind() == takatori::scalar::expression_kind::compare) {
        auto const& comparison = takatori::util::unsafe_downcast<takatori::scalar::compare>(expression);
        if (expressions.find(comparison.left()).type().kind() == k::boolean ||
            expressions.find(comparison.right()).type().kind() == k::boolean) {
            return true;
        }
    }
    if (expression.kind() == takatori::scalar::expression_kind::binary) {
        auto const& binary = takatori::util::unsafe_downcast<takatori::scalar::binary>(expression);
        if (binary.operator_kind() == takatori::scalar::binary_operator::concat && kind == k::octet) {
            return true;
        }
    }
    if (expression.kind() == takatori::scalar::expression_kind::unary) {
        auto const& unary = takatori::util::unsafe_downcast<takatori::scalar::unary>(expression);
        if (unary.operator_kind() == takatori::scalar::unary_operator::length &&
            expressions.find(unary.operand()).type().kind() == k::octet) {
            return true;
        }
    }
    if (expression.kind() == takatori::scalar::expression_kind::cast) {
        auto const& cast = takatori::util::unsafe_downcast<takatori::scalar::cast>(expression);
        auto source_kind = expressions.find(cast.operand()).type().kind();
        if (kind == k::boolean || source_kind == k::boolean || source_kind == k::date ||
            source_kind == k::time_of_day || source_kind == k::time_point) {
            return true;
        }
    }
    return false;
}

remote_execution_graph_compile_result compile_remote_execution_graph(takatori::relation::graph_type const& graph) {
    takatori::relation::expression const* terminal{};
    for (auto const& expression : graph) {
        if (expression.kind() == takatori::relation::expression_kind::offer) {
            if (terminal != nullptr) {
                return {{}, {}, "remote graph has multiple offer operators"};
            }
            terminal = std::addressof(expression);
        }
    }
    if (terminal == nullptr) {
        return {{}, {}, "remote graph has no offer operator"};
    }

    auto expressions = std::make_shared<yugawara::analyzer::expression_mapping>();
    auto variables = std::make_shared<yugawara::analyzer::variable_mapping>();
    yugawara::analyzer::expression_analyzer analyzer{expressions, variables};
    analyzer.allow_unresolved(false);
    try {
        if (!analyzer.resolve(*terminal, true, true)) {
            return {{}, {}, "failed to resolve remote graph types"};
        }
        // Check inferred types, rather than trusting caller-supplied Portable IR annotations.
        bool unsupported{};
        expressions->each([&](takatori::scalar::expression const& expression, auto const& resolution) {
            unsupported =
                unsupported_remote_expression(expression, resolution.type().kind(), *expressions) || unsupported;
        });
        if (unsupported) {
            return {{}, {}, "type or operation is outside remote execution SQL support", true};
        }
        auto info = std::make_shared<yugawara::compiled_info>(expressions, variables);
        auto processor = executor::process::prepare_processor(graph, *info);
        return {std::move(info), std::move(processor), {}};
    } catch (std::exception const& e) {
        return {{}, {}, std::string{"failed to compile remote graph: "} + e.what()};
    }
}

} // namespace jogasaki::dist
