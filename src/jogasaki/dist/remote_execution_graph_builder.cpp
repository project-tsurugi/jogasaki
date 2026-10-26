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
#include "remote_execution_graph_builder.h"

#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <takatori/relation/filter.h>
#include <takatori/relation/find.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/dist/remote_execution_block_decoder.h>

#include "remote_type_codec.h"

namespace jogasaki::dist {

namespace relation = takatori::relation;
namespace scalar = takatori::scalar;

[[nodiscard]] static remote_execution_graph_build_result failure(std::string message) {
    return {{}, std::move(message)};
}

template <class T> [[nodiscard]] static std::string missing(char const* category, T const& value) {
    std::ostringstream out{};
    out << "cannot resolve " << category << ' ' << value;
    return out.str();
}

[[nodiscard]] static std::optional<relation::endpoint_kind> endpoint_kind(remote_endpoint_kind kind) {
    switch (kind) {
        case remote_endpoint_kind::unbound:
            return relation::endpoint_kind::unbound;
        case remote_endpoint_kind::inclusive:
            return relation::endpoint_kind::inclusive;
        case remote_endpoint_kind::exclusive:
            return relation::endpoint_kind::exclusive;
        case remote_endpoint_kind::prefixed_inclusive:
            return relation::endpoint_kind::prefixed_inclusive;
        case remote_endpoint_kind::prefixed_exclusive:
            return relation::endpoint_kind::prefixed_exclusive;
    }
    return {};
}

[[nodiscard]] static std::optional<scalar::unary_operator> unary_operator(std::string_view name) {
    using kind = scalar::unary_operator;
    if (name == "plus") {
        return kind::plus;
    }
    if (name == "sign_inversion") {
        return kind::sign_inversion;
    }
    if (name == "length") {
        return kind::length;
    }
    if (name == "conditional_not") {
        return kind::conditional_not;
    }
    if (name == "is_null") {
        return kind::is_null;
    }
    if (name == "is_true") {
        return kind::is_true;
    }
    if (name == "is_false") {
        return kind::is_false;
    }
    if (name == "is_unknown") {
        return kind::is_unknown;
    }
    return {};
}

[[nodiscard]] static std::optional<scalar::binary_operator> binary_operator(std::string_view name) {
    using kind = scalar::binary_operator;
    if (name == "add") {
        return kind::add;
    }
    if (name == "subtract") {
        return kind::subtract;
    }
    if (name == "multiply") {
        return kind::multiply;
    }
    if (name == "divide") {
        return kind::divide;
    }
    if (name == "remainder") {
        return kind::remainder;
    }
    if (name == "concat") {
        return kind::concat;
    }
    if (name == "conditional_and") {
        return kind::conditional_and;
    }
    if (name == "conditional_or") {
        return kind::conditional_or;
    }
    return {};
}

[[nodiscard]] static std::optional<scalar::comparison_operator> comparison_operator(std::string_view name) {
    using kind = scalar::comparison_operator;
    if (name == "equal") {
        return kind::equal;
    }
    if (name == "not_equal") {
        return kind::not_equal;
    }
    if (name == "less") {
        return kind::less;
    }
    if (name == "less_equal") {
        return kind::less_equal;
    }
    if (name == "greater") {
        return kind::greater;
    }
    if (name == "greater_equal") {
        return kind::greater_equal;
    }
    if (name == "is_not_distinct_from") {
        return kind::is_not_distinct_from;
    }
    if (name == "is_distinct_from") {
        return kind::is_distinct_from;
    }
    return {};
}

[[nodiscard]] static std::optional<scalar::cast_loss_policy> cast_loss_policy(std::string_view name) {
    using kind = scalar::cast_loss_policy;
    if (name == "ignore") {
        return kind::ignore;
    }
    if (name == "floor") {
        return kind::floor;
    }
    if (name == "ceil") {
        return kind::ceil;
    }
    if (name == "unknown") {
        return kind::unknown;
    }
    if (name == "warn") {
        return kind::warn;
    }
    if (name == "error") {
        return kind::error;
    }
    return {};
}

namespace {

class builder {
public:
    builder(remote_execution_block const& block, remote_execution_graph_resolver const& resolver)
        : block_(&block), resolver_(&resolver) {
        for (auto id : block_->stream_variables) {
            variables_.emplace(id, bindings_.stream_variable("remote_" + std::to_string(id)));
        }
        for (auto const& expression : block_->scalar_expressions) {
            expressions_.emplace(expression.expression_id, std::addressof(expression));
        }
    }

    [[nodiscard]] remote_execution_graph_build_result build() {
        auto graph = std::make_unique<relation::graph_type>();
        relation::expression* previous{};
        for (auto const& op : block_->operators) {
            auto node = build_operator(op);
            if (!node) {
                return failure(error_);
            }
            auto& current = graph->insert(std::move(node));
            if (previous != nullptr) {
                previous->output_ports()[0] >> current.input_ports()[0];
            }
            previous = std::addressof(current);
        }
        return {std::move(graph), {}};
    }

private:
    [[nodiscard]] takatori::descriptor::variable const* variable(std::uint64_t id) {
        auto it = variables_.find(id);
        if (it == variables_.end()) {
            error_ = missing("stream variable", id);
            return nullptr;
        }
        return std::addressof(it->second);
    }

    [[nodiscard]] std::unique_ptr<scalar::expression> expression(std::uint64_t id) {
        if (++expanded_expression_count_ > 10000) {
            error_ = "scalar expression expansion limit exceeded";
            return {};
        }
        auto it = expressions_.find(id);
        if (it == expressions_.end()) {
            error_ = missing("scalar expression", id);
            return {};
        }
        auto const& source = *it->second;
        switch (source.kind) {
            case remote_scalar_expression_kind::immediate: {
                auto result = resolver_->resolve_immediate(source);
                if (!result) {
                    error_ = missing("immediate expression", id);
                }
                return result;
            }
            case remote_scalar_expression_kind::variable_reference: {
                auto* descriptor = variable(*source.variable_id);
                if (!descriptor) {
                    return {};
                }
                return std::make_unique<scalar::variable_reference>(*descriptor);
            }
            case remote_scalar_expression_kind::unary: {
                auto kind = unary_operator(source.operator_kind);
                if (!kind) {
                    error_ = missing("unary operator", source.operator_kind);
                    return {};
                }
                auto operand = expression(source.operand_expression_ids[0]);
                if (!operand) {
                    return {};
                }
                return std::make_unique<scalar::unary>(*kind, std::move(operand));
            }
            case remote_scalar_expression_kind::cast:
                return build_cast(source);
            case remote_scalar_expression_kind::binary: {
                auto kind = binary_operator(source.operator_kind);
                if (!kind) {
                    error_ = missing("binary operator", source.operator_kind);
                    return {};
                }
                auto left = expression(source.operand_expression_ids[0]);
                auto right = expression(source.operand_expression_ids[1]);
                if (!left || !right) {
                    return {};
                }
                return std::make_unique<scalar::binary>(*kind, std::move(left), std::move(right));
            }
            case remote_scalar_expression_kind::compare: {
                auto kind = comparison_operator(source.operator_kind);
                if (!kind) {
                    error_ = missing("comparison operator", source.operator_kind);
                    return {};
                }
                auto left = expression(source.operand_expression_ids[0]);
                auto right = expression(source.operand_expression_ids[1]);
                if (!left || !right) {
                    return {};
                }
                return std::make_unique<scalar::compare>(*kind, std::move(left), std::move(right));
            }
        }
        error_ = missing("scalar expression kind", id);
        return {};
    }

    template <class Key>
    [[nodiscard]] std::optional<std::vector<Key>> keys(remote_source const& source,
                                                       std::vector<std::uint64_t> const& expression_ids) {
        std::vector<Key> result{};
        for (std::size_t i = 0; i < expression_ids.size(); ++i) {
            auto column = resolver_->resolve_storage_key_column(source, i);
            if (!column) {
                error_ = missing("storage key column", i);
                return {};
            }
            auto value = expression(expression_ids[i]);
            if (!value) {
                return {};
            }
            result.emplace_back(*column, std::move(value));
        }
        return result;
    }

    [[nodiscard]] std::optional<std::vector<relation::scan::column>> columns(remote_source const& source) {
        std::vector<relation::scan::column> result{};
        for (auto const& column : source.columns) {
            auto storage_column =
                resolver_->resolve_storage_column(source, column);
            auto* destination = variable(column.destination_variable_id);
            if (!storage_column) {
                error_ = missing("storage column", column.column_name);
                return {};
            }
            if (!destination) {
                return {};
            }
            // Native variable layout retains only variables consumed by expressions or offer.
            // Resolve every input column first so forbidden/unknown columns are still rejected.
            bool used{};
            for (auto const& scalar : block_->scalar_expressions) {
                used = used || scalar.variable_id == column.destination_variable_id;
            }
            for (auto const& op : block_->operators) {
                if (op.offer) {
                    for (auto const& output : op.offer->columns) {
                        used = used || output.source_variable_id == column.destination_variable_id;
                    }
                }
            }
            if (used) {
                result.emplace_back(*storage_column, *destination);
            }
        }
        return result;
    }

    [[nodiscard]] std::unique_ptr<relation::expression> build_operator(remote_operator const& op) {
        switch (op.kind) {
            case remote_operator_kind::scan: {
                auto const& source = *op.source;
                auto storage = resolver_->resolve_storage(source);
                auto mappings = columns(source);
                auto lower_keys = keys<relation::scan::key>(source, source.lower_key_expression_ids);
                auto upper_keys = keys<relation::scan::key>(source, source.upper_key_expression_ids);
                if (!storage) {
                    error_ = missing("storage", source.table_name + "." + source.index_name);
                    return {};
                }
                if (!mappings || !lower_keys || !upper_keys) {
                    return {};
                }
                relation::scan::endpoint lower{std::move(*lower_keys), *endpoint_kind(source.lower_endpoint_kind)};
                relation::scan::endpoint upper{std::move(*upper_keys), *endpoint_kind(source.upper_endpoint_kind)};
                return std::make_unique<relation::scan>(*storage, std::move(*mappings), std::move(lower),
                                                        std::move(upper), source.limit);
            }
            case remote_operator_kind::find: {
                auto const& source = *op.source;
                auto storage = resolver_->resolve_storage(source);
                auto mappings = columns(source);
                auto find_keys = keys<relation::find::key>(source, source.find_key_expression_ids);
                if (!storage) {
                    error_ = missing("storage", source.table_name + "." + source.index_name);
                    return {};
                }
                if (!mappings || !find_keys) {
                    return {};
                }
                return std::make_unique<relation::find>(*storage, std::move(*mappings), std::move(*find_keys));
            }
            case remote_operator_kind::filter: {
                auto condition = expression(*op.filter_expression_id);
                if (!condition) {
                    return {};
                }
                return std::make_unique<relation::filter>(std::move(condition));
            }
            case remote_operator_kind::project: {
                std::vector<relation::project::column> columns{};
                for (auto const& column : op.project_columns) {
                    auto* destination = variable(column.destination_variable_id);
                    auto value = expression(column.value_expression_id);
                    if (!destination || !value) {
                        return {};
                    }
                    columns.emplace_back(*destination, std::move(value));
                }
                return std::make_unique<relation::project>(std::move(columns));
            }
            case remote_operator_kind::offer:
                return build_offer(op);
        }
        error_ = missing("operator", op.operator_id);
        return {};
    }

    [[nodiscard]] std::unique_ptr<scalar::expression> build_cast(remote_scalar_expression const& source) {
        auto type = source.type_payload.empty() ? resolver_->resolve_type(source.type)
                                                : decode_remote_type(source.type_payload);
        auto policy = cast_loss_policy(source.operator_kind);
        if (!type) {
            error_ = missing("type", source.type);
            return {};
        }
        if (!policy) {
            error_ = missing("cast loss policy", source.operator_kind);
            return {};
        }
        auto operand = expression(source.operand_expression_ids[0]);
        if (!operand) {
            return {};
        }
        return std::make_unique<scalar::cast>(std::move(type), *policy, std::move(operand));
    }

    [[nodiscard]] std::unique_ptr<relation::expression> build_offer(remote_operator const& op) {
        auto destination = resolver_->resolve_external_relation(op.offer->external_relation_id);
        if (!destination) {
            error_ = missing("external relation", op.offer->external_relation_id);
            return {};
        }
        std::vector<relation::step::offer::column> columns{};
        for (auto const& column : op.offer->columns) {
            auto* source = variable(column.source_variable_id);
            auto target =
                resolver_->resolve_external_column(op.offer->external_relation_id, column.destination_column_id);
            if (!source) {
                return {};
            }
            if (!target) {
                error_ = missing("external column", column.destination_column_id);
                return {};
            }
            columns.emplace_back(*source, *target);
        }
        return std::make_unique<relation::step::offer>(*destination, std::move(columns));
    }

    std::size_t expanded_expression_count_{};
    remote_execution_block const* block_;
    remote_execution_graph_resolver const* resolver_;
    yugawara::binding::factory bindings_{};
    std::unordered_map<std::uint64_t, takatori::descriptor::variable> variables_{};
    std::unordered_map<std::uint64_t, remote_scalar_expression const*> expressions_{};
    std::string error_{};
};

} // namespace

remote_execution_graph_build_result build_remote_execution_graph(remote_execution_block const& block,
                                                                 remote_execution_graph_resolver const& resolver) {
    if (auto error = validate_remote_execution_block(block)) {
        return failure(std::move(*error));
    }
    return builder{block, resolver}.build();
}

} // namespace jogasaki::dist
