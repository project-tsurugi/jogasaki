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
#include "remote_execution_block.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string_view>
#include <utility>

#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/find.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <yugawara/binding/external_relation_info.h>
#include <yugawara/binding/extract.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/index.h>

#include <jogasaki/dist/remote_process_execution_block_shape.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>

#include "remote_type_codec.h"

namespace jogasaki::dist {

namespace {

namespace relation = takatori::relation;
namespace scalar = takatori::scalar;

} // namespace

[[nodiscard]] static remote_operator_kind operator_kind(relation::expression_kind kind) {
    using source = relation::expression_kind;
    switch (kind) {
        case source::scan:
            return remote_operator_kind::scan;
        case source::find:
            return remote_operator_kind::find;
        case source::filter:
            return remote_operator_kind::filter;
        case source::project:
            return remote_operator_kind::project;
        case source::offer:
            return remote_operator_kind::offer;
        default:
            std::abort();
    }
}

[[nodiscard]] static std::string_view to_string_view(remote_operator_kind kind) noexcept {
    switch (kind) {
        case remote_operator_kind::scan:
            return "scan";
        case remote_operator_kind::find:
            return "find";
        case remote_operator_kind::filter:
            return "filter";
        case remote_operator_kind::project:
            return "project";
        case remote_operator_kind::offer:
            return "offer";
    }
    std::abort();
}

[[nodiscard]] static remote_endpoint_kind endpoint_kind(relation::endpoint_kind kind) noexcept {
    using source = relation::endpoint_kind;
    switch (kind) {
        case source::unbound:
            return remote_endpoint_kind::unbound;
        case source::inclusive:
            return remote_endpoint_kind::inclusive;
        case source::exclusive:
            return remote_endpoint_kind::exclusive;
        case source::prefixed_inclusive:
            return remote_endpoint_kind::prefixed_inclusive;
        case source::prefixed_exclusive:
            return remote_endpoint_kind::prefixed_exclusive;
    }
    std::abort();
}

[[nodiscard]] static std::string_view to_string_view(remote_endpoint_kind kind) noexcept {
    switch (kind) {
        case remote_endpoint_kind::unbound:
            return "unbound";
        case remote_endpoint_kind::inclusive:
            return "inclusive";
        case remote_endpoint_kind::exclusive:
            return "exclusive";
        case remote_endpoint_kind::prefixed_inclusive:
            return "prefixed_inclusive";
        case remote_endpoint_kind::prefixed_exclusive:
            return "prefixed_exclusive";
    }
    std::abort();
}

[[nodiscard]] static std::string_view to_string_view(remote_scalar_expression_kind kind) noexcept {
    switch (kind) {
        case remote_scalar_expression_kind::immediate:
            return "immediate";
        case remote_scalar_expression_kind::variable_reference:
            return "variable_reference";
        case remote_scalar_expression_kind::unary:
            return "unary";
        case remote_scalar_expression_kind::cast:
            return "cast";
        case remote_scalar_expression_kind::binary:
            return "binary";
        case remote_scalar_expression_kind::compare:
            return "compare";
    }
    std::abort();
}

[[nodiscard]] static relation::expression const* first(relation::graph_type const& graph) {
    relation::expression const* result{};
    relation::enumerate_top(graph, [&](relation::expression const& node) { result = std::addressof(node); });
    return result;
}

[[nodiscard]] static relation::expression const* downstream(relation::expression const& node) {
    auto outputs = node.output_ports();
    if (outputs.empty()) {
        return {};
    }
    auto opposite = outputs[0].opposite();
    return opposite ? std::addressof(opposite->owner()) : nullptr;
}

namespace {

class block_builder_context {
public:
    block_builder_context(remote_execution_block& block, yugawara::compiled_info const* compiled_info,
                          meta::record_meta const* result_meta, meta::variable_order const* result_order,
                          remote_catalog_snapshot const* catalogs, std::string_view remote_server)
        : block_(&block), compiled_info_(compiled_info), result_meta_(result_meta), result_order_(result_order),
          catalogs_(catalogs), remote_server_(remote_server) {}

    [[nodiscard]] std::optional<std::uint64_t> definition_id(yugawara::storage::index const& index) const {
        if (catalogs_) {
            auto identity = catalogs_->remote_definition(index);
            if (!identity || identity->kind != remote_declaration_kind::index ||
                (!remote_server_.empty() && identity->remote_server_name != remote_server_)) {
                return std::nullopt;
            }
            return identity->remote_definition_id;
        }
        // Standalone graph conversion may use a local worker declaration. A
        // coordinator sending to a server must use its retained remote snapshot.
        return remote_server_.empty() ? index.definition_id() : std::nullopt;
    }

    [[nodiscard]] std::uint64_t variable_id(takatori::descriptor::variable const& variable) {
        for (std::size_t i = 0; i < variables_.size(); ++i) {
            if (variables_[i] == variable) {
                return i;
            }
        }
        auto id = static_cast<std::uint64_t>(variables_.size());
        variables_.emplace_back(variable);
        block_->stream_variables.emplace_back(id);
        return id;
    }

    [[nodiscard]] std::optional<std::uint64_t> expression_id(scalar::expression const& expression) {
        remote_scalar_expression result{};
        switch (expression.kind()) {
            case scalar::expression_kind::immediate: {
                auto const& e = dynamic_cast<scalar::immediate const&>(expression);
                result.kind = remote_scalar_expression_kind::immediate;
                result.value_kind = std::string{takatori::value::to_string_view(e.value().kind())};
                std::ostringstream value{};
                value << e.value();
                result.value = value.str();
                std::ostringstream type{};
                type << e.type();
                result.type = type.str();
                result.type_payload = encode_remote_type(e.type());
                result.literal_payload = encode_remote_literal(e.value());
                break;
            }
            case scalar::expression_kind::variable_reference: {
                auto const& e = dynamic_cast<scalar::variable_reference const&>(expression);
                if (yugawara::binding::kind_of(e.variable()) !=
                    yugawara::binding::variable_info_kind::stream_variable) {
                    return std::nullopt;
                }
                result.kind = remote_scalar_expression_kind::variable_reference;
                result.variable_id = variable_id(e.variable());
                break;
            }
            case scalar::expression_kind::unary: {
                auto const& e = dynamic_cast<scalar::unary const&>(expression);
                result.kind = remote_scalar_expression_kind::unary;
                result.operator_kind = std::string{scalar::to_string_view(e.operator_kind())};
                auto operand = expression_id(e.operand());
                if (!operand) {
                    return std::nullopt;
                }
                result.operand_expression_ids.emplace_back(*operand);
                break;
            }
            case scalar::expression_kind::cast: {
                auto const& e = dynamic_cast<scalar::cast const&>(expression);
                result.kind = remote_scalar_expression_kind::cast;
                result.operator_kind = std::string{scalar::to_string_view(e.loss_policy())};
                std::ostringstream type{};
                type << e.type();
                result.type = type.str();
                result.type_payload = encode_remote_type(e.type());
                auto operand = expression_id(e.operand());
                if (!operand) {
                    return std::nullopt;
                }
                result.operand_expression_ids.emplace_back(*operand);
                break;
            }
            case scalar::expression_kind::binary: {
                auto const& e = dynamic_cast<scalar::binary const&>(expression);
                result.kind = remote_scalar_expression_kind::binary;
                result.operator_kind = std::string{scalar::to_string_view(e.operator_kind())};
                auto left = expression_id(e.left());
                auto right = expression_id(e.right());
                if (!left || !right) {
                    return std::nullopt;
                }
                result.operand_expression_ids = {*left, *right};
                break;
            }
            case scalar::expression_kind::compare: {
                auto const& e = dynamic_cast<scalar::compare const&>(expression);
                result.kind = remote_scalar_expression_kind::compare;
                result.operator_kind = std::string{scalar::to_string_view(e.operator_kind())};
                auto left = expression_id(e.left());
                auto right = expression_id(e.right());
                if (!left || !right) {
                    return std::nullopt;
                }
                result.operand_expression_ids = {*left, *right};
                break;
            }
            default:
                return std::nullopt;
        }
        result.expression_id = block_->scalar_expressions.size();
        block_->scalar_expressions.emplace_back(std::move(result));
        return block_->scalar_expressions.back().expression_id;
    }

    [[nodiscard]] bool create_offer_payload(relation::step::offer const& offer, remote_operator& op) {
        auto external = yugawara::binding::extract_if<yugawara::binding::external_relation_info>(offer.destination());
        auto external_relation_id = external ? static_cast<std::uint64_t>(external->relation_id()) : 0;
        if (external && external->columns().size() != offer.columns().size()) {
            return false;
        }
        op.offer = remote_offer{external_relation_id, {}};
        block_->external_relations.emplace_back(external_relation_id);
        block_->result_schema.resize(offer.columns().size());
        std::vector<bool> assigned(offer.columns().size());
        if (result_meta_ && result_meta_->field_count() != offer.columns().size()) {
            return false;
        }
        for (auto const& mapping : offer.columns()) {
            auto column_id = result_order_ ? result_order_->index(mapping.destination()) : op.offer->columns.size();
            if (external) {
                // Native external columns define the output layout, independent of offer order.
                auto const& columns = external->columns();
                auto it = std::find(columns.begin(), columns.end(), mapping.destination());
                if (it == columns.end()) return false;
                auto position = static_cast<std::size_t>(it - columns.begin());
                if (result_order_ && column_id != position) return false;
                column_id = position;
            }
            if (column_id >= assigned.size() || assigned[column_id]) {
                return false;
            }
            assigned[column_id] = true;
            op.offer->columns.emplace_back(remote_offer_column{variable_id(mapping.source()), column_id});
            remote_result_column result_column{column_id, {}, result_meta_ ? result_meta_->nullable(column_id) : false};
            if (compiled_info_) {
                std::ostringstream type{};
                type << compiled_info_->type_of(mapping.destination());
                result_column.type = type.str();
                result_column.type_payload = encode_remote_type(compiled_info_->type_of(mapping.destination()));
            }
            block_->result_schema[column_id] = std::move(result_column);
        }
        return true;
    }

private:
    remote_execution_block* block_;
    yugawara::compiled_info const* compiled_info_{};
    meta::record_meta const* result_meta_{};
    meta::variable_order const* result_order_{};
    remote_catalog_snapshot const* catalogs_{};
    std::string_view remote_server_{};
    std::vector<takatori::descriptor::variable> variables_{};
};

} // namespace

template <class Source>
[[nodiscard]] static remote_source create_source_payload(Source const& source, block_builder_context& context) {
    auto const& index = yugawara::binding::extract<yugawara::storage::index>(source.source());
    remote_source result{};
    result.table_name = index.table().simple_name();
    result.index_name = index.simple_name();
    result.definition_id = context.definition_id(index);
    for (auto const& mapping : source.columns()) {
        auto const& column = yugawara::binding::extract<yugawara::storage::column>(mapping.source());
        std::optional<std::uint32_t> column_index{};
        for (std::size_t i = 0; i < index.table().columns().size(); ++i) {
            if (std::addressof(index.table().columns()[i]) == std::addressof(column)) {
                column_index = static_cast<std::uint32_t>(i);
                break;
            }
        }
        result.columns.emplace_back(remote_source_column{
            std::string{column.simple_name()}, context.variable_id(mapping.destination()),
            column_index, encode_remote_type(column.type()),
        });
    }
    return result;
}

[[nodiscard]] static remote_source create_source_payload(relation::scan const& source, block_builder_context& context) {
    auto result = create_source_payload<relation::scan>(source, context);
    result.lower_endpoint_kind = endpoint_kind(source.lower().kind());
    result.lower_key_count = source.lower().keys().size();
    result.upper_endpoint_kind = endpoint_kind(source.upper().kind());
    result.upper_key_count = source.upper().keys().size();
    for (auto const& key : source.lower().keys()) {
        auto id = context.expression_id(key.value());
        if (id) {
            result.lower_key_expression_ids.emplace_back(*id);
        }
    }
    for (auto const& key : source.upper().keys()) {
        auto id = context.expression_id(key.value());
        if (id) {
            result.upper_key_expression_ids.emplace_back(*id);
        }
    }
    if (source.limit()) {
        result.limit = source.limit();
    }
    return result;
}

[[nodiscard]] static remote_source create_source_payload(relation::find const& source, block_builder_context& context) {
    auto result = create_source_payload<relation::find>(source, context);
    result.find_key_count = source.keys().size();
    for (auto const& key : source.keys()) {
        auto id = context.expression_id(key.value());
        if (id) {
            result.find_key_expression_ids.emplace_back(*id);
        }
    }
    return result;
}

[[nodiscard]] static bool create_operator_payload(relation::expression const& expression, remote_operator& op,
                                                  block_builder_context& context) {
    switch (expression.kind()) {
        case relation::expression_kind::scan:
            op.source = create_source_payload(dynamic_cast<relation::scan const&>(expression), context);
            return op.source->lower_key_expression_ids.size() == op.source->lower_key_count &&
                   op.source->upper_key_expression_ids.size() == op.source->upper_key_count;
        case relation::expression_kind::find:
            op.source = create_source_payload(dynamic_cast<relation::find const&>(expression), context);
            return op.source->find_key_expression_ids.size() == op.source->find_key_count;
        case relation::expression_kind::filter:
            op.filter_expression_id =
                context.expression_id(dynamic_cast<relation::filter const&>(expression).condition());
            return op.filter_expression_id.has_value();
        case relation::expression_kind::project:
            for (auto const& column : dynamic_cast<relation::project const&>(expression).columns()) {
                auto expression_id = context.expression_id(column.value());
                if (!expression_id) {
                    return false;
                }
                op.project_columns.emplace_back(
                    remote_project_column{context.variable_id(column.variable()), *expression_id});
            }
            return true;
        case relation::expression_kind::offer:
            return context.create_offer_payload(dynamic_cast<relation::step::offer const&>(expression), op);
        default:
            return false;
    }
}

std::optional<remote_execution_block> create_remote_execution_block(relation::graph_type const& graph,
                                                                    yugawara::compiled_info const* compiled_info,
                                                                    meta::record_meta const* result_meta,
                                                                    meta::variable_order const* result_order,
                                                                    remote_catalog_snapshot const* catalogs,
                                                                    std::string_view remote_server) {
    if (!is_remote_process_execution_block_shape(graph)) {
        return std::nullopt;
    }

    remote_execution_block block{};
    block_builder_context context{block, compiled_info, result_meta, result_order, catalogs, remote_server};
    auto const* current = first(graph);
    std::uint64_t operator_id{};
    while (current != nullptr) {
        auto const terminal = current->kind() == relation::expression_kind::offer;
        remote_operator op{
            operator_id,
            operator_kind(current->kind()),
            operator_id == 0 ? std::nullopt : std::optional<std::uint64_t>{operator_id - 1},
            terminal ? std::nullopt : std::optional<std::uint64_t>{operator_id},
            std::nullopt,
        };
        if (!create_operator_payload(*current, op, context)) {
            return std::nullopt;
        }
        if (op.source && (!op.source->definition_id ||
            std::any_of(op.source->columns.begin(), op.source->columns.end(), [](auto const& column) {
                return !column.column_index || column.type_payload.empty();
            }))) {
            return std::nullopt;
        }
        block.operators.emplace_back(std::move(op));
        if (!terminal) {
            block.values.emplace_back(operator_id);
        }
        ++operator_id;
        current = downstream(*current);
    }
    block.result_value_id = block.values.back();
    return block;
}

static void serialize_source(std::ostream& out, remote_operator const& op) {
    if (op.source) {
        out << "remote_execution_block.operator." << op.operator_id << ".source.table_name=" << op.source->table_name
            << '\n';
        out << "remote_execution_block.operator." << op.operator_id << ".source.index_name=" << op.source->index_name
            << '\n';
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.column_count=" << op.source->columns.size() << '\n';
        for (std::size_t column_id = 0; column_id < op.source->columns.size(); ++column_id) {
            auto const& column = op.source->columns[column_id];
            out << "remote_execution_block.operator." << op.operator_id << ".source.column." << column_id
                << ".name=" << column.column_name << '\n';
            out << "remote_execution_block.operator." << op.operator_id << ".source.column." << column_id
                << ".destination_variable_id=" << column.destination_variable_id << '\n';
        }
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.lower.kind=" << to_string_view(op.source->lower_endpoint_kind) << '\n';
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.lower.key_count=" << op.source->lower_key_count << '\n';
        for (std::size_t i = 0; i < op.source->lower_key_expression_ids.size(); ++i) {
            out << "remote_execution_block.operator." << op.operator_id << ".source.lower.key." << i
                << ".expression_id=" << op.source->lower_key_expression_ids[i] << '\n';
        }
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.upper.kind=" << to_string_view(op.source->upper_endpoint_kind) << '\n';
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.upper.key_count=" << op.source->upper_key_count << '\n';
        for (std::size_t i = 0; i < op.source->upper_key_expression_ids.size(); ++i) {
            out << "remote_execution_block.operator." << op.operator_id << ".source.upper.key." << i
                << ".expression_id=" << op.source->upper_key_expression_ids[i] << '\n';
        }
        out << "remote_execution_block.operator." << op.operator_id
            << ".source.find_key_count=" << op.source->find_key_count << '\n';
        for (std::size_t i = 0; i < op.source->find_key_expression_ids.size(); ++i) {
            out << "remote_execution_block.operator." << op.operator_id << ".source.find_key." << i
                << ".expression_id=" << op.source->find_key_expression_ids[i] << '\n';
        }
        out << "remote_execution_block.operator." << op.operator_id << ".source.limit=";
        if (op.source->limit) {
            out << *op.source->limit;
        } else {
            out << "none";
        }
        out << '\n';
    }
}

static void serialize_offer(std::ostream& out, remote_operator const& op) {
    if (op.offer) {
        out << "remote_execution_block.operator." << op.operator_id
            << ".offer.external_relation_id=" << op.offer->external_relation_id << '\n';
        out << "remote_execution_block.operator." << op.operator_id
            << ".offer.column_count=" << op.offer->columns.size() << '\n';
        for (std::size_t i = 0; i < op.offer->columns.size(); ++i) {
            auto const& column = op.offer->columns[i];
            out << "remote_execution_block.operator." << op.operator_id << ".offer.column." << i
                << ".source_variable_id=" << column.source_variable_id << '\n';
            out << "remote_execution_block.operator." << op.operator_id << ".offer.column." << i
                << ".destination_column_id=" << column.destination_column_id << '\n';
        }
    }
}

static void serialize_scalar_expressions(std::ostream& out, remote_execution_block const& block) {
    for (auto const& expression : block.scalar_expressions) {
        auto prefix = "remote_execution_block.scalar_expression." + std::to_string(expression.expression_id);
        out << prefix << ".kind=" << to_string_view(expression.kind) << '\n';
        if (!expression.operator_kind.empty()) {
            out << prefix << ".operator_kind=" << expression.operator_kind << '\n';
        }
        if (!expression.value_kind.empty()) {
            out << prefix << ".value_kind=" << expression.value_kind << '\n';
        }
        if (!expression.value.empty()) {
            out << prefix << ".value=" << expression.value << '\n';
        }
        if (!expression.type.empty()) {
            out << prefix << ".type=" << expression.type << '\n';
        }
        if (expression.variable_id) {
            out << prefix << ".variable_id=" << *expression.variable_id << '\n';
        }
        out << prefix << ".operand_count=" << expression.operand_expression_ids.size() << '\n';
        for (std::size_t i = 0; i < expression.operand_expression_ids.size(); ++i) {
            out << prefix << ".operand." << i << "=" << expression.operand_expression_ids[i] << '\n';
        }
    }
}

std::string serialize_remote_execution_block(remote_execution_block const& block) {
    std::ostringstream out{};
    out << "remote_execution_block.version=" << block.version << '\n';
    out << "remote_execution_block.operator_count=" << block.operators.size() << '\n';
    out << "remote_execution_block.value_count=" << block.values.size() << '\n';
    out << "remote_execution_block.stream_variable_count=" << block.stream_variables.size() << '\n';
    out << "remote_execution_block.scalar_expression_count=" << block.scalar_expressions.size() << '\n';
    out << "remote_execution_block.external_relation_count=" << block.external_relations.size() << '\n';
    out << "remote_execution_block.result_schema.column_count=" << block.result_schema.size() << '\n';
    out << "remote_execution_block.result_value_id=" << block.result_value_id << '\n';
    for (auto const& op : block.operators) {
        out << "remote_execution_block.operator." << op.operator_id << ".kind=" << to_string_view(op.kind) << '\n';
        out << "remote_execution_block.operator." << op.operator_id << ".input_value_id=";
        if (op.input_value_id) {
            out << *op.input_value_id;
        } else {
            out << "none";
        }
        out << '\n';
        out << "remote_execution_block.operator." << op.operator_id << ".output_value_id=";
        if (op.output_value_id) {
            out << *op.output_value_id;
        } else {
            out << "none";
        }
        out << '\n';
        serialize_source(out, op);
        if (op.filter_expression_id) {
            out << "remote_execution_block.operator." << op.operator_id
                << ".filter_expression_id=" << *op.filter_expression_id << '\n';
        }
        out << "remote_execution_block.operator." << op.operator_id
            << ".project_column_count=" << op.project_columns.size() << '\n';
        for (std::size_t i = 0; i < op.project_columns.size(); ++i) {
            auto const& column = op.project_columns[i];
            out << "remote_execution_block.operator." << op.operator_id << ".project_column." << i
                << ".destination_variable_id=" << column.destination_variable_id << '\n';
            out << "remote_execution_block.operator." << op.operator_id << ".project_column." << i
                << ".value_expression_id=" << column.value_expression_id << '\n';
        }
        serialize_offer(out, op);
    }
    serialize_scalar_expressions(out, block);
    for (auto const& column : block.result_schema) {
        auto prefix = "remote_execution_block.result_schema.column." + std::to_string(column.column_id);
        out << prefix << ".type=" << (column.type.empty() ? "unknown" : column.type) << '\n';
        out << prefix << ".nullable=" << (column.nullable ? "true" : "false") << '\n';
    }
    return out.str();
}

} // namespace jogasaki::dist
