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
#include "remote_execution_block_decoder.h"

#include <limits>
#include <memory>
#include <queue>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <absl/strings/match.h>

#include <tateyama/proto/distributed_query/remote_execution_block.pb.h>

#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>

#include "remote_type_codec.h"

namespace jogasaki::dist {

namespace dqproto = tateyama::grpc::proto;

template <class T> [[nodiscard]] static bool add_unique(std::unordered_set<T>& values, T value) {
    return values.emplace(value).second;
}

[[nodiscard]] static remote_execution_block_decode_result failure(std::string message) {
    return {{}, std::move(message)};
}

[[nodiscard]] static std::optional<remote_operator_kind> decode(dqproto::OperatorKind kind) {
    switch (kind) {
        case dqproto::OPERATOR_KIND_SCAN:
            return remote_operator_kind::scan;
        case dqproto::OPERATOR_KIND_FIND:
            return remote_operator_kind::find;
        case dqproto::OPERATOR_KIND_FILTER:
            return remote_operator_kind::filter;
        case dqproto::OPERATOR_KIND_PROJECT:
            return remote_operator_kind::project;
        case dqproto::OPERATOR_KIND_OFFER:
            return remote_operator_kind::offer;
        case dqproto::OPERATOR_KIND_UNSPECIFIED:
            break;
        default:
            break;
    }
    return {};
}

[[nodiscard]] static std::optional<remote_endpoint_kind> decode(dqproto::EndpointKind kind) {
    switch (kind) {
        case dqproto::ENDPOINT_KIND_UNBOUND:
            return remote_endpoint_kind::unbound;
        case dqproto::ENDPOINT_KIND_INCLUSIVE:
            return remote_endpoint_kind::inclusive;
        case dqproto::ENDPOINT_KIND_EXCLUSIVE:
            return remote_endpoint_kind::exclusive;
        case dqproto::ENDPOINT_KIND_PREFIXED_INCLUSIVE:
            return remote_endpoint_kind::prefixed_inclusive;
        case dqproto::ENDPOINT_KIND_PREFIXED_EXCLUSIVE:
            return remote_endpoint_kind::prefixed_exclusive;
        default:
            break;
    }
    return {};
}

[[nodiscard]] static std::optional<remote_scalar_expression_kind> decode(dqproto::ScalarExpressionKind kind) {
    switch (kind) {
        case dqproto::SCALAR_EXPRESSION_KIND_IMMEDIATE:
            return remote_scalar_expression_kind::immediate;
        case dqproto::SCALAR_EXPRESSION_KIND_VARIABLE_REFERENCE:
            return remote_scalar_expression_kind::variable_reference;
        case dqproto::SCALAR_EXPRESSION_KIND_UNARY:
            return remote_scalar_expression_kind::unary;
        case dqproto::SCALAR_EXPRESSION_KIND_CAST:
            return remote_scalar_expression_kind::cast;
        case dqproto::SCALAR_EXPRESSION_KIND_BINARY:
            return remote_scalar_expression_kind::binary;
        case dqproto::SCALAR_EXPRESSION_KIND_COMPARE:
            return remote_scalar_expression_kind::compare;
        case dqproto::SCALAR_EXPRESSION_KIND_UNSPECIFIED:
            break;
        default:
            break;
    }
    return {};
}

[[nodiscard]] static std::size_t expected_operand_count(remote_scalar_expression_kind kind) {
    switch (kind) {
        case remote_scalar_expression_kind::immediate:
        case remote_scalar_expression_kind::variable_reference:
            return 0;
        case remote_scalar_expression_kind::unary:
        case remote_scalar_expression_kind::cast:
            return 1;
        case remote_scalar_expression_kind::binary:
        case remote_scalar_expression_kind::compare:
            return 2;
    }
    return 0;
}

[[nodiscard]] static std::string id_error(char const* category, std::uint64_t id, char const* detail) {
    std::ostringstream out{};
    out << category << ' ' << id << ' ' << detail;
    return out.str();
}

namespace {
/** @brief Validate Portable IR in stages before reconstructing the execution graph. */
class block_validator {
public:
    explicit block_validator(remote_execution_block const& value) : block(std::addressof(value)) {}
    std::optional<std::string> validate() {
        if (auto error = validate_types()) {
            return error;
        }
        if (block->operators.size() < 2) {
            return "operator sequence must contain a source and an offer";
        }
        if (auto error = collect_ids()) {
            return error;
        }
        if (auto error = validate_expressions()) {
            return error;
        }
        if (auto error = validate_expression_graph()) {
            return error;
        }
        if (auto error = validate_operators()) {
            return error;
        }
        if (value_ids.count(block->result_value_id) == 0) {
            return "result value is undefined";
        }
        if (block->operators.back().input_value_id != block->result_value_id) {
            return "result value does not match offer input";
        }
        return {};
    }

private:
    std::optional<std::string> validate_types() {
        if (block->version != 2) {
            return "unsupported RemoteExecutionBlock version";
        }
        for (auto const& expression : block->scalar_expressions) {
            if (!expression.type_payload.empty()) {
                auto type = decode_remote_type(expression.type_payload);
                if (!type) {
                    return "invalid remote scalar type";
                }
                if (expression.kind == remote_scalar_expression_kind::immediate &&
                    !decode_remote_literal(expression.literal_payload, *type)) {
                    return "invalid remote literal";
                }
            }
        }
        for (auto const& column : block->result_schema) {
            if (!column.type_payload.empty() && !decode_remote_type(column.type_payload)) {
                return "invalid remote output type";
            }
        }

        return {};
    }

    std::optional<std::string> collect_ids() {
        for (auto id : block->values) {
            if (!add_unique(value_ids, id)) {
                return id_error("value", id, "is duplicated");
            }
        }
        for (auto id : block->stream_variables) {
            if (!add_unique(variable_ids, id)) {
                return id_error("stream variable", id, "is duplicated");
            }
        }
        for (auto id : block->external_relations) {
            if (!add_unique(external_relation_ids, id)) {
                return id_error("external relation", id, "is duplicated");
            }
        }
        for (auto const& column : block->result_schema) {
            if (!add_unique(result_column_ids, column.column_id)) {
                return id_error("result column", column.column_id, "is duplicated");
            }
        }

        for (auto const& expression : block->scalar_expressions) {
            if (!add_unique(expression_ids, expression.expression_id)) {
                return id_error("scalar expression", expression.expression_id, "is duplicated");
            }
        }
        return {};
    }

    std::optional<std::string> validate_expressions() {
        for (auto const& expression : block->scalar_expressions) {
            if (expression.operand_expression_ids.size() != expected_operand_count(expression.kind)) {
                return id_error("scalar expression", expression.expression_id, "has invalid operand count");
            }
            for (auto id : expression.operand_expression_ids) {
                if (expression_ids.count(id) == 0) {
                    return id_error("scalar expression operand", id, "is undefined");
                }
            }
            if (expression.kind == remote_scalar_expression_kind::variable_reference) {
                if (!expression.variable_id) {
                    return id_error("scalar expression", expression.expression_id, "has no variable");
                }
                if (variable_ids.count(*expression.variable_id) == 0) {
                    return id_error("stream variable", *expression.variable_id, "is undefined");
                }
            } else if (expression.variable_id) {
                return id_error("scalar expression", expression.expression_id, "has an unexpected variable");
            }
        }

        return {};
    }

    std::optional<std::string> validate_expression_graph() {
        // Bound recursive graph reconstruction and reject cycles before resolving expressions.
        if (block->scalar_expressions.size() > 512) {
            return "too many scalar expressions";
        }
        std::unordered_map<std::uint64_t, std::size_t> pending{};
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t>> dependents{};
        std::queue<std::uint64_t> ready{};
        for (auto const& expression : block->scalar_expressions) {
            pending[expression.expression_id] = expression.operand_expression_ids.size();
            if (expression.operand_expression_ids.empty()) {
                ready.push(expression.expression_id);
            }
            for (auto operand : expression.operand_expression_ids) {
                dependents[operand].push_back(expression.expression_id);
            }
        }
        std::size_t resolved{};
        while (!ready.empty()) {
            auto id = ready.front();
            ready.pop();
            ++resolved;
            for (auto dependent : dependents[id]) {
                if (--pending[dependent] == 0) {
                    ready.push(dependent);
                }
            }
        }
        if (resolved != block->scalar_expressions.size()) {
            return "scalar expression graph contains a cycle";
        }

        return {};
    }

    std::optional<std::string> validate_operator_shape(remote_operator const& op, std::size_t i) {
        if (!add_unique(operator_ids, op.operator_id)) {
            return id_error("operator", op.operator_id, "is duplicated");
        }
        bool const first = i == 0;
        bool const last = i + 1 == block->operators.size();
        if (first && op.kind != remote_operator_kind::scan && op.kind != remote_operator_kind::find) {
            return "the first operator must be scan or find";
        }
        if (!first && !last && op.kind != remote_operator_kind::filter && op.kind != remote_operator_kind::project) {
            return "an intermediate operator must be filter or project";
        }
        if (last && op.kind != remote_operator_kind::offer) {
            return "the last operator must be offer";
        }

        return {};
    }

    std::optional<std::string> validate_connections(remote_operator const& op, std::size_t i) {
        bool const first = i == 0;
        bool const last = i + 1 == block->operators.size();
        if (first) {
            if (op.input_value_id) {
                return "source operator must not have an input value";
            }
        } else {
            if (!op.input_value_id || value_ids.count(*op.input_value_id) == 0) {
                return id_error("operator", op.operator_id, "has an undefined input value");
            }
        }
        if (last) {
            if (op.output_value_id) {
                return "offer operator must not have an output value";
            }
        } else if (!op.output_value_id || value_ids.count(*op.output_value_id) == 0) {
            return id_error("operator", op.operator_id, "has an undefined output value");
        }
        if (i > 0 && block->operators[i - 1].output_value_id != op.input_value_id) {
            return id_error("operator", op.operator_id, "is not connected to its predecessor");
        }

        return {};
    }

    std::optional<std::string> validate_source(remote_operator const& op) {
        bool const source_kind = op.kind == remote_operator_kind::scan || op.kind == remote_operator_kind::find;
        if (source_kind != op.source.has_value()) {
            return id_error("operator", op.operator_id, "has invalid source fields");
        }
        if (op.source) {
            if (auto error = validate_source_columns(op)) {
                return error;
            }
            if (auto error = validate_source_keys(op)) {
                return error;
            }
        }
        return {};
    }

    std::optional<std::string> validate_source_columns(remote_operator const& op) {
        if (!op.source->definition_id) {
            return id_error("operator", op.operator_id, "has no storage definition ID");
        }
        if (op.source->table_name.empty() || op.source->index_name.empty()) {
            return id_error("operator", op.operator_id, "has an empty storage name");
        }
        for (auto const& column : op.source->columns) {
            if (!column.column_index || column.type_payload.empty()) {
                return id_error("operator", op.operator_id, "has no source column position or type");
            }
            if (column.column_name.empty()) {
                return id_error("operator", op.operator_id, "has an empty source column");
            }
            if (variable_ids.count(column.destination_variable_id) == 0) {
                return id_error("stream variable", column.destination_variable_id, "is undefined");
            }
        }
        return {};
    }

    std::optional<std::string> validate_source_keys(remote_operator const& op) {
        for (auto id : op.source->lower_key_expression_ids) {
            if (expression_ids.count(id) == 0) {
                return id_error("lower key expression", id, "is undefined");
            }
        }
        for (auto id : op.source->upper_key_expression_ids) {
            if (expression_ids.count(id) == 0) {
                return id_error("upper key expression", id, "is undefined");
            }
        }
        for (auto id : op.source->find_key_expression_ids) {
            if (expression_ids.count(id) == 0) {
                return id_error("find key expression", id, "is undefined");
            }
        }
        if (op.kind == remote_operator_kind::scan && !op.source->find_key_expression_ids.empty()) {
            return "scan operator must not have find keys";
        }
        if (op.kind == remote_operator_kind::find &&
            (!op.source->lower_key_expression_ids.empty() || !op.source->upper_key_expression_ids.empty() ||
             op.source->lower_endpoint_kind != remote_endpoint_kind::unbound ||
             op.source->upper_endpoint_kind != remote_endpoint_kind::unbound || op.source->limit)) {
            return "find operator must not have scan endpoints or limit";
        }
        if (op.kind == remote_operator_kind::scan &&
            ((op.source->lower_endpoint_kind == remote_endpoint_kind::unbound &&
              !op.source->lower_key_expression_ids.empty()) ||
             (op.source->upper_endpoint_kind == remote_endpoint_kind::unbound &&
              !op.source->upper_key_expression_ids.empty()))) {
            return "unbound scan endpoint must not have keys";
        }
        return {};
    }

    std::optional<std::string> validate_filter_project(remote_operator const& op) {
        if (op.kind == remote_operator_kind::filter) {
            if (!op.filter_expression_id || expression_ids.count(*op.filter_expression_id) == 0) {
                return id_error("operator", op.operator_id, "has an undefined filter expression");
            }
        } else if (op.filter_expression_id) {
            return id_error("operator", op.operator_id, "has an unexpected filter expression");
        }
        if (op.kind != remote_operator_kind::project && !op.project_columns.empty()) {
            return id_error("operator", op.operator_id, "has unexpected project columns");
        }
        for (auto const& column : op.project_columns) {
            if (variable_ids.count(column.destination_variable_id) == 0) {
                return id_error("stream variable", column.destination_variable_id, "is undefined");
            }
            if (expression_ids.count(column.value_expression_id) == 0) {
                return id_error("project expression", column.value_expression_id, "is undefined");
            }
        }
        return {};
    }

    std::optional<std::string> validate_offer(remote_operator const& op) {
        if ((op.kind == remote_operator_kind::offer) != op.offer.has_value()) {
            return id_error("operator", op.operator_id, "has invalid offer fields");
        }
        if (op.offer) {
            if (external_relation_ids.count(op.offer->external_relation_id) == 0) {
                return id_error("external relation", op.offer->external_relation_id, "is undefined");
            }
            std::unordered_set<std::uint64_t> offered_columns{};
            for (auto const& column : op.offer->columns) {
                if (!offered_columns.emplace(column.destination_column_id).second) {
                    return "duplicate offer destination column";
                }
                if (variable_ids.count(column.source_variable_id) == 0) {
                    return id_error("stream variable", column.source_variable_id, "is undefined");
                }
                if (result_column_ids.count(column.destination_column_id) == 0) {
                    return id_error("result column", column.destination_column_id, "is undefined");
                }
            }
            if (offered_columns.size() != result_column_ids.size()) {
                return "offer does not bind every result column";
            }
        }
        return {};
    }

    std::optional<std::string> validate_operators() {
        for (std::size_t i = 0; i < block->operators.size(); ++i) {
            auto const& op = block->operators[i];
            if (auto error = validate_operator_shape(op, i)) {
                return error;
            }
            if (auto error = validate_connections(op, i)) {
                return error;
            }
            if (auto error = validate_source(op)) {
                return error;
            }
            if (auto error = validate_filter_project(op)) {
                return error;
            }
            if (auto error = validate_offer(op)) {
                return error;
            }
        }
        return {};
    }
    remote_execution_block const* block;
    std::unordered_set<std::uint64_t> value_ids{};
    std::unordered_set<std::uint64_t> variable_ids{};
    std::unordered_set<std::uint64_t> external_relation_ids{};
    std::unordered_set<std::uint64_t> result_column_ids{};
    std::unordered_set<std::uint64_t> expression_ids{};
    std::unordered_set<std::uint64_t> operator_ids{};
};
} // namespace

std::optional<std::string> validate_remote_execution_block(remote_execution_block const& block) {
    return block_validator{block}.validate();
}

static bool unsupported_type(std::string const& payload, std::string const& name) {

    proto::distributed_query::PortableType type{};
    if (!payload.empty()) {
        if (payload.size() > 4096 || !type.ParseFromString(payload)) {
            return false;
        }
        return type.kind() == "int1" || type.kind() == "int2" || type.kind() == "bit" ||
               type.kind() == "datetime_interval";
    }
    return name == "int1" || absl::StartsWith(name, "int1(") || name == "int2" || absl::StartsWith(name, "int2(") ||
           name == "bit" || absl::StartsWith(name, "bit(") || name == "datetime_interval" ||
           absl::StartsWith(name, "datetime_interval(");
}

static std::optional<std::string> decode_operator(dqproto::Operator const& op, remote_operator& target) {
    auto kind = decode(op.kind());
    if (!kind) {
        return id_error("operator", op.operator_id(), "has unknown kind");
    }
    target.operator_id = op.operator_id();
    target.kind = *kind;
    if (op.input_value_id_presence_case() == dqproto::Operator::kInputValueId) {
        target.input_value_id = op.input_value_id();
    }
    if (op.output_value_id_presence_case() == dqproto::Operator::kOutputValueId) {
        target.output_value_id = op.output_value_id();
    }
    if (op.has_source()) {
        auto lower_kind = decode(op.source().lower().kind());
        auto upper_kind = decode(op.source().upper().kind());
        if (!lower_kind || !upper_kind) {
            return id_error("operator", op.operator_id(), "has unknown endpoint kind");
        }
        remote_source decoded_source{};
        decoded_source.table_name = op.source().storage().table_name();
        decoded_source.index_name = op.source().storage().index_name();
        if (op.source().storage().definition_id_presence_case() == dqproto::StorageReference::kDefinitionId) {
            decoded_source.definition_id = op.source().storage().definition_id();
        }
        decoded_source.lower_endpoint_kind = *lower_kind;
        decoded_source.upper_endpoint_kind = *upper_kind;
        decoded_source.lower_key_expression_ids.assign(op.source().lower().key_expression_ids().begin(),
                                                       op.source().lower().key_expression_ids().end());
        decoded_source.upper_key_expression_ids.assign(op.source().upper().key_expression_ids().begin(),
                                                       op.source().upper().key_expression_ids().end());
        decoded_source.find_key_expression_ids.assign(op.source().find_key_expression_ids().begin(),
                                                      op.source().find_key_expression_ids().end());
        decoded_source.lower_key_count = decoded_source.lower_key_expression_ids.size();
        decoded_source.upper_key_count = decoded_source.upper_key_expression_ids.size();
        decoded_source.find_key_count = decoded_source.find_key_expression_ids.size();
        if (op.source().limit_presence_case() == dqproto::Source::kLimit) {
            decoded_source.limit = op.source().limit();
        }
        for (auto const& column : op.source().columns()) {
            decoded_source.columns.push_back({column.column_name(), column.destination_variable_id(),
                column.column_index_presence_case() == dqproto::SourceColumn::kColumnIndex
                    ? std::optional<std::uint32_t>{column.column_index()} : std::nullopt,
                column.type_payload()});
        }
        target.source = std::move(decoded_source);
    }
    if (op.filter_expression_id_presence_case() == dqproto::Operator::kFilterExpressionId) {
        target.filter_expression_id = op.filter_expression_id();
    }
    for (auto const& column : op.project_columns()) {
        target.project_columns.push_back({column.destination_variable_id(), column.value_expression_id()});
    }
    if (op.has_offer()) {
        remote_offer offer{};
        offer.external_relation_id = op.offer().external_relation_id();
        for (auto const& column : op.offer().columns()) {
            offer.columns.push_back({column.source_variable_id(), column.destination_column_id()});
        }
        target.offer = std::move(offer);
    }
    return {};
}

remote_execution_block_decode_result decode_remote_execution_block(std::string_view payload) {
    if (payload.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return failure("RemoteExecutionBlock payload is too large");
    }
    dqproto::RemoteExecutionBlock source{};
    if (!source.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        return failure("invalid RemoteExecutionBlock protobuf payload");
    }

    // Reject unsupported SQL types before creating runtime metadata, including typed NULLs.
    for (auto const& expression : source.scalar_expressions()) {
        if (unsupported_type(expression.type_payload(), expression.type())) {
            return {{}, "unsupported SQL type is outside remote execution", true};
        }
    }
    for (auto const& column : source.result_schema().columns()) {
        if (unsupported_type(column.type_payload(), column.type())) {
            return {{}, "unsupported SQL type is outside remote execution", true};
        }
    }

    remote_execution_block block{};
    block.version = source.version();
    block.result_value_id = source.result_value_id();
    block.values.assign(source.value_ids().begin(), source.value_ids().end());
    block.stream_variables.assign(source.stream_variable_ids().begin(), source.stream_variable_ids().end());
    block.external_relations.assign(source.external_relation_ids().begin(), source.external_relation_ids().end());

    for (auto const& expression : source.scalar_expressions()) {
        auto kind = decode(expression.kind());
        if (!kind) {
            return failure(id_error("scalar expression", expression.expression_id(), "has unknown kind"));
        }
        remote_scalar_expression target{};
        target.expression_id = expression.expression_id();
        target.kind = *kind;
        target.operator_kind = expression.operator_kind();
        target.value_kind = expression.value_kind();
        target.value = expression.value();
        target.type = expression.type();
        target.type_payload = expression.type_payload();
        target.literal_payload = expression.literal_payload();
        if (expression.variable_id_presence_case() == dqproto::ScalarExpression::kVariableId) {
            target.variable_id = expression.variable_id();
        }
        target.operand_expression_ids.assign(expression.operand_expression_ids().begin(),
                                             expression.operand_expression_ids().end());
        block.scalar_expressions.emplace_back(std::move(target));
    }

    for (auto const& op : source.operators()) {
        remote_operator target{};
        if (auto error = decode_operator(op, target)) {
            return failure(std::move(*error));
        }
        block.operators.emplace_back(std::move(target));
    }
    for (auto const& column : source.result_schema().columns()) {
        block.result_schema.push_back({column.column_id(), column.type(), column.nullable(), column.type_payload()});
    }

    if (auto error = validate_remote_execution_block(block)) {
        return failure(std::move(*error));
    }
    return {std::move(block), {}};
}

} // namespace jogasaki::dist
