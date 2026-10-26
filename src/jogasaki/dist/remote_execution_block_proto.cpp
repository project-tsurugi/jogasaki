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
#include "remote_execution_block_proto.h"

#include <cstdlib>

#include <tateyama/proto/distributed_query/remote_execution_block.pb.h>

namespace jogasaki::dist {

namespace dqproto = tateyama::grpc::proto;

[[nodiscard]] static dqproto::OperatorKind encode(remote_operator_kind kind) {
    switch (kind) {
        case remote_operator_kind::scan:
            return dqproto::OPERATOR_KIND_SCAN;
        case remote_operator_kind::find:
            return dqproto::OPERATOR_KIND_FIND;
        case remote_operator_kind::filter:
            return dqproto::OPERATOR_KIND_FILTER;
        case remote_operator_kind::project:
            return dqproto::OPERATOR_KIND_PROJECT;
        case remote_operator_kind::offer:
            return dqproto::OPERATOR_KIND_OFFER;
    }
    std::abort();
}

[[nodiscard]] static dqproto::EndpointKind encode(remote_endpoint_kind kind) {
    switch (kind) {
        case remote_endpoint_kind::unbound:
            return dqproto::ENDPOINT_KIND_UNBOUND;
        case remote_endpoint_kind::inclusive:
            return dqproto::ENDPOINT_KIND_INCLUSIVE;
        case remote_endpoint_kind::exclusive:
            return dqproto::ENDPOINT_KIND_EXCLUSIVE;
        case remote_endpoint_kind::prefixed_inclusive:
            return dqproto::ENDPOINT_KIND_PREFIXED_INCLUSIVE;
        case remote_endpoint_kind::prefixed_exclusive:
            return dqproto::ENDPOINT_KIND_PREFIXED_EXCLUSIVE;
    }
    std::abort();
}

[[nodiscard]] static dqproto::ScalarExpressionKind encode(remote_scalar_expression_kind kind) {
    switch (kind) {
        case remote_scalar_expression_kind::immediate:
            return dqproto::SCALAR_EXPRESSION_KIND_IMMEDIATE;
        case remote_scalar_expression_kind::variable_reference:
            return dqproto::SCALAR_EXPRESSION_KIND_VARIABLE_REFERENCE;
        case remote_scalar_expression_kind::unary:
            return dqproto::SCALAR_EXPRESSION_KIND_UNARY;
        case remote_scalar_expression_kind::cast:
            return dqproto::SCALAR_EXPRESSION_KIND_CAST;
        case remote_scalar_expression_kind::binary:
            return dqproto::SCALAR_EXPRESSION_KIND_BINARY;
        case remote_scalar_expression_kind::compare:
            return dqproto::SCALAR_EXPRESSION_KIND_COMPARE;
    }
    std::abort();
}

static void encode_source(remote_source const& source, dqproto::Source& out) {
    out.mutable_storage()->set_table_name(source.table_name);
    out.mutable_storage()->set_index_name(source.index_name);
    if (source.definition_id) {
        out.mutable_storage()->set_definition_id(*source.definition_id);
    }
    for (auto const& column : source.columns) {
        auto* target = out.add_columns();
        target->set_column_name(column.column_name);
        target->set_destination_variable_id(column.destination_variable_id);
        if (column.column_index) {
            target->set_column_index(*column.column_index);
        }
        target->set_type_payload(column.type_payload);
    }
    out.mutable_lower()->set_kind(encode(source.lower_endpoint_kind));
    for (auto id : source.lower_key_expression_ids) {
        out.mutable_lower()->add_key_expression_ids(id);
    }
    out.mutable_upper()->set_kind(encode(source.upper_endpoint_kind));
    for (auto id : source.upper_key_expression_ids) {
        out.mutable_upper()->add_key_expression_ids(id);
    }
    for (auto id : source.find_key_expression_ids) {
        out.add_find_key_expression_ids(id);
    }
    if (source.limit) {
        out.set_limit(*source.limit);
    }
}

dqproto::RemoteExecutionBlock encode_remote_execution_block(remote_execution_block const& block) {
    dqproto::RemoteExecutionBlock out{};
    out.set_version(block.version);
    out.set_result_value_id(block.result_value_id);
    for (auto id : block.values) {
        out.add_value_ids(id);
    }
    for (auto id : block.stream_variables) {
        out.add_stream_variable_ids(id);
    }
    for (auto id : block.external_relations) {
        out.add_external_relation_ids(id);
    }

    for (auto const& op : block.operators) {
        auto* target = out.add_operators();
        target->set_operator_id(op.operator_id);
        target->set_kind(encode(op.kind));
        if (op.input_value_id) {
            target->set_input_value_id(*op.input_value_id);
        }
        if (op.output_value_id) {
            target->set_output_value_id(*op.output_value_id);
        }
        if (op.source) {
            encode_source(*op.source, *target->mutable_source());
        }
        if (op.filter_expression_id) {
            target->set_filter_expression_id(*op.filter_expression_id);
        }
        for (auto const& column : op.project_columns) {
            auto* c = target->add_project_columns();
            c->set_destination_variable_id(column.destination_variable_id);
            c->set_value_expression_id(column.value_expression_id);
        }
        if (op.offer) {
            auto* offer = target->mutable_offer();
            offer->set_external_relation_id(op.offer->external_relation_id);
            for (auto const& column : op.offer->columns) {
                auto* c = offer->add_columns();
                c->set_source_variable_id(column.source_variable_id);
                c->set_destination_column_id(column.destination_column_id);
            }
        }
    }

    for (auto const& expression : block.scalar_expressions) {
        auto* target = out.add_scalar_expressions();
        target->set_expression_id(expression.expression_id);
        target->set_kind(encode(expression.kind));
        target->set_operator_kind(expression.operator_kind);
        target->set_value_kind(expression.value_kind);
        target->set_value(expression.value);
        target->set_type(expression.type);
        target->set_type_payload(expression.type_payload);
        target->set_literal_payload(expression.literal_payload);
        if (expression.variable_id) {
            target->set_variable_id(*expression.variable_id);
        }
        for (auto id : expression.operand_expression_ids) {
            target->add_operand_expression_ids(id);
        }
    }

    for (auto const& column : block.result_schema) {
        auto* target = out.mutable_result_schema()->add_columns();
        target->set_column_id(column.column_id);
        target->set_type(column.type);
        target->set_nullable(column.nullable);
        target->set_type_payload(column.type_payload);
    }
    return out;
}

std::string serialize_remote_execution_block_proto(remote_execution_block const& block) {
    return encode_remote_execution_block(block).SerializeAsString();
}

} // namespace jogasaki::dist
