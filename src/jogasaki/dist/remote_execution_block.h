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
#pragma once



#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <takatori/relation/graph.h>

namespace yugawara { class compiled_info; }
namespace jogasaki::meta { class record_meta; class variable_order; }

/**
 * @file
 * @brief portable representation of a distributed-query execution block.
 * @note The supported operator/type set is intentionally limited to existing SQL execution capabilities.
 * @todo Extend the representation and validation together; preserve wire field numbers and version rules.
 */
namespace jogasaki::dist {

enum class remote_operator_kind {
    scan,
    find,
    filter,
    project,
    offer,
};

enum class remote_endpoint_kind {
    unbound,
    inclusive,
    exclusive,
    prefixed_inclusive,
    prefixed_exclusive,
};

enum class remote_scalar_expression_kind {
    immediate,
    variable_reference,
    unary,
    cast,
    binary,
    compare,
};

struct remote_scalar_expression {
    std::uint64_t expression_id{};
    remote_scalar_expression_kind kind{};
    std::string operator_kind{};
    std::string value_kind{};
    std::string value{};
    std::string type{};
    std::optional<std::uint64_t> variable_id{};
    std::vector<std::uint64_t> operand_expression_ids{};
    std::string type_payload{};
    std::string literal_payload{};
};

struct remote_source_column {
    std::string column_name{};
    std::uint64_t destination_variable_id{};
};

struct remote_source {
    std::string table_name{};
    std::string index_name{};
    std::vector<remote_source_column> columns{};
    remote_endpoint_kind lower_endpoint_kind{};
    std::size_t lower_key_count{};
    std::vector<std::uint64_t> lower_key_expression_ids{};
    remote_endpoint_kind upper_endpoint_kind{};
    std::size_t upper_key_count{};
    std::vector<std::uint64_t> upper_key_expression_ids{};
    std::size_t find_key_count{};
    std::vector<std::uint64_t> find_key_expression_ids{};
    std::optional<std::uint64_t> limit{};
};

struct remote_project_column {
    std::uint64_t destination_variable_id{};
    std::uint64_t value_expression_id{};
};

struct remote_offer_column {
    std::uint64_t source_variable_id{};
    std::uint64_t destination_column_id{};
};

struct remote_offer {
    std::uint64_t external_relation_id{};
    std::vector<remote_offer_column> columns{};
};

struct remote_result_column {
    std::uint64_t column_id{};
    std::string type{};
    bool nullable{};
    std::string type_payload{};
};

struct remote_operator {
    std::uint64_t operator_id{};
    remote_operator_kind kind{};
    std::optional<std::uint64_t> input_value_id{};
    std::optional<std::uint64_t> output_value_id{};
    std::optional<remote_source> source{};
    std::optional<std::uint64_t> filter_expression_id{};
    std::vector<remote_project_column> project_columns{};
    std::optional<remote_offer> offer{};
};

struct remote_execution_block {
    std::uint32_t version{1};
    std::vector<remote_operator> operators{};
    std::vector<std::uint64_t> values{};
    std::vector<std::uint64_t> stream_variables{};
    std::vector<remote_scalar_expression> scalar_expressions{};
    std::vector<std::uint64_t> external_relations{};
    std::vector<remote_result_column> result_schema{};
    std::uint64_t result_value_id{};
};

/**
 * @brief creates the structural subset of a remote execution RemoteExecutionBlock.
 * @details This preserves operator order, value connections, and source
 *     storage/column/range structure and the remote execution scalar expression subset.
 *     Result schema types are populated when compiled_info is supplied.
 *     This development subset does not represent full L03 type support.
 */
[[nodiscard]] std::optional<remote_execution_block> create_remote_execution_block(
    takatori::relation::graph_type const& graph,
    yugawara::compiled_info const* compiled_info = nullptr,
    meta::record_meta const* result_meta = nullptr,
    meta::variable_order const* result_order = nullptr);

[[nodiscard]] std::string serialize_remote_execution_block(
    remote_execution_block const& block);

} // namespace jogasaki::dist
