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


#include <algorithm>
#include <array>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/plan/process.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/find.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/values.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/primitive.h>
#include <takatori/type/character.h>
#include <takatori/util/rvalue_reference_wrapper.h>
#include <takatori/value/primitive.h>
#include <takatori/value/character.h>
#include <yugawara/binding/external_relation_info.h>
#include <yugawara/binding/extract.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <tateyama/proto/distributed_query/remote_execution_block.pb.h>

#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/dist/remote_execution_block.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_execution_block_decoder.h>
#include <jogasaki/dist/remote_execution_block_proto.h>
#include <jogasaki/dist/remote_execution_graph_builder.h>
#include <jogasaki/dist/remote_execution_graph_compiler.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_process_execution_block_shape.h>
#include <jogasaki/dist/remote_routing.h>
#include <jogasaki/executor/process/process_preparation.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/variable_order.h>

namespace jogasaki::testing {

namespace {

namespace relation = takatori::relation;
namespace scalar = takatori::scalar;
namespace type = takatori::type;
namespace value = takatori::value;

TEST(remote_execution_result_test, round_trips_multiple_rows_columns_and_null) {
    dist::remote_execution_result source{
        {{0, "int8()", false, dist::encode_remote_type(type::int8{})},
         {1, "int8()", true, dist::encode_remote_type(type::int8{})}},
        {{10, 20}, {30, std::nullopt}},
    };

    auto payload = dist::serialize_remote_execution_result(source);
    std::string error{};
    auto result = dist::deserialize_remote_execution_result(payload, error);

    ASSERT_TRUE(result) << error;
    ASSERT_EQ(2, result->columns.size());
    ASSERT_EQ(2, result->rows.size());
    EXPECT_EQ(10, result->rows[0][0]);
    EXPECT_EQ(20, result->rows[0][1]);
    EXPECT_EQ(30, result->rows[1][0]);
    EXPECT_FALSE(result->rows[1][1]);
}

TEST(remote_execution_result_test, does_not_supply_a_missing_type_payload) {
    dist::remote_execution_result source{{{0, "int8()", false}}, {{7}}};
    auto payload = dist::serialize_remote_execution_result(source);
    std::string error{};
    EXPECT_FALSE(dist::deserialize_remote_execution_result(payload, error));
    EXPECT_EQ("invalid remote result schema", error);
}

class test_graph_resolver final : public dist::remote_execution_graph_resolver {
public:
    test_graph_resolver(
        takatori::descriptor::relation storage,
        takatori::descriptor::variable storage_column,
        takatori::descriptor::relation external_relation,
        takatori::descriptor::variable external_column
    ) : storage_(std::move(storage)),
        storage_column_(std::move(storage_column)),
        external_relation_(std::move(external_relation)),
        external_column_(std::move(external_column))
    {}

    std::optional<takatori::descriptor::relation> resolve_storage(
        dist::remote_source const&) const override {
        return storage_;
    }

    std::optional<takatori::descriptor::variable> resolve_storage_column(
        dist::remote_source const&,
        dist::remote_source_column const&) const override {
        return storage_column_;
    }

    std::optional<takatori::descriptor::variable> resolve_storage_key_column(
        dist::remote_source const&,
        std::size_t) const override {
        return storage_column_;
    }

    std::optional<takatori::descriptor::relation> resolve_external_relation(
        std::uint64_t) const override {
        return external_relation_;
    }

    std::optional<takatori::descriptor::variable> resolve_external_column(
        std::uint64_t,
        std::uint64_t) const override {
        return external_column_;
    }

    std::unique_ptr<scalar::expression> resolve_immediate(
        dist::remote_scalar_expression const& expression) const override {
        auto type = dist::decode_remote_type(expression.type_payload);
        if (!type) return {};
        auto value = dist::decode_remote_literal(expression.literal_payload, *type);
        if (!value) return {};
        return std::make_unique<scalar::immediate>(std::move(value), std::move(type));
    }

private:
    takatori::descriptor::relation storage_;
    takatori::descriptor::variable storage_column_;
    takatori::descriptor::relation external_relation_;
    takatori::descriptor::variable external_column_;
};

TEST(remote_execution_block_test, rejects_multiple_remote_processes_for_one_execution) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    takatori::plan::graph_type plan{};
    auto add = [&] {
        auto& process = plan.insert(takatori::plan::process{});
        auto variable = bindings.stream_variable();
        auto& scan = process.operators().insert(relation::scan{bindings(*index), {{bindings(table->columns()[0]), variable}}, {}, {}});
        auto& forward = plan.insert(takatori::plan::forward{bindings.exchange_column()});
        auto& offer = process.operators().insert(relation::step::offer{bindings.exchange(forward), {{variable, forward.columns()[0]}}});
        scan.output() >> offer.input();
        process >> forward;
    };
    add();
    dist::remote_catalog_snapshot catalogs{{}, {{table.get(), "worker"}}, {{index.get(), {"worker", dist::remote_declaration_kind::index, 1257}}}};
    EXPECT_TRUE(dist::validate_remote_plan(plan, &catalogs).empty());
    status code{};
    EXPECT_NE(std::string::npos, dist::validate_remote_plan(plan, nullptr, "worker", &code).find("catalog snapshot"));
    EXPECT_EQ(status::err_compiler_error, code);
    add();
    EXPECT_NE(std::string::npos, dist::validate_remote_plan(plan, nullptr, "worker", &code).find("catalog snapshot"));
    EXPECT_EQ(status::err_compiler_error, code);
    EXPECT_NE(std::string::npos, dist::validate_remote_plan(plan, &catalogs).find("at most one"));
    // Without remote metadata or an explicit override, both ordinary SQL processes are valid.
    EXPECT_TRUE(dist::validate_remote_plan(plan, nullptr).empty());
}
TEST(remote_execution_block_test, common_preparation_requires_processor_and_relation_map) {
    using namespace executor::process;
    auto processor = std::make_shared<processor_info>();
    auto relations = std::make_shared<relation_io_map>();
    EXPECT_THROW((void)prepare_process({}, relations), std::logic_error);
    EXPECT_THROW((void)prepare_process(processor, {}), std::logic_error);
    EXPECT_THROW((void)prepare_process({}, {}), std::logic_error);
}
TEST(remote_execution_block_test, common_preparation_requires_variables_and_blocks_together) {
    using namespace executor::process;
    relation::graph_type graph{};
    yugawara::compiled_info info{};
    auto variables = std::make_shared<impl::variables_info_list>();
    auto blocks = std::make_shared<impl::block_indices>();
    EXPECT_THROW((void)prepare_processor(graph, info, nullptr, variables, {}), std::logic_error);
    EXPECT_THROW((void)prepare_processor(graph, info, nullptr, {}, blocks), std::logic_error);
    auto processor = prepare_processor(graph, info, nullptr, variables, blocks);
    ASSERT_TRUE(processor);
    EXPECT_EQ(variables.get(), std::addressof(processor->vars_info_list()));
    EXPECT_EQ(blocks.get(), std::addressof(processor->block_indices()));
    EXPECT_EQ(std::addressof(graph), std::addressof(processor->relations()));
}
TEST(remote_execution_block_test, common_preparation_initializes_once_before_activation) {
    using namespace executor::process;
    auto process = prepare_process(std::make_shared<processor_info>(), std::make_shared<relation_io_map>());
    auto exchanges = std::make_shared<io_exchange_map>();
    ASSERT_FALSE(process.io_info());
    bind_process_io(process, exchanges);
    ASSERT_TRUE(process.io_info());
    EXPECT_EQ(exchanges, process.io_exchange_map());
    EXPECT_THROW(bind_process_io(process, exchanges), std::logic_error);
}
TEST(remote_execution_block_test, common_preparation_rejects_inconsistent_runtime_binding) {
    using namespace executor::process;
    auto processor = std::make_shared<processor_info>();
    auto relations = std::make_shared<relation_io_map>();
    auto process = prepare_process(processor, relations);
    auto exchanges = std::make_shared<io_exchange_map>();
    exchanges->add_output(0, nullptr);
    EXPECT_THROW(bind_process_io(process, exchanges), std::logic_error);
}
TEST(remote_execution_block_test, common_preparation_rejects_unbound_slot_before_io_metadata) {
    using namespace executor::process;
    takatori::plan::forward output{};
    yugawara::binding::factory bindings{};
    takatori::relation::step::offer offer{bindings.exchange(output), std::vector<takatori::relation::step::offer::column>{}};
    auto relations = std::make_shared<relation_io_map>(relation_io_map::input_entity_type{}, relation_io_map::output_entity_type{{&offer, 0}});
    auto process = prepare_process(std::make_shared<processor_info>(), relations);
    auto exchanges = std::make_shared<io_exchange_map>();
    exchanges->add_output(0, nullptr);
    EXPECT_THROW(bind_process_io(process, exchanges), std::logic_error);
}
TEST(remote_execution_block_test, offer_wire_order_matches_downstream_columns) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}, {"C1", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto first = bindings.stream_variable(); auto second = bindings.stream_variable();
    auto output_first = bindings.exchange_column(); auto output_second = bindings.exchange_column();
    takatori::plan::forward forward{output_first, output_second};
    relation::graph_type graph{};
    auto& scan = graph.insert(relation::scan{bindings(*index),
        {{bindings(table->columns()[0]), first}, {bindings(table->columns()[1]), second}}, {}, {}});
    auto& offer = graph.insert(relation::step::offer{bindings.exchange(forward),
        {{second, output_second}, {first, output_first}}});
    scan.output() >> offer.input();
    meta::variable_order order{meta::variable_ordering_enum_tag<meta::variable_ordering_kind::flat_record>, forward.columns()};
    auto block = dist::create_remote_execution_block(graph, nullptr, nullptr, &order);
    ASSERT_TRUE(block);
    ASSERT_EQ(2, block->result_schema.size());
    EXPECT_EQ(0, block->result_schema[0].column_id);
    EXPECT_EQ(1, block->result_schema[1].column_id);
    EXPECT_EQ(1, block->operators.back().offer->columns[0].destination_column_id);
    EXPECT_EQ(0, block->operators.back().offer->columns[1].destination_column_id);
}
TEST(remote_execution_block_test, creates_ordered_operator_and_value_graph) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");

    relation::graph_type graph{};
    auto lower = relation::scan::endpoint{
        relation::scan::key{
            bindings(table->columns()[0]),
            scalar::immediate{value::int8{10}, type::int8{}},
        },
        relation::endpoint_kind::inclusive,
    };
    auto& scan = graph.insert(relation::scan{
        bindings(*index),
        {{bindings(table->columns()[0]), c0}},
        std::move(lower),
        {},
        {},
        {{}, 25},
    });
    auto& filter = graph.insert(relation::filter{scalar::compare{
        scalar::comparison_operator::greater,
        scalar::binary{
            scalar::binary_operator::add,
            scalar::variable_reference{c0},
            scalar::immediate{value::int8{1}, type::int8{}},
        },
        scalar::immediate{value::int8{10}, type::int8{}},
    }});
    using column = relation::project::column;
    auto& project = graph.insert(relation::project{
        std::initializer_list<takatori::util::rvalue_reference_wrapper<column>>{
            column{
                c1,
                scalar::cast{
                    type::int8{},
                    scalar::cast_loss_policy::error,
                    scalar::unary{
                        scalar::unary_operator::plus,
                        scalar::variable_reference{c0},
                    },
                },
            },
        },
    });
    takatori::plan::forward forward{bindings.exchange_column()};
    auto& offer = graph.insert(relation::step::offer{
        bindings.exchange(forward),
        {{c0, forward.columns()[0]}},
    });
    scan.output() >> filter.input();
    filter.output() >> project.input();
    project.output() >> offer.input();

    auto block = dist::create_remote_execution_block(graph);

    ASSERT_TRUE(block);
    ASSERT_EQ(1, block->result_schema.size());
    // This graph has one BIGINT output; no compiler type map is supplied by this fixture.
    block->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    EXPECT_EQ(3, block->version);
    ASSERT_EQ(4, block->operators.size());
    EXPECT_EQ(dist::remote_operator_kind::scan, block->operators[0].kind);
    EXPECT_EQ(dist::remote_operator_kind::filter, block->operators[1].kind);
    EXPECT_EQ(dist::remote_operator_kind::project, block->operators[2].kind);
    EXPECT_EQ(dist::remote_operator_kind::offer, block->operators[3].kind);
    EXPECT_FALSE(block->operators[0].input_value_id);
    EXPECT_EQ(0, block->operators[0].output_value_id);
    EXPECT_EQ(2, block->operators[3].input_value_id);
    EXPECT_FALSE(block->operators[3].output_value_id);
    EXPECT_EQ((std::vector<std::uint64_t>{0, 1, 2}), block->values);
    EXPECT_EQ(2, block->result_value_id);
    EXPECT_EQ((std::vector<std::uint64_t>{0, 1}), block->stream_variables);

    ASSERT_TRUE(block->operators[0].source);
    auto const& source = *block->operators[0].source;
    EXPECT_EQ("T0", source.table_name);
    EXPECT_EQ("I0", source.index_name);
    ASSERT_EQ(1, source.columns.size());
    EXPECT_EQ("C0", source.columns[0].column_name);
    EXPECT_EQ(0, source.columns[0].destination_variable_id);
    EXPECT_EQ(dist::remote_endpoint_kind::inclusive, source.lower_endpoint_kind);
    EXPECT_EQ(1, source.lower_key_count);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, source.upper_endpoint_kind);
    EXPECT_EQ(0, source.upper_key_count);
    EXPECT_EQ(0, source.find_key_count);
    ASSERT_TRUE(source.limit);
    EXPECT_EQ(25, *source.limit);
    ASSERT_EQ(1, source.lower_key_expression_indexes.size());
    EXPECT_EQ(0, source.lower_key_expression_indexes[0]);

    ASSERT_EQ(9, block->scalar_expressions.size());
    EXPECT_EQ(dist::remote_scalar_expression_kind::immediate, block->scalar_expressions[0].kind);
    EXPECT_EQ(dist::remote_scalar_expression_kind::variable_reference, block->scalar_expressions[1].kind);
    EXPECT_EQ(0, block->scalar_expressions[1].variable_id);
    EXPECT_EQ(dist::remote_scalar_expression_kind::binary, block->scalar_expressions[3].kind);
    EXPECT_EQ("add", block->scalar_expressions[3].operator_kind);
    EXPECT_EQ((std::vector<std::uint64_t>{1, 2}), block->scalar_expressions[3].operand_expression_indexes);
    EXPECT_EQ(dist::remote_scalar_expression_kind::compare, block->scalar_expressions[5].kind);
    EXPECT_EQ("greater", block->scalar_expressions[5].operator_kind);
    EXPECT_EQ(5, block->operators[1].filter_expression_index);
    EXPECT_EQ(dist::remote_scalar_expression_kind::unary, block->scalar_expressions[7].kind);
    EXPECT_EQ(dist::remote_scalar_expression_kind::cast, block->scalar_expressions[8].kind);
    ASSERT_EQ(1, block->operators[2].project_columns.size());
    EXPECT_EQ(1, block->operators[2].project_columns[0].destination_variable_id);
    EXPECT_EQ(8, block->operators[2].project_columns[0].expression_index);
    ASSERT_TRUE(block->operators[3].offer);
    EXPECT_EQ(0, block->operators[3].offer->external_relation_id);
    ASSERT_EQ(1, block->operators[3].offer->columns.size());
    EXPECT_EQ(0, block->operators[3].offer->columns[0].source_variable_id);
    EXPECT_EQ(0, block->operators[3].offer->columns[0].destination_column_id);
    EXPECT_EQ((std::vector<std::uint64_t>{0}), block->external_relations);
    ASSERT_EQ(1, block->result_schema.size());
    EXPECT_EQ(0, block->result_schema[0].column_id);
    EXPECT_TRUE(block->result_schema[0].type.empty());
    EXPECT_FALSE(block->result_schema[0].nullable);

    auto serialized = dist::serialize_remote_execution_block(*block);
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.version=3\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.2.kind=project\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.result_value_id=2\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.table_name=T0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.column.0.name=C0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.lower.kind=inclusive\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.lower.key.0.expression_index=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.limit=25\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.1.filter_expression_index=5\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.scalar_expression.3.operator_kind=add\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.scalar_expression.8.kind=cast\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.3.offer.external_relation_id=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.3.offer.column.0.source_variable_id=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.result_schema.column.0.type=unknown\n"));

    ASSERT_TRUE(block->operators.front().source->definition_id);
    EXPECT_EQ(1257U, *block->operators.front().source->definition_id);
    ASSERT_EQ(1U, block->operators.front().source->columns.size());
    EXPECT_EQ(std::optional<std::uint32_t>{0}, block->operators.front().source->columns[0].column_index);
    EXPECT_EQ(dist::encode_remote_type(type::int8{}), block->operators.front().source->columns[0].type_payload);
    auto bytes = dist::serialize_remote_execution_block_proto(*block);
    tateyama::grpc::proto::RemoteExecutionBlock decoded{};
    ASSERT_TRUE(decoded.ParseFromString(bytes));
    EXPECT_EQ(3, decoded.version());
    ASSERT_TRUE(!decoded.operators(0).source().storage().remote_definition_id().empty());
    EXPECT_EQ("1257", decoded.operators(0).source().storage().remote_definition_id());
    ASSERT_TRUE(decoded.operators(0).source().columns(0).has_column_index());
    EXPECT_EQ(0U, decoded.operators(0).source().columns(0).column_index());
    ASSERT_EQ(4, decoded.operators_size());
    EXPECT_EQ(tateyama::grpc::proto::OPERATOR_KIND_SCAN, decoded.operators(0).kind());
    EXPECT_EQ("T0", decoded.operators(0).source().storage().table_name());
    EXPECT_EQ(tateyama::grpc::proto::OPERATOR_KIND_OFFER, decoded.operators(3).kind());
    EXPECT_EQ(0, decoded.operators(3).offer().external_relation_id());
    ASSERT_EQ(9, decoded.scalar_expressions_size());
    EXPECT_EQ(tateyama::grpc::proto::SCALAR_EXPRESSION_KIND_COMPARE,
        decoded.scalar_expressions(5).kind());
    ASSERT_EQ(1, decoded.result_schema().columns_size());
    EXPECT_EQ(0, decoded.result_schema().columns(0).column_id());

    auto decoded_block = dist::decode_remote_execution_block(bytes);
    ASSERT_TRUE(decoded_block) << decoded_block.error;
    ASSERT_EQ(4, decoded_block.block->operators.size());
    EXPECT_EQ("T0", decoded_block.block->operators.front().source->table_name);
    EXPECT_EQ(2, decoded_block.block->result_value_id);
    block->operators.front().source->definition_id = 0;
    auto zero = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(*block));
    ASSERT_TRUE(zero) << zero.error;
    EXPECT_EQ(std::optional<std::uint64_t>{0}, zero.block->operators.front().source->definition_id);
    EXPECT_EQ(std::optional<std::uint32_t>{0}, zero.block->operators.front().source->columns[0].column_index);
    scan.row_slice() = relation::scan::row_slice_type{1, 25};
    EXPECT_FALSE(dist::create_remote_execution_block(graph));

}

TEST(remote_execution_block_test, distinguishes_generation_failures_without_publishing_partial_blocks) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1}, table, "T"});
    auto input = bindings.stream_variable("input");
    auto column = bindings.exchange_column("output");
    takatori::plan::forward output{column};
    relation::graph_type graph{};
    auto& scan = graph.insert(relation::scan{bindings(*index), {{bindings(table->columns()[0]), input}}, {}, {}});
    auto& offer = graph.insert(relation::step::offer{bindings.exchange(output), {{input, column}}});
    scan.output() >> offer.input();
    dist::remote_execution_block_generation_error error{};
    auto generate = [&] { return dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, nullptr, {}, &error); };
    ASSERT_TRUE(generate());
    EXPECT_EQ(status::ok, error.code);
    scan.row_slice() = relation::scan::row_slice_type{1, 25};
    EXPECT_FALSE(generate());
    EXPECT_EQ(status::err_unsupported, error.code);
    scan.row_slice() = relation::scan::row_slice_type{};
    offer.columns().emplace_back(input, column);
    EXPECT_FALSE(generate());
    EXPECT_EQ(status::err_invalid_argument, error.code);
    offer.columns().pop_back();
    offer.columns().clear();
    offer.columns().emplace_back(bindings.stream_variable("undefined"), column);
    EXPECT_FALSE(generate());
    EXPECT_EQ(status::err_invalid_argument, error.code);
    EXPECT_NE(std::string::npos, error.message.find("unresolved stream variable"));
    offer.columns().clear();
    offer.columns().emplace_back(input, column);
    scan.columns().emplace_back(bindings(table->columns()[0]), input);
    EXPECT_FALSE(generate());
    EXPECT_EQ(status::err_invalid_argument, error.code);
    EXPECT_NE(std::string::npos, error.message.find("duplicate stream variable"));
    scan.columns().pop_back();
    meta::variable_order missing_order{};
    EXPECT_FALSE(dist::create_remote_execution_block(graph, nullptr, nullptr, &missing_order, nullptr, {}, &error));
    EXPECT_EQ(status::err_invalid_argument, error.code);
    ASSERT_TRUE(generate());
    EXPECT_EQ(status::ok, error.code);
    EXPECT_TRUE(error.message.empty());
}

TEST(remote_execution_block_test, requires_index_identity_and_preserves_zero) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T", {{"C0", type::int8{}}}});
    auto index = std::make_shared<yugawara::storage::index>(table, "T");
    auto variable = bindings.stream_variable("c0");
    takatori::plan::forward output{bindings.exchange_column("c0")};
    relation::graph_type graph{};
    auto& scan = graph.insert(relation::scan{bindings(*index), {{bindings(table->columns()[0]), variable}}, {}, {}});
    auto& offer = graph.insert(relation::step::offer{bindings.exchange(output), {{variable, output.columns()[0]}}});
    scan.output() >> offer.input();
    dist::remote_execution_block_generation_error error{};
    EXPECT_FALSE(dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, nullptr, {}, &error));
    EXPECT_EQ(status::err_invalid_argument, error.code);
    index->definition_id(0);
    auto block = dist::create_remote_execution_block(graph);
    ASSERT_TRUE(block);
    ASSERT_EQ(1, block->result_schema.size());
    // This graph has one BIGINT output; no compiler type map is supplied by this fixture.
    block->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    EXPECT_EQ(std::optional<std::uint64_t>{0}, block->operators[0].source->definition_id);
    EXPECT_FALSE(dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, nullptr, "worker", &error));
    EXPECT_EQ(status::err_compiler_error, error.code);
    using identity = dist::remote_declaration_identity;
    using kind = dist::remote_declaration_kind;
    auto make_snapshot = [&](identity value) {
        return dist::remote_catalog_snapshot{{}, {}, {{index.get(), std::move(value)}}};
    };
    auto remote = make_snapshot({"worker", kind::index, 5678});
    auto forwarded = dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, &remote, "worker");
    ASSERT_TRUE(forwarded);
    auto checked = dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, &remote, "worker", &error);
    ASSERT_TRUE(checked);
    EXPECT_EQ(status::ok, error.code);
    EXPECT_EQ(std::optional<std::uint64_t>{5678}, forwarded->operators[0].source->definition_id);
    auto remote_zero = make_snapshot({"worker", kind::index, 0});
    auto zero = dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, &remote_zero, "worker");
    ASSERT_TRUE(zero);
    EXPECT_EQ(std::optional<std::uint64_t>{0}, zero->operators[0].source->definition_id);
    for (auto invalid : {identity{"other", kind::index, 5678}, identity{"worker", kind::table, 5678},
                         identity{"worker", kind::index, std::nullopt}}) {
        auto expected = invalid.remote_definition_id ? status::err_compiler_error : status::err_invalid_argument;
        auto snapshot = make_snapshot(std::move(invalid));
        EXPECT_FALSE(dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, &snapshot, "worker", &error));
        EXPECT_EQ(expected, error.code);
    }
    dist::remote_catalog_snapshot missing_identity{};
    EXPECT_FALSE(dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, &missing_identity, "worker"));
    auto legacy = *block;
    legacy.version = 1;
    auto rejected = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(legacy));
    EXPECT_FALSE(rejected);
    EXPECT_EQ("unsupported RemoteExecutionBlock version", rejected.error);
    auto missing_column = *block;
    missing_column.operators[0].source->columns[0].column_index.reset();
    auto missing_position = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(missing_column));
    EXPECT_FALSE(missing_position);
    EXPECT_NE(std::string::npos, missing_position.error.find("no source column position or type"));
    auto missing_type = *block;
    missing_type.operators[0].source->columns[0].type_payload.clear();
    auto missing_payload = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(missing_type));
    EXPECT_FALSE(missing_payload);
    EXPECT_NE(std::string::npos, missing_payload.error.find("no source column position or type"));
    block->operators[0].source->definition_id.reset();
    auto missing = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(*block));
    EXPECT_FALSE(missing);
    EXPECT_NE(std::string::npos, missing.error.find("no storage definition ID"));
}

TEST(remote_execution_block_test, rejects_invalid_protobuf_payload) {
    auto decoded = dist::decode_remote_execution_block("not protobuf");
    EXPECT_FALSE(decoded);
    EXPECT_EQ("invalid RemoteExecutionBlock protobuf payload", decoded.error);
}

TEST(remote_execution_block_test, rejects_undefined_expression_reference) {
    tateyama::grpc::proto::RemoteExecutionBlock block{};
    block.set_version(3);
    block.add_value_ids(0);
    block.add_stream_variable_ids(0);
    block.add_external_relation_ids(0);
    block.set_result_value_id(0);
    auto* result_column = block.mutable_result_schema()->add_columns();
    result_column->set_column_id(0);
    result_column->set_type_payload(dist::encode_remote_type(type::int8{}));

    auto* scan = block.add_operators();
    scan->set_operator_id(0);
    scan->set_kind(tateyama::grpc::proto::OPERATOR_KIND_SCAN);
    scan->set_output_value_id(0);
    scan->mutable_source()->mutable_storage()->set_table_name("T0");
    scan->mutable_source()->mutable_storage()->set_index_name("I0");
    scan->mutable_source()->mutable_storage()->set_remote_definition_id("1257");
    scan->mutable_source()->mutable_lower()->set_kind(tateyama::grpc::proto::ENDPOINT_KIND_UNBOUND);
    scan->mutable_source()->mutable_upper()->set_kind(tateyama::grpc::proto::ENDPOINT_KIND_UNBOUND);
    scan->mutable_source()->add_find_key_expression_indexes(999);

    auto* offer = block.add_operators();
    offer->set_operator_id(1);
    offer->set_kind(tateyama::grpc::proto::OPERATOR_KIND_OFFER);
    offer->set_input_value_id(0);
    offer->mutable_offer()->set_external_relation_id(0);

    auto decoded = dist::decode_remote_execution_block(block.SerializeAsString());
    EXPECT_FALSE(decoded);
    EXPECT_EQ("find key expression 999 is undefined", decoded.error);
}

TEST(remote_execution_block_test, rejects_non_operator_sequence) {
    tateyama::grpc::proto::RemoteExecutionBlock block{};
    block.set_version(3);
    block.add_value_ids(0);
    block.set_result_value_id(0);
    auto* project = block.add_operators();
    project->set_operator_id(0);
    project->set_kind(tateyama::grpc::proto::OPERATOR_KIND_PROJECT);
    project->set_output_value_id(0);
    auto* offer = block.add_operators();
    offer->set_operator_id(1);
    offer->set_kind(tateyama::grpc::proto::OPERATOR_KIND_OFFER);
    offer->set_input_value_id(0);
    offer->mutable_offer()->set_external_relation_id(0);
    block.add_external_relation_ids(0);

    auto decoded = dist::decode_remote_execution_block(block.SerializeAsString());
    EXPECT_FALSE(decoded);
    EXPECT_EQ("operator graph must contain exactly one source", decoded.error);
}

TEST(remote_execution_block_test, creates_find_source_payload) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto c0 = bindings.stream_variable("c0");

    relation::graph_type graph{};
    std::vector<relation::find::column> columns{
        {bindings(table->columns()[0]), c0},
    };
    std::vector<relation::find::key> keys{};
    keys.emplace_back(
        bindings(table->columns()[0]),
        scalar::immediate{value::int8{10}, type::int8{}}
    );
    auto& find = graph.insert(relation::find{
        bindings(*index),
        std::move(columns),
        std::move(keys),
    });
    takatori::plan::forward forward{bindings.exchange_column()};
    auto& offer = graph.insert(relation::step::offer{
        bindings.exchange(forward),
        {{c0, forward.columns()[0]}},
    });
    find.output() >> offer.input();

    auto block = dist::create_remote_execution_block(graph);

    ASSERT_TRUE(block);
    ASSERT_EQ(1, block->result_schema.size());
    // This graph has one BIGINT output; no compiler type map is supplied by this fixture.
    block->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    ASSERT_TRUE(block->operators[0].source);
    auto const& source = *block->operators[0].source;
    EXPECT_EQ(1, source.find_key_count);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, source.lower_endpoint_kind);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, source.upper_endpoint_kind);
    EXPECT_FALSE(source.limit);
    auto decoded = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(*block));
    ASSERT_TRUE(decoded) << decoded.error;
    ASSERT_TRUE(decoded.block->operators[0].source);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, decoded.block->operators[0].source->lower_endpoint_kind);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, decoded.block->operators[0].source->upper_endpoint_kind);
    EXPECT_FALSE(decoded.block->operators[0].source->limit);
}

TEST(remote_execution_block_test, rebuilds_unordered_operators_through_wire_and_direct_entry) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto c0 = bindings.stream_variable("c0");

    relation::graph_type source_graph{};
    auto& scan = source_graph.insert(relation::scan{
        bindings(*index),
        {{bindings(table->columns()[0]), c0}},
        {},
        {},
        {},
    });
    auto& filter = source_graph.insert(relation::filter{scalar::compare{
        scalar::comparison_operator::greater,
        scalar::variable_reference{c0},
        scalar::immediate{value::int8{10}, type::int8{}},
    }});
    takatori::plan::forward forward{bindings.exchange_column("result")};
    auto& offer = source_graph.insert(relation::step::offer{
        bindings.exchange(forward),
        {{c0, forward.columns()[0]}},
    });
    scan.output() >> filter.input();
    filter.output() >> offer.input();

    auto source_block = dist::create_remote_execution_block(source_graph);
    ASSERT_TRUE(source_block);
    ASSERT_EQ(1, source_block->result_schema.size());
    // This graph has one BIGINT output; no compiler type map is supplied by this fixture.
    source_block->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    source_block->external_relations = {42};
    source_block->operators.back().offer->external_relation_id = 42;
    auto const operators = source_block->operators;
    auto external = std::make_shared<yugawara::binding::external_relation_info>(42, forward.columns());
    test_graph_resolver resolver{bindings(*index), bindings(table->columns()[0]),
        bindings.external_relation(external), forward.columns()[0]};
    std::array<std::size_t, 3> permutation{0, 1, 2};
    std::size_t count{};
    do {
        SCOPED_TRACE(++count);
        for (std::size_t i = 0; i < permutation.size(); ++i) source_block->operators[i] = operators[permutation[i]];
        auto decoded = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(*source_block));
        ASSERT_TRUE(decoded) << decoded.error;
        ASSERT_EQ(3, decoded.operator_order.size());
        for (std::size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(operators[i].operator_id, decoded.block->operators[decoded.operator_order[i]].operator_id);
        }
        auto rebuilt = dist::build_remote_execution_graph(*source_block, resolver);
        ASSERT_TRUE(rebuilt) << rebuilt.error;
        ASSERT_EQ(3, rebuilt.graph->size());
        std::size_t scans{};
        for (auto const& node : *rebuilt.graph) {
            if (node.kind() != relation::expression_kind::scan) continue;
            ++scans;
            ASSERT_TRUE(node.output_ports()[0].opposite());
            auto const& next = node.output_ports()[0].opposite()->owner();
            ASSERT_EQ(relation::expression_kind::filter, next.kind());
            ASSERT_TRUE(next.output_ports()[0].opposite());
            EXPECT_EQ(relation::expression_kind::offer, next.output_ports()[0].opposite()->owner().kind());
        }
        EXPECT_EQ(1, scans);
        for (std::size_t i = 0; i < permutation.size(); ++i) {
            EXPECT_EQ(operators[permutation[i]].operator_id, source_block->operators[i].operator_id);
        }
    } while (std::next_permutation(permutation.begin(), permutation.end()));
    EXPECT_EQ(6, count);

    auto valid = *source_block;
    valid.operators = operators;
    auto reject = [&](auto const& broken, std::string_view reason) {
        SCOPED_TRACE(reason);
        std::vector<std::size_t> unchanged{99};
        auto error = dist::validate_remote_execution_block(broken, &unchanged);
        ASSERT_TRUE(error);
        EXPECT_NE(std::string::npos, error->find(reason)) << *error;
        EXPECT_EQ((std::vector<std::size_t>{99}), unchanged);
        EXPECT_FALSE(dist::build_remote_execution_graph(broken, resolver));
        EXPECT_FALSE(dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(broken)));
    };
    auto missing_type = valid;
    auto literal = std::find_if(missing_type.scalar_expressions.begin(), missing_type.scalar_expressions.end(),
        [](auto const& expression) { return expression.kind == dist::remote_scalar_expression_kind::immediate; });
    ASSERT_NE(missing_type.scalar_expressions.end(), literal);
    auto literal_id = static_cast<std::uint64_t>(literal - missing_type.scalar_expressions.begin());
    literal->type_payload.clear();
    reject(missing_type, "missing remote scalar type payload");
    missing_type = valid;
    dist::remote_scalar_expression cast{};

    cast.kind = dist::remote_scalar_expression_kind::cast;
    cast.operator_kind = "error";
    cast.type = "int8()";
    cast.operand_expression_indexes = {literal_id};
    missing_type.scalar_expressions.emplace_back(std::move(cast));
    reject(missing_type, "missing remote scalar type payload");
    missing_type = valid;
    missing_type.result_schema[0].type_payload.clear();
    reject(missing_type, "missing remote output type payload");
    missing_type = valid;
    missing_type.operators[0].source->columns[0].type_payload.clear();
    reject(missing_type, "no source column position or type");

    auto broken = valid;
    broken.operators[2].input_value_id = operators[0].output_value_id;
    broken.operators[1].input_value_id = 10; // isolated self-cycle
    broken.operators[1].output_value_id = 10;
    broken.values.push_back(10);
    reject(broken, "cycle or disconnected");
    broken = valid;
    broken.operators[2].input_value_id = broken.operators[0].output_value_id; // branch
    reject(broken, "must not branch");
    broken = valid;
    broken.operators[1].output_value_id = broken.operators[0].output_value_id; // duplicate producer
    reject(broken, "duplicate value producers");
    broken = valid;
    broken.operators[1].input_value_id = 999; // undefined/disconnected input
    reject(broken, "disconnected output");
    broken = valid;
    broken.operators[1] = broken.operators[0]; // second source
    broken.operators[1].operator_id = 100;
    reject(broken, "exactly one source");
    broken = valid;
    broken.operators[1] = broken.operators[2]; // second offer
    broken.operators[1].operator_id = 100;
    broken.operators[1].input_value_id = operators[0].output_value_id;
    reject(broken, "exactly one offer");
    broken = valid;
    broken.result_value_id = *broken.operators[0].output_value_id; // wrong result
    reject(broken, "result value does not match");
    broken = valid;
    broken.operators.clear();
    reject(broken, "must contain a source and an offer");
    broken = valid;
    broken.operators = {operators[0], operators[1]};
    reject(broken, "exactly one offer");
    broken = valid;
    broken.operators[0].input_value_id = operators[1].output_value_id;
    reject(broken, "must not branch");
    // A valid source-to-offer component plus a disconnected two-operator cycle.
    broken = valid;
    broken.operators[2].input_value_id = operators[0].output_value_id;
    broken.operators[1].input_value_id = 10;
    broken.operators[1].output_value_id = 11;
    auto other = broken.operators[1];
    other.operator_id = 100;
    other.input_value_id = 11;
    other.output_value_id = 10;
    broken.operators.push_back(std::move(other));
    broken.values.insert(broken.values.end(), {10, 11});
    reject(broken, "cycle or disconnected");
}

TEST(remote_execution_block_test, rebuilds_takatori_graph) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto c0 = bindings.stream_variable("c0");

    relation::graph_type source_graph{};
    auto& scan = source_graph.insert(relation::scan{
        bindings(*index),
        {{bindings(table->columns()[0]), c0}},
        {},
        {},
        {},
    });
    auto& filter = source_graph.insert(relation::filter{scalar::compare{
        scalar::comparison_operator::greater,
        scalar::variable_reference{c0},
        scalar::immediate{value::int8{10}, type::int8{}},
    }});
    takatori::plan::forward forward{bindings.exchange_column("result")};
    auto& offer = source_graph.insert(relation::step::offer{
        bindings.exchange(forward),
        {{c0, forward.columns()[0]}},
    });
    scan.output() >> filter.input();
    filter.output() >> offer.input();

    auto source_block = dist::create_remote_execution_block(source_graph);
    ASSERT_TRUE(source_block);
    ASSERT_EQ(1, source_block->result_schema.size());
    // This graph has one BIGINT output; no compiler type map is supplied by this fixture.
    source_block->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    source_block->external_relations = {42};
    source_block->operators.back().offer->external_relation_id = 42;
    std::weak_ptr<yugawara::binding::external_relation_info> external_owner{};
    {
        // The restored graph retains the external information after the resolver is gone.
        auto rebuilt = [&] {
            auto external = std::make_shared<yugawara::binding::external_relation_info>(42, forward.columns());
            external_owner = external;
            test_graph_resolver resolver{
                bindings(*index),
                bindings(table->columns()[0]),
                bindings.external_relation(external),
                forward.columns()[0],
            };
            return dist::build_remote_execution_graph(*source_block, resolver);
        }();

        ASSERT_TRUE(rebuilt) << rebuilt.error;
        EXPECT_FALSE(external_owner.expired());
        ASSERT_EQ(3, rebuilt.graph->size());
        for (auto const& expression : *rebuilt.graph) {
            if (expression.kind() != relation::expression_kind::offer) continue;
            auto const& restored = dynamic_cast<relation::step::offer const&>(expression);
            auto const& external = yugawara::binding::extract<yugawara::binding::external_relation_info>(restored.destination());
            EXPECT_EQ(42, external.relation_id());
            ASSERT_EQ(1, external.columns().size());
            EXPECT_EQ(forward.columns()[0], external.columns()[0]);
            EXPECT_EQ(external.columns()[0], restored.columns()[0].destination());
        }
        EXPECT_TRUE(dist::is_remote_process_execution_block_shape(*rebuilt.graph));
        auto round_trip = dist::create_remote_execution_block(*rebuilt.graph);
        ASSERT_TRUE(round_trip);
        ASSERT_EQ(3, round_trip->operators.size());
        EXPECT_EQ((std::vector<std::uint64_t>{42}), round_trip->external_relations);
        EXPECT_EQ(42, round_trip->operators.back().offer->external_relation_id);
        EXPECT_EQ(dist::remote_operator_kind::scan, round_trip->operators[0].kind);
        EXPECT_EQ(dist::remote_operator_kind::filter, round_trip->operators[1].kind);
        EXPECT_EQ(dist::remote_operator_kind::offer, round_trip->operators[2].kind);
        ASSERT_EQ(3, round_trip->scalar_expressions.size());
        EXPECT_EQ(dist::remote_scalar_expression_kind::compare,
            round_trip->scalar_expressions.back().kind);

        auto compiled = dist::compile_remote_execution_graph(*rebuilt.graph);
        ASSERT_TRUE(compiled) << compiled.error;
        EXPECT_TRUE(compiled.processor_info->details().has_scan_operator());
    }
    // No resolver or graph remains: the external information must be released.
    EXPECT_TRUE(external_owner.expired());
}

TEST(remote_execution_block_test, native_external_layout_preserves_order_and_rejects_invalid_bindings) {
    for (int invalid = 0; invalid != 4; ++invalid) {
        SCOPED_TRACE(invalid);
        yugawara::binding::factory bindings{};
        yugawara::storage::configurable_provider storages{};
        auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
        auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
        auto source = bindings.stream_variable("source");
        auto first = bindings.exchange_column("first");
        auto second = bindings.exchange_column("second");
        std::vector<takatori::descriptor::variable> columns{first, second};
        if (invalid == 3) columns.pop_back();
        auto destination = bindings.external_relation(yugawara::binding::external_relation_info{73, columns});
        relation::graph_type graph{};
        auto& scan = graph.insert(relation::scan{
            bindings(*index), {{bindings(table->columns()[0]), source}}, {}, {}, {},
        });
        auto other = first;
        if (invalid == 1) other = second;
        if (invalid == 2) other = bindings.exchange_column("unknown");
        auto& offer = graph.insert(relation::step::offer{destination, {{source, second}, {source, other}}});
        scan.output() >> offer.input();
        auto block = dist::create_remote_execution_block(graph);
        if (invalid) {
            EXPECT_FALSE(block);
            continue;
        }
        ASSERT_TRUE(block);
        EXPECT_EQ((std::vector<std::uint64_t>{73}), block->external_relations);
        EXPECT_EQ(1, block->operators.back().offer->columns[0].destination_column_id);
        EXPECT_EQ(0, block->operators.back().offer->columns[1].destination_column_id);
    }
}

TEST(remote_execution_block_test, rejects_scalar_overflow_and_invalid_literals_before_publication) {
    auto generate = [](std::size_t count, std::string_view text, dist::remote_execution_block_generation_error* error = nullptr) {
        yugawara::binding::factory bindings{};
        yugawara::storage::configurable_provider storages{};
        auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
        auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
        auto input = bindings.stream_variable("input");
        auto output = bindings.stream_variable("output");
        auto column = bindings.exchange_column("column");
        relation::graph_type graph{};
        auto& scan = graph.insert(relation::scan{bindings(*index), {{bindings(table->columns()[0]), input}}, {}, {}, {}});
        std::unique_ptr<scalar::expression> expression = std::make_unique<scalar::immediate>(value::int8{1}, type::int8{});
        if (!text.empty()) {
            expression = std::make_unique<scalar::immediate>(value::character{std::string{text}}, type::character{type::varying, 2});
        }
        for (std::size_t i = 1; i < count; ++i) {
            expression = std::make_unique<scalar::unary>(scalar::unary_operator::plus, std::move(expression));
        }
        std::vector<relation::project::column> columns{};
        columns.emplace_back(output, std::move(expression));
        auto& project = graph.insert(relation::project{std::move(columns)});
        auto destination = bindings.external_relation(yugawara::binding::external_relation_info{73, {column}});
        auto& offer = graph.insert(relation::step::offer{destination, {{output, column}}});
        scan.output() >> project.input();
        project.output() >> offer.input();
        return dist::create_remote_execution_block(graph, nullptr, nullptr, nullptr, nullptr, {}, error);
    };
    auto valid = generate(512, {});
    ASSERT_TRUE(valid);
    EXPECT_EQ(512U, valid->scalar_expressions.size());
    EXPECT_FALSE(dist::validate_remote_scalar_expressions(*valid));
    dist::remote_execution_block_generation_error error{};
    EXPECT_FALSE(generate(513, {}, &error));
    EXPECT_EQ(status::err_invalid_argument, error.code);
    EXPECT_TRUE(generate(1, "ab"));
    EXPECT_FALSE(generate(1, "abc", &error));
    EXPECT_EQ(status::err_invalid_argument, error.code);
}

TEST(remote_execution_block_test, direct_builder_preserves_scalar_failure_classification) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({std::uint64_t{1257}, table, "I0"});
    auto variable = bindings.stream_variable("input");
    auto column = bindings.exchange_column("output");
    auto external = bindings.external_relation(yugawara::binding::external_relation_info{73, {column}});
    relation::graph_type graph{};
    auto& scan = graph.insert(relation::scan{bindings(*index), {{bindings(table->columns()[0]), variable}}, {}, {}, {}});
    auto& offer = graph.insert(relation::step::offer{external, {{variable, column}}});
    scan.output() >> offer.input();
    auto valid = dist::create_remote_execution_block(graph);
    ASSERT_TRUE(valid);
    valid->result_schema[0].type_payload = dist::encode_remote_type(type::int8{});
    test_graph_resolver resolver{bindings(*index), bindings(table->columns()[0]), external, column};
    ASSERT_TRUE(dist::build_remote_execution_graph(*valid, resolver));
    dist::remote_scalar_expression literal{};
    literal.kind = dist::remote_scalar_expression_kind::immediate;
    literal.type_payload = dist::encode_remote_type(type::int8{});
    literal.literal_payload = dist::encode_remote_literal(value::int8{7});
    auto check = [&](dist::remote_execution_block const& block, bool unsupported) {
        auto result = dist::build_remote_execution_graph(block, resolver);
        EXPECT_FALSE(result);
        EXPECT_EQ(unsupported, result.unsupported) << result.error;
        EXPECT_EQ(!unsupported, result.invalid_request) << result.error;
    };
    for (auto const* name : {"", "future_operation"}) {
        auto block = *valid;
        block.scalar_expressions = {literal,
            {dist::remote_scalar_expression_kind::unary, name, {}, {}, {}, {}, {0}}};
        check(block, name[0] != '\0');
    }
    auto block = *valid;
    block.scalar_expressions = {literal};
    proto::distributed_query::PortableType unknown{};
    unknown.set_kind("future_type");
    block.scalar_expressions[0].type_payload = unknown.SerializeAsString();
    block.scalar_expressions[0].literal_payload = dist::encode_remote_literal(value::unknown{value::unknown_kind::null});
    check(block, true);
    auto decoded = dist::decode_remote_execution_block(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(decoded);
    EXPECT_TRUE(decoded.unsupported);
    block.scalar_expressions[0] = literal;
    block.scalar_expressions[0].kind = static_cast<dist::remote_scalar_expression_kind>(2026);
    check(block, true);
    block.scalar_expressions[0].operand_expression_indexes = {0, 999};
    check(block, true); // Unknown native kinds are not interpreted using known arity rules.
    block.scalar_expressions.assign(513, literal);
    check(block, false);
    block.version = 4;
    check(block, true); // Version precedes the resource bound, including direct C++ input.
    EXPECT_EQ("unsupported RemoteExecutionBlock version", dist::build_remote_execution_graph(block, resolver).error);
    block.version = 0;
    check(block, false);
    EXPECT_EQ("missing RemoteExecutionBlock version", dist::build_remote_execution_graph(block, resolver).error);
}

TEST(remote_execution_block_test, counts_filter_and_storage_key_roots_in_expansion_budget) {
    for (int root_kind = 0; root_kind != 4; ++root_kind) {
        SCOPED_TRACE(root_kind);
        dist::remote_execution_block block{};
        dist::remote_scalar_expression literal{};
        literal.kind = dist::remote_scalar_expression_kind::immediate;
        literal.type_payload = dist::encode_remote_type(type::int8{});
        literal.literal_payload = dist::encode_remote_literal(value::int8{1});
        block.scalar_expressions = {literal};
        for (std::uint64_t index = 1; index != 8; ++index) {
            block.scalar_expressions.push_back({dist::remote_scalar_expression_kind::binary,
                "add", {}, {}, {}, {}, {index - 1, index - 1}});
        }
        dist::remote_operator roots{};
        // The other root accounts for one shared tree of 255 nodes.
        roots.source.emplace();
        switch (root_kind) {
            case 0: roots.filter_expression_index = 7; break;
            case 1: roots.source->find_key_expression_indexes = {7}; break;
            case 2: roots.source->lower_key_expression_indexes = {7}; break;
            case 3: roots.source->upper_key_expression_indexes = {7}; break;
        }
        for (std::uint64_t index = 0; index != 93; ++index) {
            roots.project_columns.push_back({index + 1, index < 38 ? 7U : 0U});
        }
        block.operators = {roots};
        // Test the scalar budget independently of the enclosing operator-graph validation.
        EXPECT_FALSE(dist::validate_remote_scalar_expressions(block));
        block.operators[0].project_columns.push_back({94, 0});
        EXPECT_EQ("scalar expression expansion limit exceeded", dist::validate_remote_scalar_expressions(block));
        block.operators[0].project_columns = {{1, 999}};
        EXPECT_EQ("scalar expression index is out of range", dist::validate_remote_scalar_expressions(block));
    }
}

TEST(remote_execution_block_test, rejects_non_graph) {
    yugawara::binding::factory bindings{};
    relation::graph_type graph{};
    graph.insert(relation::values{{}, {}});

    EXPECT_FALSE(dist::create_remote_execution_block(graph));
}

} // namespace


TEST(remote_execution_result_test, rejects_version_legacy_rows_and_duplicate_columns) {
    for (int sample = 0; sample != 3; ++sample) {
        SCOPED_TRACE(sample);
        jogasaki::proto::distributed_query::RemoteExecutionResult message{};
        message.set_version(sample == 0 ? 1 : 2);
        if (sample == 1) message.add_rows();
        if (sample == 2) {
            for (int i = 0; i != 2; ++i) {
                auto* column = message.mutable_schema()->add_columns();
                column->set_column_id(1055);
                column->set_type_payload(dist::encode_remote_type(type::int8{}));
            }
        }
        std::string error{};
        EXPECT_FALSE(dist::deserialize_remote_execution_result(message.SerializeAsString(), error));
        EXPECT_FALSE(error.empty());
    }
}

} // namespace jogasaki::testing
