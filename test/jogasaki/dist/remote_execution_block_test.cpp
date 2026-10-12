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
#include <takatori/util/rvalue_reference_wrapper.h>
#include <takatori/value/primitive.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <tateyama/proto/distributed_query/remote_execution_block.pb.h>

#include <jogasaki/dist/remote_execution_block.h>
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
        {{0, "int8()", false}, {1, "int8()", true}},
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
        std::string_view,
        std::string_view) const override {
        return storage_;
    }

    std::optional<takatori::descriptor::variable> resolve_storage_column(
        std::string_view,
        std::string_view,
        std::string_view) const override {
        return storage_column_;
    }

    std::optional<takatori::descriptor::variable> resolve_storage_key_column(
        std::string_view,
        std::string_view,
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
        if (expression.value_kind != "int8" || expression.type != "int8()") return {};
        auto value = expression.value;
        if (auto begin = value.rfind('('); begin != std::string::npos && value.back() == ')') {
            value = value.substr(begin + 1, value.size() - begin - 2);
        }
        return std::make_unique<scalar::immediate>(
            value::int8{std::stoll(value)},
            type::int8{});
    }

    std::shared_ptr<type::data const> resolve_type(std::string_view name) const override {
        if (name != "int8()") return {};
        return std::make_shared<type::int8>();
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
    auto index = storages.add_index({table, "I0"});
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
    dist::remote_catalog_snapshot catalogs{{}, {{table.get(), "worker"}}};
    EXPECT_TRUE(dist::validate_remote_plan(plan, &catalogs).empty());
    EXPECT_TRUE(dist::validate_remote_plan(plan, nullptr, "worker").empty());
    add();
    EXPECT_NE(std::string::npos, dist::validate_remote_plan(plan, nullptr, "worker").find("at most one"));
    EXPECT_NE(std::string::npos, dist::validate_remote_plan(plan, &catalogs).find("at most one"));
    // Without remote metadata or an explicit override, both ordinary SQL processes are valid.
    EXPECT_TRUE(dist::validate_remote_plan(plan, nullptr).empty());
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
    auto index = storages.add_index({table, "I0"});
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
    auto index = storages.add_index({table, "I0"});
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
        25,
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
    EXPECT_EQ(1, block->version);
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
    ASSERT_EQ(1, source.lower_key_expression_ids.size());
    EXPECT_EQ(0, source.lower_key_expression_ids[0]);

    ASSERT_EQ(9, block->scalar_expressions.size());
    EXPECT_EQ(dist::remote_scalar_expression_kind::immediate, block->scalar_expressions[0].kind);
    EXPECT_EQ(dist::remote_scalar_expression_kind::variable_reference, block->scalar_expressions[1].kind);
    EXPECT_EQ(0, block->scalar_expressions[1].variable_id);
    EXPECT_EQ(dist::remote_scalar_expression_kind::binary, block->scalar_expressions[3].kind);
    EXPECT_EQ("add", block->scalar_expressions[3].operator_kind);
    EXPECT_EQ((std::vector<std::uint64_t>{1, 2}), block->scalar_expressions[3].operand_expression_ids);
    EXPECT_EQ(dist::remote_scalar_expression_kind::compare, block->scalar_expressions[5].kind);
    EXPECT_EQ("greater", block->scalar_expressions[5].operator_kind);
    EXPECT_EQ(5, block->operators[1].filter_expression_id);
    EXPECT_EQ(dist::remote_scalar_expression_kind::unary, block->scalar_expressions[7].kind);
    EXPECT_EQ(dist::remote_scalar_expression_kind::cast, block->scalar_expressions[8].kind);
    ASSERT_EQ(1, block->operators[2].project_columns.size());
    EXPECT_EQ(1, block->operators[2].project_columns[0].destination_variable_id);
    EXPECT_EQ(8, block->operators[2].project_columns[0].value_expression_id);
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
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.version=1\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.2.kind=project\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.result_value_id=2\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.table_name=T0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.column.0.name=C0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.lower.kind=inclusive\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.lower.key.0.expression_id=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.0.source.limit=25\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.1.filter_expression_id=5\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.scalar_expression.3.operator_kind=add\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.scalar_expression.8.kind=cast\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.3.offer.external_relation_id=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.operator.3.offer.column.0.source_variable_id=0\n"));
    EXPECT_NE(std::string::npos, serialized.find("remote_execution_block.result_schema.column.0.type=unknown\n"));

    auto bytes = dist::serialize_remote_execution_block_proto(*block);
    tateyama::grpc::proto::RemoteExecutionBlock decoded{};
    ASSERT_TRUE(decoded.ParseFromString(bytes));
    EXPECT_EQ(1, decoded.version());
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
}

TEST(remote_execution_block_test, rejects_invalid_protobuf_payload) {
    auto decoded = dist::decode_remote_execution_block("not protobuf");
    EXPECT_FALSE(decoded);
    EXPECT_EQ("invalid RemoteExecutionBlock protobuf payload", decoded.error);
}

TEST(remote_execution_block_test, rejects_undefined_expression_reference) {
    tateyama::grpc::proto::RemoteExecutionBlock block{};
    block.set_version(1);
    block.add_value_ids(0);
    block.add_stream_variable_ids(0);
    block.add_external_relation_ids(0);
    block.set_result_value_id(0);
    auto* result_column = block.mutable_result_schema()->add_columns();
    result_column->set_column_id(0);

    auto* scan = block.add_operators();
    scan->set_operator_id(0);
    scan->set_kind(tateyama::grpc::proto::OPERATOR_KIND_SCAN);
    scan->set_output_value_id(0);
    scan->mutable_source()->mutable_storage()->set_table_name("T0");
    scan->mutable_source()->mutable_storage()->set_index_name("I0");
    scan->mutable_source()->mutable_lower()->set_kind(tateyama::grpc::proto::ENDPOINT_KIND_UNBOUND);
    scan->mutable_source()->mutable_upper()->set_kind(tateyama::grpc::proto::ENDPOINT_KIND_UNBOUND);
    scan->mutable_source()->add_find_key_expression_ids(999);

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
    block.set_version(1);
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
    EXPECT_EQ("the first operator must be scan or find", decoded.error);
}

TEST(remote_execution_block_test, creates_find_source_payload) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({table, "I0"});
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
    ASSERT_TRUE(block->operators[0].source);
    auto const& source = *block->operators[0].source;
    EXPECT_EQ(1, source.find_key_count);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, source.lower_endpoint_kind);
    EXPECT_EQ(dist::remote_endpoint_kind::unbound, source.upper_endpoint_kind);
    EXPECT_FALSE(source.limit);
}

TEST(remote_execution_block_test, rebuilds_takatori_graph) {
    yugawara::binding::factory bindings{};
    yugawara::storage::configurable_provider storages{};
    auto table = storages.add_table({"T0", {{"C0", type::int8{}}}});
    auto index = storages.add_index({table, "I0"});
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
    test_graph_resolver resolver{
        bindings(*index),
        bindings(table->columns()[0]),
        bindings.exchange(forward),
        forward.columns()[0],
    };

    auto rebuilt = dist::build_remote_execution_graph(*source_block, resolver);

    ASSERT_TRUE(rebuilt) << rebuilt.error;
    ASSERT_EQ(3, rebuilt.graph->size());
    EXPECT_TRUE(dist::is_remote_process_execution_block_shape(*rebuilt.graph));
    auto round_trip = dist::create_remote_execution_block(*rebuilt.graph);
    ASSERT_TRUE(round_trip);
    ASSERT_EQ(3, round_trip->operators.size());
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

TEST(remote_execution_block_test, rejects_non_graph) {
    yugawara::binding::factory bindings{};
    relation::graph_type graph{};
    graph.insert(relation::values{{}, {}});

    EXPECT_FALSE(dist::create_remote_execution_block(graph));
}

} // namespace

} // namespace jogasaki::testing
