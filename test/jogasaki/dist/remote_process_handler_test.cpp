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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <gtest/gtest.h>

#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/character.h>
#include <takatori/value/date.h>
#include <takatori/value/decimal.h>
#include <takatori/value/octet.h>
#include <takatori/value/primitive.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <yugawara/type/conversion.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/dist/remote_execution_block_proto.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_process_handler.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/details/decimal_context.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/proto/sql/response.pb.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/scheduler/thread_local_info.h>
#include <jogasaki/serializer/value_input.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/create_tx.h>

namespace jogasaki::testing {
namespace {
dist::remote_execution_block scan_block() {
    dist::remote_execution_block block{};
    block.values = {0};
    block.stream_variables = {0};
    block.external_relations = {0};
    block.result_schema = {{0, "int8()", false}};
    dist::remote_operator scan{};
    scan.operator_id = 0;
    scan.kind = dist::remote_operator_kind::scan;
    scan.output_value_id = 0;
    scan.source = dist::remote_source{};
    scan.source->table_name = "T";
    scan.source->index_name = "T";
    scan.source->columns = {{"C0", 0}};
    dist::remote_operator offer{};
    offer.operator_id = 1;
    offer.kind = dist::remote_operator_kind::offer;
    offer.input_value_id = 0;
    offer.offer = dist::remote_offer{0, {{0, 0}}};
    block.operators = {scan, offer};
    return block;
}
// Projects a 1 MiB literal per scanned row so that the stream sink is called during the scan.
dist::remote_execution_block large_row_block() {
    auto block = scan_block();
    block.values = {0, 1};
    block.stream_variables = {0, 1};
    block.result_value_id = 1;
    auto literal = takatori::value::character{std::string(1024 * 1024, 'x')};
    dist::remote_scalar_expression expression{};
    expression.expression_id = 0;
    expression.kind = dist::remote_scalar_expression_kind::immediate;
    expression.type_payload = dist::encode_remote_type(takatori::type::character{takatori::type::varying});
    expression.literal_payload = dist::encode_remote_literal(literal);
    block.scalar_expressions = {expression};
    dist::remote_operator project{};
    project.operator_id = 1;
    project.kind = dist::remote_operator_kind::project;
    project.input_value_id = 0; project.output_value_id = 1;
    project.project_columns = {{1, 0}};
    auto offer = block.operators.back();
    offer.operator_id = 2; offer.input_value_id = 1;
    offer.offer->columns[0].source_variable_id = 1;
    block.operators = {block.operators.front(), project, offer};
    block.result_schema = {{0, "character()", true, dist::encode_remote_type(takatori::type::character{takatori::type::varying})}};
    return block;
}
}
class remote_process_handler_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(); }
    void TearDown() override { if (db_) db_teardown(); }
};
class remote_process_handler_single_worker_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }
    void SetUp() override {
        auto config = std::make_shared<configuration>();
        config->thread_pool_size(1);
        db_setup(config);
    }
    void TearDown() override { if (db_) db_teardown(); }
};
TEST_F(remote_process_handler_test, stream_emits_schema_for_empty_result) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    dist::remote_process_handler handler{*db_};
    std::size_t chunks{};
    auto result = handler.execute_stream(dist::serialize_remote_execution_block_proto(scan_block()), {},
        [&](std::string_view payload) {
            ++chunks;
            std::string error{};
            auto rows = dist::deserialize_remote_execution_result(payload, error);
            EXPECT_TRUE(rows);
            if (rows) { EXPECT_EQ(1, rows->columns.size()); EXPECT_TRUE(rows->encoded_rows.empty()); }
            return true;
        });
    EXPECT_TRUE(result.success) << result.error;
    EXPECT_TRUE(result.payload.empty());
    EXPECT_EQ(1, chunks);
}
TEST_F(remote_process_handler_test, stream_stops_when_sink_rejects_result) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES(1)");
    dist::remote_process_handler handler{*db_};
    std::size_t calls{};
    auto result = handler.execute_stream(dist::serialize_remote_execution_block_proto(scan_block()), {},
        [&](std::string_view) { ++calls; return false; });
    EXPECT_FALSE(result.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::cancelled, result.error_kind);
    EXPECT_EQ(1, calls);
}
TEST_F(remote_process_handler_test, scan_observes_cancel_on_rpc_thread) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10), (20), (30)");
    std::atomic_int observed{};
    std::atomic_bool on_shared_worker{};
    auto response = dist::remote_process_handler{*db_}.execute(
        dist::serialize_remote_execution_block_proto(scan_block()), [&] {
            // The remote job runs on the calling thread, never on the shared worker pool.
            if (scheduler::thread_local_info_.is_worker_thread()) on_shared_worker = true;
            return ++observed >= 2;
        });
    EXPECT_GE(observed.load(), 2);
    EXPECT_FALSE(on_shared_worker.load());
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::cancelled, response.error_kind);
}
TEST_F(remote_process_handler_single_worker_test, blocked_stream_sink_does_not_occupy_shared_worker) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1), (2), (3), (4)");
    std::mutex mutex{};
    std::condition_variable cv{};
    bool entered{};
    bool released{};
    std::atomic_bool sink_on_shared_worker{};
    std::atomic_bool local_done{};
    std::size_t local_rows{};
    tateyama::grpc::distributed_query::remote_process_result stream_result{};
    std::thread remote{};
    std::thread local{};
    struct cleanup {
        std::function<void()> body;
        ~cleanup() { body(); }
    } guard{[&] {
        // Always unblock the sink so that both threads can finish even when an assertion fails.
        {
            std::lock_guard lock{mutex};
            released = true;
        }
        cv.notify_all();
        if (remote.joinable()) remote.join();
        if (local.joinable()) local.join();
    }};
    remote = std::thread{[&] {
        stream_result = dist::remote_process_handler{*db_}.execute_stream(
            dist::serialize_remote_execution_block_proto(large_row_block()), {}, [&](std::string_view) {
                if (scheduler::thread_local_info_.is_worker_thread()) sink_on_shared_worker = true;
                std::unique_lock lock{mutex};
                entered = true;
                cv.notify_all();
                cv.wait(lock, [&] { return released; });
                return true;
            });
    }};
    {
        std::unique_lock lock{mutex};
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds{20}, [&] { return entered; }));
    }
    local = std::thread{[&] {
        std::vector<mock::basic_record> rows{};
        execute_query("SELECT C0 FROM T", rows);
        local_rows = rows.size();
        local_done = true;
    }};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (!local_done && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    // The local SQL must complete on the only shared worker while the remote sink is still blocked.
    ASSERT_TRUE(local_done.load());
    {
        std::lock_guard lock{mutex};
        EXPECT_FALSE(released);
    }
    guard.body();
    guard.body = [] {};
    EXPECT_EQ(4, local_rows);
    EXPECT_FALSE(sink_on_shared_worker.load());
    EXPECT_TRUE(stream_result.success) << stream_result.error;
}
TEST_F(remote_process_handler_test, non_standard_sink_exception_during_scan_is_internal_failure) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1), (2), (3)");
    std::size_t calls{};
    auto response = dist::remote_process_handler{*db_}.execute_stream(
        dist::serialize_remote_execution_block_proto(large_row_block()), {}, [&](std::string_view) -> bool {
            ++calls;
            throw 42;  // NOLINT
        });
    EXPECT_EQ(1, calls);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::internal, response.error_kind);
}
TEST_F(remote_process_handler_test, rpc_cancel_reaches_filtered_scan_when_scan_cancel_is_disabled) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    std::string sql = "INSERT INTO T VALUES ";
    for (int i = 0; i < 2000; ++i) { if (i) sql += ','; sql += '(' + std::to_string(i) + ')'; }
    execute_statement(sql);
    // C0 > 1000000 matches no row, so the sink is never reached while scanning.
    auto block = scan_block();
    block.values = {0, 1};
    block.result_value_id = 1;
    block.scalar_expressions = {
        {0, dist::remote_scalar_expression_kind::variable_reference, {}, {}, {}, {}, 0, {}},
        {1, dist::remote_scalar_expression_kind::immediate, {}, "int8", "1000000", "int8()", {}, {}},
        {2, dist::remote_scalar_expression_kind::compare, "greater", {}, {}, {}, {}, {0, 1}},
    };
    dist::remote_operator filter{};
    filter.operator_id = 1;
    filter.kind = dist::remote_operator_kind::filter;
    filter.input_value_id = 0;
    filter.output_value_id = 1;
    filter.filter_expression_id = 2;
    auto offer = block.operators.back();
    offer.operator_id = 2;
    offer.input_value_id = 1;
    block.operators = {block.operators.front(), filter, offer};
    auto payload = dist::serialize_remote_execution_block_proto(block);

    auto config = global::config_pool();
    auto previous = config->req_cancel_config();
    constexpr std::size_t cancel_at = 50;
    std::atomic_size_t checks{};
    std::atomic_bool force_cancel{};
    std::future<tateyama::grpc::distributed_query::remote_process_result> call{};
    struct cleanup {
        std::function<void()> body;
        ~cleanup() { body(); }
    } guard{[&] {
        // Always let the RPC finish and restore the global cancel configuration.
        force_cancel = true;
        if (call.valid()) call.wait();
        global::config_pool()->req_cancel_config(previous);
    }};
    // No kind is enabled, so local SQL would not check cancellation in scan.
    config->req_cancel_config(std::make_shared<request_cancel_config>());
    ASSERT_FALSE(utils::request_cancel_enabled(request_cancel_kind::scan));
    call = std::async(std::launch::async, [&] {
        return dist::remote_process_handler{*db_}.execute_stream(payload,
            [&] { return ++checks >= cancel_at || force_cancel.load(); },
            [](std::string_view) { return true; });
    });
    ASSERT_EQ(std::future_status::ready, call.wait_for(std::chrono::seconds{20}));
    auto response = call.get();
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::cancelled, response.error_kind);
    // Without the scan observing the RPC, only the entry, RTX start and final checks would run.
    EXPECT_EQ(cancel_at, checks.load());
}
TEST_F(remote_process_handler_test, escaped_task_exception_discards_job_before_next_rpc_on_thread) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1), (2), (3), (4)");
    dist::remote_process_handler handler{*db_};
    bool flushed{};
    bool thrown{};
    // The task wrapper converts only std/boost exceptions, so a non-standard exception from the
    // cancellation check inside the scan leaves the serial scheduler while the job is unfinished.
    auto run_failing = [&] {
        return handler.execute_stream(dist::serialize_remote_execution_block_proto(large_row_block()), [&] {
            if (flushed && !thrown) {
                thrown = true;
                throw 42;  // NOLINT
            }
            return false;
        }, [&](std::string_view) {
            flushed = true;
            return true;
        });
    };
    tateyama::grpc::distributed_query::remote_process_result failed{};
    EXPECT_NO_THROW(failed = run_failing());
    EXPECT_TRUE(thrown);
    EXPECT_FALSE(failed.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::internal, failed.error_kind);

    // The next RPC on the same thread must see none of the discarded work and complete normally.
    std::size_t rows{};
    auto next = handler.execute_stream(dist::serialize_remote_execution_block_proto(scan_block()), {},
        [&](std::string_view payload) {
            std::string error{};
            auto result = dist::deserialize_remote_execution_result(payload, error);
            EXPECT_TRUE(result) << error;
            if (result) rows += result->encoded_rows.size();
            return true;
        });
    EXPECT_TRUE(next.success) << next.error;
    EXPECT_EQ(4, rows);
}
TEST_F(remote_process_handler_test, stops_when_encoded_output_exceeds_limit) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    std::string sql = "INSERT INTO T VALUES ";
    for (int i = 0; i < 70; ++i) { if (i) sql += ','; sql += '(' + std::to_string(i) + ')'; }
    execute_statement(sql);
    auto block = scan_block();
    block.values = {0, 1};
    block.stream_variables = {0, 1};
    block.result_value_id = 1;
    auto literal = takatori::value::character{std::string(1024 * 1024, 'x')};
    dist::remote_scalar_expression expression{};
    expression.expression_id = 0;
    expression.kind = dist::remote_scalar_expression_kind::immediate;
    expression.type_payload = dist::encode_remote_type(takatori::type::character{takatori::type::varying});
    expression.literal_payload = dist::encode_remote_literal(literal);
    block.scalar_expressions = {expression};
    dist::remote_operator project{};
    project.operator_id = 1;
    project.kind = dist::remote_operator_kind::project;
    project.input_value_id = 0; project.output_value_id = 1;
    project.project_columns = {{1, 0}};
    auto offer = block.operators.back();
    offer.operator_id = 2; offer.input_value_id = 1;
    offer.offer->columns[0].source_variable_id = 1;
    block.operators = {block.operators.front(), project, offer};
    block.result_schema = {{0, "character()", true, dist::encode_remote_type(takatori::type::character{takatori::type::varying})}};
    auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_TRUE(response.payload.empty());
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::resource_exhausted, response.error_kind);
}
TEST_F(remote_process_handler_test, stream_result_total_may_exceed_message_limit) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    std::string sql = "INSERT INTO T VALUES ";
    for (int i = 0; i < 70; ++i) { if (i) sql += ','; sql += '(' + std::to_string(i) + ')'; }
    execute_statement(sql);
    auto block = scan_block();
    block.values = {0, 1};
    block.stream_variables = {0, 1};
    block.result_value_id = 1;
    auto literal = takatori::value::character{std::string(1024 * 1024, 'x')};
    dist::remote_scalar_expression expression{};
    expression.expression_id = 0;
    expression.kind = dist::remote_scalar_expression_kind::immediate;
    expression.type_payload = dist::encode_remote_type(takatori::type::character{takatori::type::varying});
    expression.literal_payload = dist::encode_remote_literal(literal);
    block.scalar_expressions = {expression};
    dist::remote_operator project{};
    project.operator_id = 1;
    project.kind = dist::remote_operator_kind::project;
    project.input_value_id = 0; project.output_value_id = 1;
    project.project_columns = {{1, 0}};
    auto offer = block.operators.back();
    offer.operator_id = 2; offer.input_value_id = 1;
    offer.offer->columns[0].source_variable_id = 1;
    block.operators = {block.operators.front(), project, offer};
    block.result_schema = {{0, "character()", true, dist::encode_remote_type(takatori::type::character{takatori::type::varying})}};
    std::size_t chunks{}, rows{}, total{};
    auto response = dist::remote_process_handler{*db_}.execute_stream(
        dist::serialize_remote_execution_block_proto(block), {}, [&](std::string_view payload) {
            ++chunks;
            total += payload.size();
            EXPECT_LT(payload.size(), std::size_t{64} * 1024U * 1024U);
            std::string error{};
            auto result = dist::deserialize_remote_execution_result(payload, error);
            EXPECT_TRUE(result) << error;
            if (result) rows += result->encoded_rows.size();
            // Model a slow consumer: the synchronous sink provides backpressure.
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            return true;
        });
    EXPECT_TRUE(response.success) << response.error;
    EXPECT_TRUE(response.payload.empty());
    EXPECT_GT(chunks, 1);
    EXPECT_EQ(70, rows);
    EXPECT_GT(total, std::size_t{64} * 1024U * 1024U);
}

TEST_F(remote_process_handler_test, sink_exception_is_internal_failure_not_client_cancellation) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY, C1 BIGINT)");
    execute_statement("INSERT INTO T VALUES (10, 99)");
    dist::remote_process_handler handler{*db_};
    auto response = handler.execute_stream(dist::serialize_remote_execution_block_proto(scan_block()), {},
        [](std::string_view) -> bool { throw std::runtime_error("sink failed"); });
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::internal, response.error_kind);
}

TEST_F(remote_process_handler_test, resolves_sales_output_in_schema_column_order) {
    execute_statement("CREATE TABLE sales(sale_number BIGINT PRIMARY KEY, amount BIGINT)");
    execute_statement("INSERT INTO sales VALUES (308, 20000), (101, 12000), (205, 18000)");
    auto block = scan_block();
    // Stream variable IDs are separate from output column IDs and column positions.
    block.stream_variables = {123, 987};
    block.external_relations = {42};
    block.operators[0].source->table_name = "sales";
    block.operators[0].source->index_name = "sales";
    block.operators[0].source->columns = {{"sale_number", 123}, {"amount", 987}};
    block.operators.back().offer = dist::remote_offer{42, {{123, 1055}, {987, 1900}}};
    // Both the normal and reversed schemas must resolve sparse IDs, not use IDs as positions.
    for (bool reversed : {false, true}) {
        block.result_schema = reversed
            ? std::vector<dist::remote_result_column>{{1900, "int8()", true}, {1055, "int8()", false}}
            : std::vector<dist::remote_result_column>{{1055, "int8()", false}, {1900, "int8()", true}};
        auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
        ASSERT_TRUE(response.success) << response.error;
        std::string error{};
        auto result = dist::deserialize_remote_execution_result(response.payload, error);
        ASSERT_TRUE(result) << error;
        ASSERT_EQ(3, result->rows.size());
        auto number_position = reversed ? 1 : 0;
        auto amount_position = reversed ? 0 : 1;
        EXPECT_EQ(101, result->rows[0][number_position]);
        EXPECT_EQ(12000, result->rows[0][amount_position]);
        EXPECT_EQ(205, result->rows[1][number_position]);
        EXPECT_EQ(18000, result->rows[1][amount_position]);
        EXPECT_EQ(308, result->rows[2][number_position]);
        EXPECT_EQ(20000, result->rows[2][amount_position]);
    }
}
TEST_F(remote_process_handler_test, resolves_sparse_external_ids_and_schema_column_order) {
    execute_statement("CREATE TABLE sales(sale_number BIGINT PRIMARY KEY, amount BIGINT)");
    execute_statement("INSERT INTO sales VALUES (101, 12000), (205, NULL)");
    auto block = scan_block();
    block.stream_variables = {123, 987};
    block.external_relations = {42};
    block.operators[0].source->table_name = "sales";
    block.operators[0].source->index_name = "sales";
    block.operators[0].source->columns = {{"sale_number", 123}, {"amount", 987}};
    // Reverse the output order while retaining the documentation's sparse column IDs.
    block.result_schema = {{1900, "int8()", true}, {1055, "int8()", false}};
    block.operators.back().offer = dist::remote_offer{42, {{123, 1055}, {987, 1900}}};
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    ASSERT_TRUE(response.success) << response.error;
    std::string error{};
    auto result = dist::deserialize_remote_execution_result(response.payload, error);
    ASSERT_TRUE(result) << error;
    ASSERT_EQ(2, result->rows.size());
    EXPECT_EQ(12000, result->rows[0][0]); EXPECT_EQ(101, result->rows[0][1]);
    EXPECT_FALSE(result->rows[1][0]); EXPECT_EQ(205, result->rows[1][1]);
}
TEST_F(remote_process_handler_test, rejects_unknown_external_binding_before_execution) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    auto block = scan_block();
    block.operators.back().offer->external_relation_id = 999;
    auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
}
TEST_F(remote_process_handler_test, rejects_unknown_output_column_before_execution) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    auto block = scan_block();
    block.external_relations = {42};
    block.result_schema[0].column_id = 1055;
    // The destination column is absent from result_schema, although the relation is valid.
    block.operators.back().offer = dist::remote_offer{42, {{0, 1055}, {0, 1900}}};
    auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
    EXPECT_NE(std::string::npos, response.error.find("result column"));
    EXPECT_NE(std::string::npos, response.error.find("1900"));
}
TEST_F(remote_process_handler_test, rejects_duplicate_and_missing_output_columns) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    for (bool duplicate : {false, true}) {
        auto block = scan_block();
        if (duplicate) block.operators.back().offer->columns.push_back({0, 0});
        else block.result_schema.push_back({42, "int8()", false});
        auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
        EXPECT_FALSE(response.success);
        EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
    }
}
TEST_F(remote_process_handler_test, declared_output_type_does_not_override_inferred_source_type) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES(42)");
    auto block = scan_block();
    // A valid but wrong schema must not coerce BIGINT to INTEGER via type hints.
    block.result_schema[0].type = "int4()";
    block.result_schema[0].type_payload = dist::encode_remote_type(takatori::type::int4{});
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
    EXPECT_EQ("remote result schema type mismatch", response.error);
}

TEST_F(remote_process_handler_test, reads_worker_table_in_independent_rtx) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10), (20)");
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(scan_block()));
    ASSERT_TRUE(response.success) << response.error;
    std::string error{};
    auto result = dist::deserialize_remote_execution_result(response.payload, error);
    ASSERT_TRUE(result) << error;
    ASSERT_EQ(2, result->rows.size());
    EXPECT_EQ(10, result->rows[0][0]);
    EXPECT_EQ(20, result->rows[1][0]);
}
TEST_F(remote_process_handler_test, filters_and_projects_worker_rows) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10), (20)");
    auto block = scan_block();
    block.values = {0, 1, 2};
    block.stream_variables = {0, 1};
    block.result_value_id = 2;
    block.scalar_expressions = {
        {0, dist::remote_scalar_expression_kind::variable_reference, {}, {}, {}, {}, 0, {}},
        {1, dist::remote_scalar_expression_kind::immediate, {}, "int8", "10", "int8()", {}, {}},
        {2, dist::remote_scalar_expression_kind::compare, "greater", {}, {}, {}, {}, {0, 1}},
        {3, dist::remote_scalar_expression_kind::binary, "add", {}, {}, {}, {}, {0, 1}},
    };
    dist::remote_operator filter{};
    filter.operator_id = 1;
    filter.kind = dist::remote_operator_kind::filter;
    filter.input_value_id = 0;
    filter.output_value_id = 1;
    filter.filter_expression_id = 2;
    dist::remote_operator project{};
    project.operator_id = 2;
    project.kind = dist::remote_operator_kind::project;
    project.input_value_id = 1;
    project.output_value_id = 2;
    project.project_columns = {{1, 3}};
    auto offer = block.operators.back();
    offer.operator_id = 3;
    offer.input_value_id = 2;
    offer.offer->columns[0].source_variable_id = 1;
    block.operators = {block.operators.front(), filter, project, offer};
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    ASSERT_TRUE(response.success) << response.error;
    std::string error{};
    auto result = dist::deserialize_remote_execution_result(response.payload, error);
    ASSERT_TRUE(result) << error;
    ASSERT_EQ(1, result->rows.size());
    EXPECT_EQ(30, result->rows[0][0]);
}
TEST_F(remote_process_handler_test, evaluates_and_returns_all_output_types) {
    namespace t = takatori::type;
    namespace v = takatori::value;
    struct sample { std::shared_ptr<t::data const> type; std::unique_ptr<v::data> value; };
    std::vector<sample> samples{};
    samples.push_back({std::make_shared<t::boolean>(), std::make_unique<v::boolean>(true)});
    samples.push_back({std::make_shared<t::int4>(), std::make_unique<v::int4>(123456)});
    samples.push_back({std::make_shared<t::int8>(), std::make_unique<v::int8>(123456789012LL)});
    samples.push_back({std::make_shared<t::float4>(), std::make_unique<v::float4>(1.25F)});
    samples.push_back({std::make_shared<t::float8>(), std::make_unique<v::float8>(1.125)});
    samples.push_back({std::make_shared<t::decimal>(10, 2), std::make_unique<v::decimal>("12.34")});
    samples.push_back({std::make_shared<t::character>(t::varying, 40), std::make_unique<v::character>("abc-日本語")});
    samples.push_back({std::make_shared<t::octet>(t::varying, 40), std::make_unique<v::octet>(std::string{"a\0b", 3})});
    samples.push_back({std::make_shared<t::date>(), std::make_unique<v::date>(takatori::datetime::date{2024, 1, 2})});
    samples.push_back({std::make_shared<t::time_of_day>(), std::make_unique<v::time_of_day>(takatori::datetime::time_of_day{12, 34, 56})});
    samples.push_back({std::make_shared<t::time_point>(), std::make_unique<v::time_point>(takatori::datetime::time_point{takatori::datetime::date{2024, 1, 2}, takatori::datetime::time_of_day{12, 34, 56}})});
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10)");
    dist::remote_process_handler handler{*db_};
    for (auto const& sample : samples) {
        auto block = scan_block();
        block.values = {0, 1};
        block.stream_variables = {0, 1};
        block.result_value_id = 1;
        block.result_schema[0].type_payload = dist::encode_remote_type(*sample.type);
        block.scalar_expressions = {{0, dist::remote_scalar_expression_kind::immediate}};
        block.scalar_expressions[0].type_payload = dist::encode_remote_type(*sample.type);
        block.scalar_expressions[0].literal_payload = dist::encode_remote_literal(*sample.value);
        dist::remote_operator project{};
        project.operator_id = 1;
        project.kind = dist::remote_operator_kind::project;
        project.input_value_id = 0;
        project.output_value_id = 1;
        project.project_columns = {{1, 0}};
        auto offer = block.operators.back();
        offer.operator_id = 2;
        offer.input_value_id = 1;
        offer.offer->columns[0].source_variable_id = 1;
        block.operators = {block.operators.front(), project, offer};
        auto response = handler(dist::serialize_remote_execution_block_proto(block));
        ASSERT_TRUE(response.success) << *sample.type << ": " << response.error;
        std::string error{};
        auto result = dist::deserialize_remote_execution_result(response.payload, error);
        ASSERT_TRUE(result) << error;
        ASSERT_EQ(1, result->encoded_rows.size());
        char const* pos = result->encoded_rows[0].data();
        auto* end = pos + result->encoded_rows[0].size();
        ASSERT_EQ(1, serializer::read_row_begin(pos, end));
        auto actual = dist::decode_remote_literal(std::string_view{pos, static_cast<std::size_t>(end-pos)}, *sample.type);
        ASSERT_TRUE(actual);
        EXPECT_EQ(*sample.value, *actual) << *sample.type;
    }
}
TEST_F(remote_process_handler_test, returns_native_sql_error_details) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10)");
    auto block = scan_block();
    block.values = {0, 1};
    block.stream_variables = {0, 1};
    block.result_value_id = 1;
    block.scalar_expressions = {
        {0, dist::remote_scalar_expression_kind::variable_reference, {}, {}, {}, {}, 0, {}},
        {1, dist::remote_scalar_expression_kind::immediate, {}, "int8", "0", "int8()", {}, {}},
        {2, dist::remote_scalar_expression_kind::binary, "divide", {}, {}, {}, {}, {0, 1}},
    };
    dist::remote_operator project{};
    project.operator_id = 1;
    project.kind = dist::remote_operator_kind::project;
    project.input_value_id = 0;
    project.output_value_id = 1;
    project.project_columns = {{1, 2}};
    auto offer = block.operators.back();
    offer.operator_id = 2;
    offer.input_value_id = 1;
    offer.offer->columns[0].source_variable_id = 1;
    block.operators = {block.operators.front(), project, offer};
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::sql_error, response.error_kind);
    proto::sql::response::Error error{};
    ASSERT_TRUE(error.ParseFromString(response.sql_error_details));
    EXPECT_EQ(proto::sql::error::VALUE_EVALUATION_EXCEPTION, error.code());
    EXPECT_FALSE(error.detail().empty());
}
TEST_F(remote_process_handler_test, evaluates_supported_intermediate_records) {
    namespace t = takatori::type;
    namespace v = takatori::value;
    using kind = dist::remote_scalar_expression_kind;
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10)");
    dist::remote_process_handler handler{*db_};
    auto run = [&](t::data const& input_type, v::data const& literal, std::string operation,
                   t::data const& output_type, v::data const& expected) {
        auto block = scan_block();
        block.values = {0, 1, 2};
        block.stream_variables = {0, 1, 2};
        block.result_value_id = 2;
        block.result_schema[0].type_payload = dist::encode_remote_type(output_type);
        block.scalar_expressions = {{0, kind::immediate}, {1, kind::variable_reference}, {2, kind::unary, operation}};
        block.scalar_expressions[0].type_payload = dist::encode_remote_type(input_type);
        block.scalar_expressions[0].literal_payload = dist::encode_remote_literal(literal);
        block.scalar_expressions[1].variable_id = 1;
        block.scalar_expressions[2].operand_expression_ids = {1};
        dist::remote_operator first{};
        first.operator_id = 1; first.kind = dist::remote_operator_kind::project;
        first.input_value_id = 0; first.output_value_id = 1; first.project_columns = {{1, 0}};
        auto second = first;
        second.operator_id = 2; second.input_value_id = 1; second.output_value_id = 2; second.project_columns = {{2, 2}};
        if (operation == "concat_length") {
            block.scalar_expressions[2].kind = kind::binary;
            block.scalar_expressions[2].operator_kind = "concat";
            block.scalar_expressions[2].operand_expression_ids = {1, 1};
            block.scalar_expressions.push_back({3, kind::unary, "length", {}, {}, {}, {}, {2}});
            second.project_columns = {{2, 3}};
        }
        if (operation == "cast") {
            block.scalar_expressions[2].kind = kind::cast;
            block.scalar_expressions[2].operator_kind = "error";
            block.scalar_expressions[2].type_payload = dist::encode_remote_type(output_type);
        }

        auto offer = block.operators.back();
        offer.operator_id = 3; offer.input_value_id = 2; offer.offer->columns[0].source_variable_id = 2;
        block.operators = {block.operators.front(), first, second, offer};
        auto response = handler(dist::serialize_remote_execution_block_proto(block));
        ASSERT_TRUE(response.success) << response.error;
        std::string error;
        auto result = dist::deserialize_remote_execution_result(response.payload, error);
        ASSERT_TRUE(result) << error;
        ASSERT_EQ(1, result->encoded_rows.size());
        char const* pos = result->encoded_rows[0].data();
        auto* end = pos + result->encoded_rows[0].size();
        ASSERT_EQ(1, serializer::read_row_begin(pos, end));
        auto actual = dist::decode_remote_literal(std::string_view{pos, static_cast<std::size_t>(end-pos)}, output_type);
        ASSERT_TRUE(actual);
        EXPECT_EQ(expected, *actual);
    };
    run(t::character{t::varying, 5}, v::character{"abc"}, "length", t::int4{}, v::int4{3});
    run(t::character{t::varying, 5}, v::character{"abc"}, "concat_length", t::int4{}, v::int4{6});
}

TEST_F(remote_process_handler_test, rejects_boolean_and_octet_operation_extensions) {
    namespace t = takatori::type;
    namespace v = takatori::value;
    using kind = dist::remote_scalar_expression_kind;
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (10)");
    auto run = [&](std::vector<dist::remote_scalar_expression> expressions, t::data const& type, v::data const&) {
        auto block = scan_block();
        block.values = {0, 1}; block.stream_variables = {0, 1}; block.result_value_id = 1;
        block.result_schema[0].type_payload = dist::encode_remote_type(type);
        block.scalar_expressions = std::move(expressions);
        dist::remote_operator project{};
        project.operator_id = 1; project.kind = dist::remote_operator_kind::project;
        project.input_value_id = 0; project.output_value_id = 1;
        project.project_columns = {{1, block.scalar_expressions.back().expression_id}};
        auto offer = block.operators.back(); offer.operator_id = 2; offer.input_value_id = 1;
        offer.offer->columns[0].source_variable_id = 1;
        block.operators = {block.operators.front(), project, offer};
        auto response = dist::remote_process_handler{*db_}(dist::serialize_remote_execution_block_proto(block));
        EXPECT_FALSE(response.success);
        EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::unsupported, response.error_kind);
    };
    std::vector<dist::remote_scalar_expression> operands{{0, kind::immediate}, {1, kind::immediate}};
    for (std::size_t i = 0; i < 2; ++i) {
        operands[i].type_payload = dist::encode_remote_type(t::boolean{});
        operands[i].literal_payload = dist::encode_remote_literal(v::boolean{i == 1});
    }
    for (auto const& [operation, expected] : std::vector<std::pair<std::string, bool>>{
        {"equal", false}, {"not_equal", true}, {"less", true}, {"less_equal", true},
        {"greater", false}, {"greater_equal", false}, {"is_distinct_from", true}, {"is_not_distinct_from", false}}) {
        SCOPED_TRACE(operation);
        auto expressions = operands; expressions.push_back({2, kind::compare, operation});
        expressions.back().operand_expression_ids = {0, 1};
        run(std::move(expressions), t::boolean{}, v::boolean{expected});
    }
    auto cast = operands;
    cast.push_back({2, kind::cast, "error"});
    cast.back().operand_expression_ids = {0};
    cast.back().type_payload = dist::encode_remote_type(t::character{t::varying, 5});
    run(std::move(cast), t::character{t::varying, 5}, v::character{"false"});
    operands[0].type_payload = operands[1].type_payload = dist::encode_remote_type(t::octet{t::varying, 3});
    operands[0].literal_payload = dist::encode_remote_literal(v::octet{std::string{"a\0", 2}});
    operands[1].literal_payload = dist::encode_remote_literal(v::octet{"b"});
    operands.push_back({2, kind::binary, "concat"}); operands.back().operand_expression_ids = {0, 1};
    run(operands, t::octet{t::varying, 6}, v::octet{std::string{"a\0b", 3}});
    operands.back().kind = kind::unary;
    operands.back().operator_kind = "length";
    operands.back().operand_expression_ids = {0};
    run(std::move(operands), t::int4{}, v::int4{2});
}
TEST_F(remote_process_handler_test, remote_cast_preserves_existing_sql_limits) {
    namespace t = takatori::type;
    namespace expr = executor::expr;
    using data::any;
    memory::lifo_paged_memory_resource resource{std::addressof(global::page_pool())};
    expr::evaluator_context context{&resource};
    EXPECT_FALSE(global::config_pool()->support_boolean());
    EXPECT_FALSE(global::config_pool()->support_smallint());
    auto boolean = any{std::in_place_type<bool>, true};
    auto small = any{std::in_place_type<std::int32_t>, 42};
    EXPECT_TRUE(expr::details::conduct_cast(context, t::boolean{}, t::character{t::varying, 5}, boolean).error());
    for (auto const& type : std::vector<std::shared_ptr<t::data>>{std::make_shared<t::int1>(), std::make_shared<t::int2>()}) {
        EXPECT_TRUE(expr::details::conduct_cast(context, *type, t::int8{}, small).error());
    }
    auto date = any{std::in_place_type<takatori::datetime::date>, takatori::datetime::date{2024, 2, 29}};
    EXPECT_TRUE(expr::details::conduct_cast(context, t::date{}, t::time_point{}, date).error());
    auto text = any{std::in_place_type<accessor::text>, accessor::text{&resource, "2024-02-29"}};
    auto result = expr::details::conduct_cast(context, t::character{t::varying, 10}, t::date{}, text);
    ASSERT_TRUE(result) << context;
    EXPECT_EQ(date, result);

}
TEST_F(remote_process_handler_test, covers_analyzer_accepted_cast_pairs) {
    namespace t = takatori::type;
    namespace v = takatori::value;
    namespace expr = executor::expr;
    struct sample { std::shared_ptr<t::data> type; std::unique_ptr<v::data> value; };
    std::vector<sample> samples{};
    samples.push_back({std::make_shared<t::int4>(), std::make_unique<v::int4>(1)});
    samples.push_back({std::make_shared<t::int8>(), std::make_unique<v::int8>(1)});
    samples.push_back({std::make_shared<t::float4>(), std::make_unique<v::float4>(1)});
    samples.push_back({std::make_shared<t::float8>(), std::make_unique<v::float8>(1)});
    samples.push_back({std::make_shared<t::decimal>(10, 2), std::make_unique<v::decimal>("1")});
    samples.push_back({std::make_shared<t::character>(t::varying, 100), std::make_unique<v::character>("1")});
    samples.push_back({std::make_shared<t::octet>(t::varying, 40), std::make_unique<v::octet>("a")});
    samples.push_back({std::make_shared<t::date>(), std::make_unique<v::date>(takatori::datetime::date{2024, 1, 2})});
    samples.push_back({std::make_shared<t::time_of_day>(), std::make_unique<v::time_of_day>(takatori::datetime::time_of_day{12, 34, 56})});
    samples.push_back({std::make_shared<t::time_point>(), std::make_unique<v::time_point>(takatori::datetime::time_point{takatori::datetime::date{2024, 1, 2}, takatori::datetime::time_of_day{12, 34, 56}})});
    samples.push_back({std::make_shared<t::time_of_day>(t::with_time_zone), std::make_unique<v::time_of_day>(takatori::datetime::time_of_day{12, 34, 56})});
    samples.push_back({std::make_shared<t::time_point>(t::with_time_zone), std::make_unique<v::time_point>(takatori::datetime::time_point{takatori::datetime::date{2024, 1, 2}, takatori::datetime::time_of_day{12, 34, 56}})});
    auto tx = utils::create_transaction(*db_, true, false);
    auto transaction = api::get_transaction_context(*tx);
    transaction->start_time(transaction_context::clock::now());
    memory::lifo_paged_memory_resource resource{std::addressof(global::page_pool())};
    expr::evaluator_context context{&resource, transaction.get()};
    expr::details::ensure_decimal_context();
    std::size_t checked{};
    for (auto const& source : samples) {
        auto value = utils::as_any(*source.value, *source.type, &resource);
        for (auto const& target : samples) {
            if (yugawara::type::is_cast_convertible(*source.type, *target.type) != true) continue;
            SCOPED_TRACE(::testing::Message{} << *source.type << " -> " << *target.type);
            auto input = value;
            if (source.type->kind() == t::type_kind::character) {
                std::string text = "1";
                switch (target.type->kind()) {
                    case t::type_kind::boolean: text = "true"; break;
                    case t::type_kind::date: text = "2024-01-02"; break;
                    case t::type_kind::time_of_day: text = "12:34:56"; break;
                    case t::type_kind::time_point: text = "2024-01-02 12:34:56"; break;
                    case t::type_kind::octet: text = "01"; break;
                    default: break;
                }
                input = data::any{std::in_place_type<accessor::text>, accessor::text{&resource, text}};
            }
            auto result = expr::details::conduct_cast(context, *source.type, *target.type, input);
            auto source_kind = source.type->kind();
            if (source_kind == t::type_kind::date || source_kind == t::type_kind::time_of_day || source_kind == t::type_kind::time_point) {
                EXPECT_TRUE(result.error()) << context;
            } else {
                EXPECT_TRUE(result) << context;
            }
            ++checked;
        }
    }
    EXPECT_GE(checked, 50);
    RecordProperty("cast_pairs_checked", static_cast<int>(checked));
}
TEST_F(remote_process_handler_test, rejects_unsupported_types_before_execution) {
    for (auto const* name : {"int1", "int2", "bit", "datetime_interval"}) {
        proto::distributed_query::PortableType type{};
        type.set_kind(name);
        for (bool output : {false, true}) {
            for (bool typed_payload : {false, true}) {
                auto block = scan_block();
                if (output) {
                    block.result_schema[0].type = std::string{name} + "()";
                    if (typed_payload) block.result_schema[0].type_payload = type.SerializeAsString();
                } else {
                    block.scalar_expressions = {{0, dist::remote_scalar_expression_kind::immediate}};
                    auto& expression = block.scalar_expressions[0];
                    expression.type = std::string{name} + "()";
                    if (typed_payload) expression.type_payload = type.SerializeAsString();
                    expression.literal_payload = dist::encode_remote_literal(takatori::value::unknown{takatori::value::unknown_kind::null});
                }
                dist::remote_process_handler handler{*db_};
                auto response = handler(dist::serialize_remote_execution_block_proto(block));
                EXPECT_FALSE(response.success);
                EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::unsupported, response.error_kind);
            }
        }
    }
}
TEST_F(remote_process_handler_test, rejects_unsupported_output_before_runtime_layout) {
    auto block = scan_block();
    proto::distributed_query::PortableType unsupported_type{};
    unsupported_type.set_kind("int1");
    block.result_schema[0].type_payload = unsupported_type.SerializeAsString();
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::unsupported, response.error_kind);
}
TEST_F(remote_process_handler_test, rejects_blob_source_even_when_not_offered) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY, C1 BLOB)");
    auto block = scan_block();
    block.stream_variables = {0, 1};
    block.operators[0].source->columns.push_back({"C1", 1});
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::unsupported, response.error_kind);
}
TEST_F(remote_process_handler_test, rejects_cyclic_expression_graph) {
    auto block = scan_block();
    block.scalar_expressions = {{1, dist::remote_scalar_expression_kind::unary, "plus", {}, {}, {}, {}, {1}}};
    dist::remote_process_handler handler{*db_};
    auto response = handler(dist::serialize_remote_execution_block_proto(block));
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
}
TEST_F(remote_process_handler_test, rejects_invalid_payload) {
    dist::remote_process_handler handler{*db_};
    auto response = handler("not-protobuf");
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::invalid_request, response.error_kind);
}
TEST_F(remote_process_handler_test, cancels_before_execution) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)");
    dist::remote_process_handler handler{*db_};
    auto response = handler.execute(dist::serialize_remote_execution_block_proto(scan_block()), [] { return true; });
    EXPECT_FALSE(response.success);
    EXPECT_EQ(tateyama::grpc::distributed_query::remote_process_error_kind::cancelled, response.error_kind);
}

} // namespace jogasaki::testing
