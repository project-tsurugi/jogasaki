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
#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_execution_block_decoder.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/executor/common/foreign_catalog_statement.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/runner.h>

namespace jogasaki::testing {
namespace {
namespace wire = tateyama::grpc::proto;
class catalog_worker : public wire::DistributedQueryService::Service {
public:
    std::int64_t value{};
    std::atomic_int calls{};
    grpc::Status ExecuteRemoteProcess(grpc::ServerContext*, wire::RemoteProcessRequest const* request, grpc::ServerWriter<wire::RemoteProcessResult>* writer) override {
        ++calls;
        dist::remote_execution_result result{};
        auto block = dist::decode_remote_execution_block(request->execution_block().SerializeAsString());
        if (!block) return {grpc::StatusCode::INVALID_ARGUMENT, block.error};
        if (block.block->operators.front().source->definition_id != std::optional<std::uint64_t>{1257}) {
            return {grpc::StatusCode::INVALID_ARGUMENT, "unexpected storage definition identity"};
        }
        result.columns = block.block->result_schema; result.rows = {{value}};
        wire::RemoteProcessResult response{};
        std::string error{};
        auto encoded = dist::deserialize_remote_execution_result(dist::serialize_remote_execution_result(result), error);
        if (!encoded) return {grpc::StatusCode::INTERNAL, error};
        for (auto const& row : encoded->encoded_rows) response.add_record_list(row);
        return writer->Write(response) ? grpc::Status::OK : grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
    }
};
}
class remote_catalog_sql_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }
    void SetUp() override {
        if (!dist::distributed_query_fixtures_available()) GTEST_SKIP();
        unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_SERVER");
        int first{}, second{};
        grpc::ServerBuilder a{}; a.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &first); a.RegisterService(&a_); server_a_ = a.BuildAndStart();
        grpc::ServerBuilder b{}; b.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &second); b.RegisterService(&b_); server_b_ = b.BuildAndStart();
        ASSERT_TRUE(server_a_); ASSERT_TRUE(server_b_); a_.value = 101; b_.value = 202;
        path_ = std::filesystem::temp_directory_path() / ("jogasaki-catalog-sql-" + std::to_string(getpid()) + ".json");
        using json = nlohmann::json;
        json table{{"name", "T"}, {"definition_id", 1234}, {"index_definition_id", 1257}, {"columns", json::array({{{"name", "C0"}, {"type", "bigint"}, {"nullable", false}}})}, {"primary_key", json::array({"C0"})}};
        json catalogs = json::array();
        for (auto const& location : {std::pair{"worker_a", "schema_a"}, std::pair{"worker_b", "schema_b"}}) {
            catalogs.push_back({{"name", location.first}, {"server", location.first}, {"schemas", json::array({{{"name", location.second}, {"tables", json::array({table})}}})}});
        }
        std::ofstream{path_} << json{{"version", 1}, {"catalogs", catalogs}};
        auto cfg = std::make_shared<configuration>(); cfg->thread_pool_size(4); cfg->distributed_query_coordinator_enabled(true);
        cfg->distributed_query_remote_servers({
            {"worker_a", "127.0.0.1:" + std::to_string(first), false, dist::root_certificate_source::none, {}},
            {"worker_b", "127.0.0.1:" + std::to_string(second), false, dist::root_certificate_source::none, {}}});
        cfg->distributed_query_catalog({dist::remote_catalog_source::fixture, path_.string()});
        db_setup(cfg);
    }
    void TearDown() override {
        if (db_) db_teardown();
        if (server_a_) server_a_->Shutdown(); if (server_b_) server_b_->Shutdown();
        if (!path_.empty()) std::filesystem::remove(path_);
    }
    std::int64_t query(std::string_view text) {
        auto tx = utils::create_transaction(*db_, true, false);
        std::vector<mock::basic_record> records{}; execute_query(text, *tx, records);
        EXPECT_EQ(status::ok, tx->commit());
        EXPECT_EQ(1, records.size());
        return records.empty() ? -1 : records[0].get_value<std::int64_t>(0);
    }
protected:
    catalog_worker a_{}, b_{};
    std::unique_ptr<grpc::Server> server_a_{}, server_b_{};
    std::filesystem::path path_{};
};
TEST_F(remote_catalog_sql_test, request_factory_connects_catalog_ddl_to_database_registry) {
    auto request = api::impl::create_request_context(*db_impl(), {}, {}, {}, {}, false, {});
    auto job_id = request->job()->id();
    EXPECT_EQ(db_impl()->remote_catalogs_manager(), request->remote_catalogs_manager());
    executor::common::drop_foreign_catalog drop{{"worker_a", false}};
    request->stmt_scheduler()->schedule(drop, *request);
    EXPECT_EQ(status::ok, request->status_code());
    EXPECT_FALSE(db_impl()->remote_catalogs_manager()->find_catalog("worker_a"));
    db_impl()->task_scheduler()->unregister_job(job_id);
}

TEST_F(remote_catalog_sql_test, executable_keeps_catalog_and_routing_after_drop) {
    std::unique_ptr<api::executable_statement> statement{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT C0 FROM schema_a.T", statement));
    auto manager = db_impl()->remote_catalogs_manager();
    ASSERT_TRUE(manager);
    ASSERT_EQ(status::ok, manager->drop("worker_a"));
    EXPECT_FALSE(manager->find_catalog("worker_a"));
    auto tx = utils::create_transaction(*db_, true, false);
    std::unique_ptr<api::result_set> results{};
    std::shared_ptr<error::error_info> error{};
    std::shared_ptr<request_statistics> statistics{};
    ASSERT_EQ(status::ok, executor::execute(*db_impl(), api::get_transaction_context(*tx),
        *statement, results, error, statistics, request_info{}));
    ASSERT_TRUE(results);
    auto iterator = results->iterator();
    ASSERT_TRUE(iterator->has_next());
    EXPECT_EQ(101, iterator->next()->get_int8(0));
    EXPECT_FALSE(iterator->has_next());
    results->close();
    ASSERT_EQ(status::ok, tx->commit());
    EXPECT_EQ(1, a_.calls);
    std::unique_ptr<api::executable_statement> later{};
    EXPECT_NE(status::ok, db_->create_executable("SELECT C0 FROM schema_a.T", later));
}

TEST_F(remote_catalog_sql_test, prepared_statement_resolves_with_original_snapshot_after_drop) {
    auto manager = db_impl()->remote_catalogs_manager();
    ASSERT_TRUE(manager);
    std::weak_ptr<dist::remote_catalog_snapshot const> original = manager->snapshot();
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("SELECT C0 FROM schema_a.T", prepared));
    ASSERT_EQ(status::ok, manager->drop("worker_a"));
    EXPECT_FALSE(manager->find_catalog("worker_a"));
    ASSERT_FALSE(original.expired());
    EXPECT_EQ(0, a_.calls);

    std::unique_ptr<api::executable_statement> statement{};
    ASSERT_EQ(status::ok, db_->resolve(prepared, std::shared_ptr{api::create_parameter_set()}, statement));
    ASSERT_TRUE(statement);
    EXPECT_EQ(0U, api::impl::get_impl(*statement).body()->mirrors()->mutable_storage_operation().storages().size());
    EXPECT_EQ(0, a_.calls);
    EXPECT_EQ(original.lock(), api::impl::get_impl(*statement).body()->mirrors()->remote_catalogs());
    EXPECT_NE(manager->snapshot(), api::impl::get_impl(*statement).body()->mirrors()->remote_catalogs());
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
    ASSERT_FALSE(original.expired());

    auto tx = utils::create_transaction(*db_, true, false);
    std::unique_ptr<api::result_set> results{};
    std::shared_ptr<error::error_info> error{};
    std::shared_ptr<request_statistics> statistics{};
    ASSERT_EQ(status::ok, executor::execute(*db_impl(), api::get_transaction_context(*tx),
        *statement, results, error, statistics, request_info{}));
    ASSERT_TRUE(results);
    auto iterator = results->iterator();
    ASSERT_TRUE(iterator->has_next());
    EXPECT_EQ(101, iterator->next()->get_int8(0));
    EXPECT_FALSE(iterator->has_next());
    results->close();
    ASSERT_EQ(status::ok, tx->commit());
    EXPECT_EQ(1, a_.calls);
    EXPECT_EQ(0, b_.calls);

    api::statement_handle later{};
    EXPECT_NE(status::ok, db_->prepare("SELECT C0 FROM schema_a.T", later));
}

TEST_F(remote_catalog_sql_test, occ_remote_failure_completes_graph) {
    auto tx = utils::create_transaction(*db_);
    status result{};
    builder().text("SELECT C0 FROM schema_a.T").tx(*tx).expect_error(true).st(result).run();
    EXPECT_EQ(status::err_unsupported, result);
    EXPECT_EQ(0, a_.calls);
}
TEST_F(remote_catalog_sql_test, host_variables_are_rejected_before_rpc) {
    api::statement_handle statement{};
    std::unordered_map<std::string, api::field_type_kind> variables{{"key", api::field_type_kind::int8}};
    EXPECT_EQ(status::err_unsupported, db_->prepare("SELECT C0 FROM schema_a.T WHERE C0 = :key", variables, statement));
    EXPECT_EQ(0, a_.calls); EXPECT_EQ(0, b_.calls);
}
TEST_F(remote_catalog_sql_test, remote_write_is_rejected_before_rpc) {
    api::statement_handle statement{};
    EXPECT_NE(status::ok, db_->prepare("INSERT INTO schema_a.T(C0) VALUES(999)", statement));
    EXPECT_EQ(0, a_.calls);
    EXPECT_EQ(0, b_.calls);
}
TEST_F(remote_catalog_sql_test, qualified_same_table_names_select_distinct_workers) {
    EXPECT_EQ(101, query("SELECT C0 FROM schema_a.T"));
    EXPECT_EQ(202, query("SELECT C0 FROM schema_b.T"));
    EXPECT_EQ(1, a_.calls); EXPECT_EQ(1, b_.calls);
}
TEST_F(remote_catalog_sql_test, local_collision_wins_and_qualified_remote_remains_available) {
    execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY)"); execute_statement("INSERT INTO T VALUES(303)");
    std::unique_ptr<api::executable_statement> remote{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT C0 FROM schema_b.T", remote));
    EXPECT_EQ(0U, api::impl::get_impl(*remote).body()->mirrors()->mutable_storage_operation().storages().size());
    EXPECT_EQ(303, query("SELECT C0 FROM T")); EXPECT_EQ(0, a_.calls); EXPECT_EQ(0, b_.calls);
    EXPECT_EQ(202, query("SELECT C0 FROM schema_b.T")); EXPECT_EQ(1, b_.calls);
}
TEST_F(remote_catalog_sql_test, missing_table_and_multi_remote_plan_do_not_start_rpc) {
    api::statement_handle statement{};
    EXPECT_NE(status::ok, db_->prepare("SELECT C0 FROM schema_a.MISSING", statement));
    EXPECT_EQ(status::err_unsupported, db_->prepare("SELECT a.C0 FROM schema_a.T a JOIN schema_b.T b ON a.C0=b.C0", statement));
    EXPECT_EQ(0, a_.calls); EXPECT_EQ(0, b_.calls);
}

TEST_F(remote_catalog_sql_test, request_factory_keeps_original_snapshot_after_drop_and_new_request_sees_drop) {
    auto manager = db_impl()->remote_catalogs_manager();
    auto before = api::impl::create_request_context(*db_impl(), {}, {}, {}, {}, false);
    auto original = before->remote_catalogs();
    ASSERT_TRUE(original);
    ASSERT_EQ(2, original->catalogs().size());
    EXPECT_EQ(manager, before->remote_catalogs_manager());
    EXPECT_EQ(std::addressof(db_impl()->remote_server_channels()), before->remote_server_channels());
    ASSERT_EQ(status::ok, manager->drop("worker_a"));
    auto after = api::impl::create_request_context(*db_impl(), {}, {}, {}, {}, false);
    EXPECT_EQ(original, before->remote_catalogs());
    EXPECT_NE(original, after->remote_catalogs());
    ASSERT_TRUE(after->remote_catalogs());
    ASSERT_EQ(1, after->remote_catalogs()->catalogs().size());
    EXPECT_EQ("worker_b", after->remote_catalogs()->catalogs().front()->name());
    EXPECT_EQ(2, before->remote_catalogs()->catalogs().size());
    db_impl()->task_scheduler()->unregister_job(before->job()->id());
    db_impl()->task_scheduler()->unregister_job(after->job()->id());
}

}
