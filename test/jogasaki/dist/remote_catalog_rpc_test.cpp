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
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/primitive.h>
#include <takatori/util/reference_vector.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>
#include <yugawara/aggregate/provider.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/function/provider.h>
#include <yugawara/schema/catalog.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/provider.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>
#include <tateyama/grpc/distributed_query/service_adapter.h>
#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/dist/local_catalog_handler.h>
#include <jogasaki/dist/remote_catalog_command.h>
#include <jogasaki/dist/remote_catalog_decoder.h>
#include <jogasaki/dist/remote_catalog_encoder.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_function_id_registry.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>

namespace jogasaki::testing {
namespace {

namespace wire = ::tateyama::grpc::proto;
namespace dq = ::tateyama::grpc::distributed_query;
using type_ptr = std::shared_ptr<takatori::type::data const>;

enum class stream_mode { normal, fail_after_first, sequence_gap };

// one element per response, so that reassembly across fragments is always exercised
std::vector<wire::RemoteCatalogResponse> one_element_per_fragment(wire::RemoteCatalog const& catalog) {
    std::vector<wire::RemoteCatalogResponse> result{};
    auto next = [&]() -> wire::RemoteCatalog& {
        result.emplace_back();
        result.back().set_sequence_number(result.size() - 1);
        return *result.back().mutable_catalog_fragment();
    };
    for (auto const& e : catalog.schema_list()) *next().add_schema_list() = e;
    for (auto const& e : catalog.table_list()) *next().add_table_list() = e;
    for (auto const& e : catalog.column_list()) *next().add_column_list() = e;
    for (auto const& e : catalog.index_list()) *next().add_index_list() = e;
    for (auto const& e : catalog.function_list()) *next().add_function_list() = e;
    for (auto const& e : catalog.aggregate_function_list()) *next().add_aggregate_function_list() = e;
    return result;
}

/** @brief stream catalog fragments from the in-process Tateyama catalog handler. */
class catalog_server final : public wire::DistributedQueryService::Service {
public:
    std::shared_ptr<dq::remote_catalog_handler> handler{};
    std::atomic<stream_mode> mode{stream_mode::normal};
    std::atomic_int calls{};
    std::atomic_int execution_calls{};

    grpc::Status ExecuteRemoteProcess(grpc::ServerContext*, wire::RemoteProcessRequest const*,
        grpc::ServerWriter<wire::RemoteProcessResult>*) override {
        ++execution_calls;
        return {grpc::StatusCode::INTERNAL, "unexpected remote execution during catalog test"};
    }


    grpc::Status GetRemoteCatalog(
        grpc::ServerContext* context,
        wire::RemoteCatalogRequest const* request,
        grpc::ServerWriter<wire::RemoteCatalogResponse>* writer
    ) override {
        ++calls;
        {
            std::lock_guard lock{mutex_};
            metadata_ = request->metadata();
            finite_deadline_ = context->deadline() < std::chrono::system_clock::time_point::max();
        }
        auto result = (*handler)();
        if (!result.success) {
            return {result.error_kind == dq::remote_catalog_error_kind::unsupported ?
                grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, result.error};
        }
        wire::RemoteCatalog catalog{};
        if (!catalog.ParseFromString(result.catalog)) return {grpc::StatusCode::INTERNAL, "malformed catalog"};
        auto responses = one_element_per_fragment(catalog);
        for (std::size_t i = 0; i < responses.size(); ++i) {
            if (i == 1 && mode == stream_mode::fail_after_first) {
                return {grpc::StatusCode::UNAVAILABLE, "injected failure after the first fragment"};
            }
            if (i == 1 && mode == stream_mode::sequence_gap) responses[i].set_sequence_number(5);
            if (!writer->Write(responses[i])) return {grpc::StatusCode::CANCELLED, "closed"};
        }
        return grpc::Status::OK;
    }

    [[nodiscard]] wire::CatalogRequestMetadata metadata() {
        std::lock_guard lock{mutex_};
        return metadata_;
    }

    [[nodiscard]] bool finite_deadline() {
        std::lock_guard lock{mutex_};
        return finite_deadline_;
    }

private:
    std::mutex mutex_{};
    wire::CatalogRequestMetadata metadata_{};
    bool finite_deadline_{};
};

class encoding_handler final : public dq::remote_catalog_handler {
public:
    explicit encoding_handler(std::shared_ptr<yugawara::schema::catalog const> catalog) : catalog_(std::move(catalog)) {}

    dq::remote_catalog_result operator()() override {
        wire::RemoteCatalog message{};
        std::string diagnostic{};
        if (auto rc = dist::remote_catalog_encoder{}.encode(*catalog_, message, diagnostic); rc != status::ok) {
            return {false, {}, diagnostic, rc == status::err_unsupported ?
                dq::remote_catalog_error_kind::unsupported : dq::remote_catalog_error_kind::internal};
        }
        return {true, message.SerializeAsString(), {}};
    }

private:
    std::shared_ptr<yugawara::schema::catalog const> catalog_;
};

class unsupported_handler final : public dq::remote_catalog_handler {
public:
    dq::remote_catalog_result operator()() override {
        return {false, {}, "unsupported data type: bit", dq::remote_catalog_error_kind::unsupported};
    }
};

type_ptr int8_type() { return std::make_shared<takatori::type::int8>(); }

// remote_schema.T(C0 BIGINT PRIMARY KEY) with one scalar and one aggregate function
std::shared_ptr<yugawara::schema::catalog const> remote_catalog() {
    auto storage = std::make_shared<yugawara::storage::configurable_provider>();
    auto table = std::make_shared<yugawara::storage::table>(
        std::optional<yugawara::storage::table::definition_id_type>{501}, "T",
        takatori::util::reference_vector<yugawara::storage::column>{}, std::string{});
    table->columns().emplace_back("C0", int8_type(), yugawara::variable::criteria{yugawara::variable::nullity{false}},
        yugawara::storage::column_value{}, yugawara::storage::column_feature_set{});
    storage->add_table(table);
    std::vector<yugawara::storage::index::key> keys{};
    keys.emplace_back(table->columns()[0], takatori::relation::sort_direction::ascendant);
    storage->add_index(yugawara::storage::index{
        std::optional<yugawara::storage::index::definition_id_type>{502}, table, "T", std::move(keys), {},
        {yugawara::storage::index_feature::primary, yugawara::storage::index_feature::find,
            yugawara::storage::index_feature::scan, yugawara::storage::index_feature::unique}, std::string{}});
    auto functions = std::make_shared<yugawara::function::configurable_provider>();
    functions->add(yugawara::function::declaration{503, "remote_f", int8_type(), std::vector<type_ptr>{int8_type()},
        {yugawara::function::function_feature::scalar_function}});
    auto aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
    aggregates->add(yugawara::aggregate::declaration{504, "remote_agg", int8_type(), std::vector<type_ptr>{int8_type()}, true});
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "remote_schema", storage, {}, functions, aggregates});
    return std::make_shared<yugawara::schema::catalog>("remote_source", std::nullopt, schemas);
}

template <class Provider, class Declaration>
std::shared_ptr<Declaration const> first(Provider const& provider) {
    std::shared_ptr<Declaration const> result{};
    provider.each([&](std::shared_ptr<Declaration const> const& d) { if (!result) result = d; });
    return result;
}

}  // namespace

class remote_catalog_rpc_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }

    void SetUp() override {
        int port{};
        grpc::ServerBuilder builder{};
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&remote_);
        server_ = builder.BuildAndStart();
        ASSERT_TRUE(server_);
        remote_.handler = std::make_shared<encoding_handler>(remote_catalog());
        auto cfg = std::make_shared<configuration>();
        cfg->distributed_query_coordinator_enabled(true);
        cfg->distributed_query_remote_servers({
            {"worker", "127.0.0.1:" + std::to_string(port), false, dist::root_certificate_source::none, {}}});
        db_setup(cfg);
    }

    void TearDown() override {
        if (db_) db_teardown();
        if (server_) server_->Shutdown();
    }

protected:
    catalog_server remote_{};
    std::unique_ptr<grpc::Server> server_{};

    std::shared_ptr<dist::remote_catalog_manager> manager() { return db_impl()->remote_catalogs_manager(); }

    status create(std::string const& name, std::string& diagnostic) {
        return dist::execute_create_foreign_catalog({{name, "worker"}, false, false}, *manager(),
            db_impl()->remote_server_channels(), diagnostic);
    }
};

TEST_F(remote_catalog_rpc_test, create_fetches_over_rpc_and_publishes_resolvable_catalog) {
    // Default product configuration starts empty; CREATE uses RPC without a catalog-source setting.
    EXPECT_EQ(0, remote_.calls);
    ASSERT_TRUE(manager()->entries());
    EXPECT_TRUE(manager()->entries()->empty());

    std::string diagnostic{};
    ASSERT_EQ(status::ok, create("remote", diagnostic)) << diagnostic;
    EXPECT_EQ(1, remote_.calls);
    auto metadata = remote_.metadata();
    EXPECT_EQ(1U, metadata.protocol_version().major());
    EXPECT_EQ(0U, metadata.protocol_version().minor());
    EXPECT_FALSE(metadata.request_id().empty());
    EXPECT_TRUE(remote_.finite_deadline());

    auto catalog = manager()->find_catalog("remote");
    ASSERT_TRUE(catalog);
    EXPECT_EQ("remote", catalog->name());
    auto schema = catalog->schema_provider().find("remote_schema");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("T");
    ASSERT_TRUE(table);
    auto snapshot = manager()->snapshot();
    EXPECT_EQ(501U, snapshot->remote_definition(*table)->remote_definition_id);
    ASSERT_TRUE(snapshot->remote_server(*table));
    EXPECT_EQ("worker", *snapshot->remote_server(*table));
    auto index = schema->storage_provider().find_index("T");
    ASSERT_TRUE(index);
    EXPECT_EQ(502U, snapshot->remote_definition(*index)->remote_definition_id);
    auto function = first<yugawara::function::provider, yugawara::function::declaration>(schema->function_provider());
    ASSERT_TRUE(function);
    EXPECT_NE(503U, function->definition_id());
    EXPECT_EQ(503U, snapshot->remote_definition(*function)->remote_definition_id);
    auto aggregate = first<yugawara::aggregate::provider, yugawara::aggregate::declaration>(schema->set_function_provider());
    ASSERT_TRUE(aggregate);
    EXPECT_TRUE(aggregate->incremental());
    EXPECT_EQ(504U, snapshot->remote_definition(*aggregate)->remote_definition_id);

    if (!dist::distributed_query_fixtures_available()) {
        GTEST_SKIP() << "SQL name resolution of remote catalogs needs the development search-path adapter";
    }
    std::unique_ptr<api::executable_statement> statement{};
    EXPECT_EQ(status::ok, db_->create_executable("SELECT C0 FROM remote_schema.T", statement));
    // the fetch happened only once, during CREATE
    EXPECT_EQ(1, remote_.calls);
}

TEST_F(remote_catalog_rpc_test, drop_keeps_references_obtained_before) {
    std::string diagnostic{};
    ASSERT_EQ(status::ok, create("remote", diagnostic)) << diagnostic;
    auto old_snapshot = manager()->snapshot();
    auto old_catalog = manager()->find_catalog("remote");
    ASSERT_TRUE(old_catalog);
    auto ids = db_impl()->remote_function_ids();
    ASSERT_TRUE(ids);
    EXPECT_EQ(2U, ids->reserved_count());

    ASSERT_EQ(status::ok, dist::execute_drop_foreign_catalog({"remote", false}, *manager(), diagnostic));
    EXPECT_FALSE(manager()->find_catalog("remote"));
    EXPECT_TRUE(manager()->snapshot()->catalogs().empty());
    // earlier references keep the catalog, its declarations and their coordinator IDs
    ASSERT_EQ(1U, old_snapshot->catalogs().size());
    EXPECT_EQ(old_catalog, old_snapshot->catalogs().front());
    auto table = old_catalog->schema_provider().find("remote_schema")->storage_provider().find_table("T");
    ASSERT_TRUE(table);
    EXPECT_TRUE(old_snapshot->remote_server(*table));
    EXPECT_EQ(2U, ids->reserved_count());
    table.reset();
    old_catalog.reset();
    old_snapshot.reset();
    EXPECT_EQ(0U, ids->reserved_count());

    if (dist::distributed_query_fixtures_available()) {
        std::unique_ptr<api::executable_statement> statement{};
        EXPECT_NE(status::ok, db_->create_executable("SELECT C0 FROM remote_schema.T", statement));
    }
    // a new CREATE fetches again
    ASSERT_EQ(status::ok, create("remote", diagnostic)) << diagnostic;
    EXPECT_EQ(2, remote_.calls);
}

TEST_F(remote_catalog_rpc_test, failed_fetch_publishes_nothing) {
    auto ids = db_impl()->remote_function_ids();
    ASSERT_TRUE(ids);
    auto check_failed = [&](char const* name) {
        std::string diagnostic{};
        EXPECT_NE(status::ok, create("remote", diagnostic)) << name;
        EXPECT_FALSE(diagnostic.empty()) << name;
        EXPECT_FALSE(manager()->find_catalog("remote")) << name;
        EXPECT_TRUE(manager()->entries()->empty()) << name;
        EXPECT_TRUE(manager()->snapshot()->catalogs().empty()) << name;
        EXPECT_EQ(0U, ids->reserved_count()) << name;
    };
    remote_.mode = stream_mode::fail_after_first;
    check_failed("stream failure after a fragment");
    remote_.mode = stream_mode::sequence_gap;
    check_failed("sequence gap");
    remote_.mode = stream_mode::normal;
    remote_.handler = std::make_shared<unsupported_handler>();
    check_failed("UNIMPLEMENTED");
    EXPECT_EQ(3, remote_.calls);

    // nothing is retried automatically; the next CREATE fetches again
    remote_.handler = std::make_shared<encoding_handler>(remote_catalog());
    std::string diagnostic{};
    ASSERT_EQ(status::ok, create("remote", diagnostic)) << diagnostic;
    EXPECT_EQ(4, remote_.calls);
}

TEST_F(remote_catalog_rpc_test, unreachable_server_publishes_nothing) {
    server_->Shutdown();
    server_.reset();
    std::string diagnostic{};
    EXPECT_NE(status::ok, create("remote", diagnostic));
    EXPECT_FALSE(diagnostic.empty());
    EXPECT_TRUE(manager()->entries()->empty());
}

TEST_F(remote_catalog_rpc_test, local_catalog_handler_serves_local_definitions) {
    execute_statement("CREATE TABLE LT (C0 BIGINT PRIMARY KEY, C1 VARCHAR(10))");
    dist::local_catalog_handler handler{*db_};
    auto result = handler();
    ASSERT_TRUE(result.success) << result.error;
    wire::RemoteCatalog message{};
    ASSERT_TRUE(message.ParseFromString(result.catalog));
    ASSERT_EQ(1, message.schema_list_size());
    EXPECT_EQ("public", message.schema_list(0).schema_name());
    auto local = db_impl()->tables()->find_table("LT");
    ASSERT_TRUE(local);
    bool found = false;
    for (auto const& table : message.table_list()) {
        if (table.table_name() != "LT") continue;
        found = true;
        EXPECT_EQ(local->definition_id().value_or(0), table.remote_definition_id());
        ASSERT_EQ(2, table.column_index_list_size());
        auto const& c1 = message.column_list(static_cast<int>(table.column_index_list(1)));
        EXPECT_EQ("C1", c1.column_name());
        EXPECT_TRUE(c1.data_type().character_type().varying());
        EXPECT_EQ(10U, c1.data_type().character_type().length());
    }
    EXPECT_TRUE(found);
    // built-in regular and aggregate functions are part of the local catalog
    EXPECT_GT(message.function_list_size(), 0);
    EXPECT_GT(message.aggregate_function_list_size(), 0);
    dist::remote_catalog_information decoded{};
    std::string diagnostic{};
    EXPECT_EQ(status::ok, dist::remote_catalog_decoder{}.decode(message, decoded, diagnostic)) << diagnostic;
}

TEST_F(remote_catalog_rpc_test, local_catalog_round_trips_through_rpc_into_manager) {
    execute_statement("CREATE TABLE LT (C0 BIGINT PRIMARY KEY, C1 VARCHAR(10))");
    remote_.handler = std::make_shared<dist::local_catalog_handler>(*db_);
    std::string diagnostic{};
    ASSERT_EQ(status::ok, create("loop", diagnostic)) << diagnostic;
    auto schema = manager()->find_catalog("loop")->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("LT");
    ASSERT_TRUE(table);
    EXPECT_EQ(db_impl()->tables()->find_table("LT")->definition_id().value_or(0),
        manager()->snapshot()->remote_definition(*table)->remote_definition_id);
}

TEST_F(remote_catalog_rpc_test, remote_only_functions_never_compile_for_local_execution) {
    if (!dist::distributed_query_fixtures_available()) GTEST_SKIP();
    std::string diagnostic{};
    ASSERT_EQ(status::ok, create("remote", diagnostic)) << diagnostic;
    auto snapshot = manager()->snapshot();
    ASSERT_EQ(1, snapshot->catalogs().size());
    auto schema = snapshot->catalogs().front()->schema_provider().find("remote_schema");
    ASSERT_TRUE(schema);
    auto function = first<yugawara::function::provider, yugawara::function::declaration>(schema->function_provider());
    auto aggregate = first<yugawara::aggregate::provider, yugawara::aggregate::declaration>(schema->set_function_provider());
    ASSERT_TRUE(function);
    ASSERT_TRUE(aggregate);
    execute_statement("CREATE TABLE L(C0 BIGINT PRIMARY KEY)");
    for (auto const* sql : {"SELECT remote_f(1)", "SELECT remote_schema.remote_f(1)",
            "SELECT remote_f(C0) FROM L", "SELECT remote_agg(C0) FROM L"}) {
        api::statement_handle statement{};
        std::shared_ptr<error::error_info> info{};
        EXPECT_NE(status::ok, db_impl()->prepare(sql, statement, info)) << sql;
        ASSERT_TRUE(info) << sql;
        EXPECT_EQ(error_code::symbol_analyze_exception, info->code()) << sql;
        EXPECT_EQ(1, remote_.calls) << sql;
        EXPECT_EQ(0, remote_.execution_calls) << sql;
    }
    // Ordinary builtin resolution still succeeds while the remote catalog is present.
    api::statement_handle local{};
    EXPECT_EQ(status::ok, db_->prepare("SELECT ABS(-1)", local));
    EXPECT_EQ(1, remote_.calls);
    EXPECT_EQ(snapshot, manager()->snapshot());
    EXPECT_EQ(function, (first<yugawara::function::provider, yugawara::function::declaration>(schema->function_provider())));
    EXPECT_EQ(aggregate, (first<yugawara::aggregate::provider, yugawara::aggregate::declaration>(schema->set_function_provider())));
    EXPECT_TRUE(snapshot->remote_definition(*function));
    EXPECT_TRUE(snapshot->remote_definition(*aggregate));
}

}  // namespace jogasaki::testing
