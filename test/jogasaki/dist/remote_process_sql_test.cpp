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
#include <cstdlib>
#include <functional>
#include <mutex>
#include <poll.h>
#include <spawn.h>
#include <thread>
#include <unistd.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>
#include <grpcpp/grpcpp.h>
#include <takatori/type/primitive.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <sys/wait.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/dist/local_catalog_handler.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_execution_block_proto.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_process_handler.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_record_stream.h>
#include <jogasaki/dist/remote_server_configuration.h>
#include <jogasaki/dist/remote_sql_error.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/termination_state.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/transaction_state_kind.h>
#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/runner.h>

extern char** environ;
namespace jogasaki::testing {
namespace {
namespace wire = tateyama::grpc::proto;
grpc::Status sql_failure(proto::sql::response::Error const& error) {
    google::protobuf::Any detail{};
    detail.set_type_url("type.googleapis.com/jogasaki.proto.sql.response.Error");
    detail.set_value(error.SerializeAsString());
    std::string bytes{};
    {
        google::protobuf::io::StringOutputStream stream{&bytes};
        google::protobuf::io::CodedOutputStream out{&stream};
        using wire = google::protobuf::internal::WireFormatLite;
        wire::WriteEnum(1, 2, &out);
        wire::WriteString(2, error.detail(), &out);
        wire::WriteBytes(3, detail.SerializeAsString(), &out);
    }
    return {grpc::StatusCode::UNKNOWN, error.detail(), bytes};
}
bool write_message(grpc::ServerWriter<wire::RemoteProcessResult>* writer, std::uint64_t sequence, std::string const& payload) {
    wire::RemoteProcessResult response{};
    response.set_sequence_number(sequence);
    proto::distributed_query::RemoteExecutionResult result{};
    if (!result.ParseFromString(payload)) return false;
    for (auto const& record : result.encoded_rows()) response.add_record_list(record);
    return writer->Write(response);
}
class rpc_service final : public wire::DistributedQueryService::Service {
public:
    std::function<tateyama::grpc::distributed_query::remote_catalog_result()> catalog_handler{};
    grpc::Status GetRemoteCatalog(grpc::ServerContext*, wire::RemoteCatalogRequest const*,
        grpc::ServerWriter<wire::RemoteCatalogResponse>* writer) override {
        if (!catalog_handler) return {grpc::StatusCode::UNAVAILABLE, "catalog handler is not ready"};
        auto result = catalog_handler();
        if (!result.success) return {grpc::StatusCode::INTERNAL, result.error};
        wire::RemoteCatalogResponse response{};
        response.set_sequence_number(0);
        if (!response.mutable_catalog_fragment()->ParseFromString(result.catalog)) {
            return {grpc::StatusCode::INTERNAL, "invalid catalog"};
        }
        // The current compiler adapter resolves schema.table. A distinct fixture
        // schema prevents these reads from resolving to the coordinator's local T.
        for (auto& schema : *response.mutable_catalog_fragment()->mutable_schema_list()) {
            schema.set_schema_name("worker_schema");
        }
        return writer->Write(response) ? grpc::Status::OK : grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
    }
    std::function<tateyama::grpc::distributed_query::remote_process_result(std::string_view)> handler{};
    std::function<tateyama::grpc::distributed_query::remote_process_result(std::string_view,
        tateyama::grpc::distributed_query::remote_process_handler::cancellation_check const&,
        tateyama::grpc::distributed_query::remote_process_handler::result_sink const&)> stream_handler{};
    // Writes responses directly to model protocol violations of a remote server.
    std::function<grpc::Status(std::string_view, grpc::ServerWriter<wire::RemoteProcessResult>*)> raw{};
    std::atomic_size_t chunks{};
    std::atomic_size_t calls{};
    std::chrono::milliseconds delay{};
    grpc::Status injected_status{};
    grpc::StatusCode failure{grpc::StatusCode::OK};
    bool typed_records{};
    std::uint64_t sequence_offset{};
    grpc::Status ExecuteRemoteProcess(grpc::ServerContext* context, wire::RemoteProcessRequest const* request,
        grpc::ServerWriter<wire::RemoteProcessResult>* writer) override {
        ++calls;
        if (!request->has_metadata() || request->metadata().protocol_version().major() != 1) return {grpc::StatusCode::INVALID_ARGUMENT, "invalid metadata"};
        auto deadline = std::chrono::steady_clock::now() + delay;
        while (std::chrono::steady_clock::now() < deadline) {
            if (context->IsCancelled()) return {grpc::StatusCode::CANCELLED, "cancelled"};
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!injected_status.ok()) return injected_status;
        if (failure != grpc::StatusCode::OK) return {failure, "test remote failure"};
        if (raw) return raw(request->execution_block().SerializeAsString(), writer);
        if (stream_handler && !typed_records) {
            auto sequence = sequence_offset;
            auto result = stream_handler(request->execution_block().SerializeAsString(), [context] { return context->IsCancelled(); },
                [&](std::string_view payload) {
                    wire::RemoteProcessResult response{};
                    response.set_sequence_number(sequence++);
                    std::string error{};
                    auto result = dist::deserialize_remote_execution_result(payload, error);
                    if (!result) return false;
                    for (auto const& record : result->encoded_rows) response.add_record_list(record);
                    ++chunks;
                    return writer->Write(response);
                });
            if (!result.success) return {grpc::StatusCode::INTERNAL, result.error};
            return grpc::Status::OK;
        }
        auto result = handler(request->execution_block().SerializeAsString());
        if (!result.success) return {grpc::StatusCode::INVALID_ARGUMENT, result.error};
        if (typed_records) {
            std::string error{};
            auto records = dist::deserialize_remote_execution_result(result.payload, error);
            if (!records) return {grpc::StatusCode::INTERNAL, error};
            // Exercise the formal record-list RPC with a small batch limit.
            dist::remote_record_stream_writer stream{[&](auto const& message) {
                wire::RemoteProcessResult response{};
                response.set_sequence_number(message.sequence_number());
                for (auto const& record : message.record_list()) response.add_record_list(record);
                return writer->Write(response) ? status::ok : status::request_canceled;
            }, 6, [&] { return context->IsCancelled(); }};
            for (auto const& record : records->encoded_rows) if (auto st = stream.append(record); st != status::ok)
                return {grpc::StatusCode::RESOURCE_EXHAUSTED, "record stream failed"};
            return stream.finish() == status::ok ? grpc::Status::OK : grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
        }
        wire::RemoteProcessResult response{};
        response.set_sequence_number(sequence_offset);
        std::string error{};
        auto records = dist::deserialize_remote_execution_result(result.payload, error);
        if (!records) return {grpc::StatusCode::INTERNAL, error};
        for (auto const& record : records->encoded_rows) response.add_record_list(record);
        return writer->Write(response) ? grpc::Status::OK : grpc::Status{grpc::StatusCode::CANCELLED, "stream closed"};
    }
};
}
class remote_process_sql_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }
    void SetUp() override {
#ifndef ENABLE_DISTRIBUTED_QUERY_FIXTURES
        GTEST_SKIP() << "development remote selection is disabled";
#else
        grpc::ServerBuilder builder{};
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port_);
        bool external = std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()}.find("two_process_") == 0;
        if (external) launch_worker();
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        ASSERT_TRUE(server_);
        auto config = std::make_shared<configuration>();
        bool single_worker = std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()}.find("single_worker_") == 0;
        config->thread_pool_size(single_worker ? 1 : 4);
        config->distributed_query_coordinator_enabled(std::getenv("JOGASAKI_DQ_TEST_WORKER_READY_FD") == nullptr);
        config->distributed_query_remote_servers({dist::remote_server_configuration{"worker", "127.0.0.1:" + std::to_string(external ? worker_port_ : port_), false, dist::root_certificate_source::none, {}}});
        db_setup(config);
        service_.catalog_handler = [this] { return dist::local_catalog_handler{*db_}(); };
        service_.handler = [this](std::string_view payload) { return dist::remote_process_handler{*db_}(payload); };
        execute_statement("CREATE TABLE T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(20), C2 DECIMAL(10,2))");
        install_identity("T", 1257);
        execute_statement("INSERT INTO T VALUES (10, 'worker-value', 12.34), (20, NULL, 56.78)");
        if (std::getenv("JOGASAKI_DQ_TEST_WORKER_READY_FD")) {
            execute_statement("DELETE FROM T");
            execute_statement("INSERT INTO T VALUES (900, 'remote-process', 12.34), (1000, NULL, 56.78)");
        } else {
            refresh_catalog();
        }
#endif
    }
    void install_identity(std::string_view name, std::uint64_t identity) {
        auto original = db_impl()->tables()->find_index(name);
        ASSERT_TRUE(original);
        auto replacement = std::make_shared<yugawara::storage::index>(*original);
        replacement->definition_id(identity);
        db_impl()->tables()->add_index(std::move(replacement), true);
    }
    void refresh_catalog() {
        auto manager = db_impl()->remote_catalogs_manager();
        if (manager->find_catalog("remote")) ASSERT_EQ(status::ok, manager->drop("remote"));
        std::string diagnostic{};
        ASSERT_EQ(status::ok, manager->create({"remote", "worker"}, db_impl()->remote_server_channels(), diagnostic))
            << diagnostic;
    }
    void TearDown() override {
        unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_SERVER");
        unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS");
        if (server_) server_->Shutdown();
        if (db_) db_teardown();
        if (child_ > 0) {
            char stop = 'x';
            auto stop_written = write(stop_fd_, &stop, 1);
            EXPECT_EQ(1, stop_written);
            close(stop_fd_);
            int status{};
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (waitpid(child_, &status, WNOHANG) == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (waitpid(child_, &status, WNOHANG) == 0) { kill(child_, SIGKILL); (void) waitpid(child_, &status, 0); }
            EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        }
    }
    void query(std::string_view sql, std::vector<mock::basic_record>& rows) {
        auto tx = utils::create_transaction(*db_, true, false);
        execute_query(sql, *tx, rows);
        ASSERT_EQ(status::ok, tx->commit());
    }
    void expect_remote_termination(bool expect_success, request_info info = {}) {
        std::unique_ptr<api::executable_statement> statement{};
        ASSERT_EQ(status::ok, db_->create_executable("SELECT C0 FROM worker_schema.T ORDER BY C0", statement));
        auto tx = utils::create_transaction(*db_, true, false);
        auto transaction = api::get_transaction_context(*tx);
        auto store = std::make_unique<data::result_store>();
        auto channel = std::make_shared<api::impl::result_store_channel>(
            takatori::util::maybe_shared_ptr{store.get()});
        auto context = api::impl::create_request_context(api::impl::get_impl(*db_), transaction,
            channel, std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
            std::move(info), true, nullptr);
        auto* raw = context.get();
        std::size_t completions{};
        ASSERT_TRUE(executor::execute_async_on_context(api::impl::get_impl(*db_), context,
            takatori::util::maybe_shared_ptr{statement.get()},
            [&](status result, std::shared_ptr<error::error_info> /*error*/,
                std::shared_ptr<request_statistics> /*statistics*/) {
                ++completions;
                EXPECT_EQ(expect_success, result == status::ok);
                EXPECT_EQ(0, raw->job()->task_count().load());
                if (auto flows = raw->flows()) {
                    for (std::size_t i = 0; i < flows->size(); ++i) EXPECT_FALSE(flows->exists(i));
                }
            }, true));
        EXPECT_EQ(1, completions);
        EXPECT_EQ(0, transaction->termination_mgr().state().task_use_count());
        if (expect_success) EXPECT_EQ(status::ok, tx->commit());
        else if (transaction->termination_mgr().state().going_to_abort()) {
            EXPECT_EQ(transaction_state_kind::aborted, transaction->state());
        } else EXPECT_EQ(status::ok, tx->abort());
    }
    void launch_worker() {
        int ready[2]{}, stop[2]{};
        ASSERT_EQ(0, pipe(ready));
        ASSERT_EQ(0, pipe(stop));
        std::vector<std::string> variables{};
        for (char** p = environ; *p; ++p) variables.emplace_back(*p);
        variables.emplace_back("JOGASAKI_DQ_TEST_WORKER_READY_FD=" + std::to_string(ready[1]));
        variables.emplace_back("JOGASAKI_DQ_TEST_WORKER_STOP_FD=" + std::to_string(stop[0]));
        std::vector<char*> environment{};
        for (auto& value : variables) environment.push_back(value.data());
        environment.push_back(nullptr);
        char executable[] = "/proc/self/exe";
        char filter[] = "--gtest_filter=remote_process_sql_test.worker_process_helper";
        char* args[] = {executable, filter, nullptr};
        posix_spawn_file_actions_t actions{};
        ASSERT_EQ(0, posix_spawn_file_actions_init(&actions));
        posix_spawn_file_actions_addclose(&actions, ready[0]);
        posix_spawn_file_actions_addclose(&actions, stop[1]);
        auto result = posix_spawn(&child_, executable, &actions, nullptr, args, environment.data());
        posix_spawn_file_actions_destroy(&actions);
        close(ready[1]);
        close(stop[0]);
        ASSERT_EQ(0, result);
        stop_fd_ = stop[1];
        pollfd fd{ready[0], POLLIN, 0};
        ASSERT_GT(poll(&fd, 1, 20000), 0);
        char port[32]{};
        ASSERT_GT(read(ready[0], port, sizeof(port)-1), 0);
        close(ready[0]);
        worker_port_ = std::stoi(port);
        ASSERT_GT(worker_port_, 0);
    }
protected:
    pid_t child_{};
    int stop_fd_{};
    int worker_port_{};
    rpc_service service_{};
    std::unique_ptr<grpc::Server> server_{};
    int port_{};
};
TEST_F(remote_process_sql_test, streaming_worker_results_reach_existing_downstream_exchange) {
    service_.stream_handler = [this](std::string_view payload, auto const& check, auto const& sink) {
        return dist::remote_process_handler{*db_}.execute_stream(payload, check, sink);
    };
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    ASSERT_EQ(2, rows.size());
    EXPECT_GT(service_.chunks.load(), 0);
    EXPECT_EQ(1, service_.calls.load());
}

TEST_F(remote_process_sql_test, multiple_result_messages_preserve_rows_and_contiguous_sequence) {
    service_.stream_handler = [this](std::string_view payload, auto const&, auto const& sink) {
        auto result = dist::remote_process_handler{*db_}(payload);
        if (!result.success) return result;
        std::string error{};
        auto decoded = dist::deserialize_remote_execution_result(result.payload, error);
        if (!decoded) return tateyama::grpc::distributed_query::remote_process_result{false, {}, error};
        for (auto const& row : decoded->encoded_rows) {
            dist::remote_execution_result chunk{};
            chunk.columns = decoded->columns;
            chunk.encoded_rows = {row};
            auto bytes = dist::serialize_remote_execution_result(chunk);
            if (!sink(bytes)) return tateyama::grpc::distributed_query::remote_process_result{false, {}, "closed"};
        }
        result.payload.clear();
        return result;
    };
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(10, rows[0].get_value<std::int64_t>(0));
    EXPECT_EQ(20, rows[1].get_value<std::int64_t>(0));
    EXPECT_EQ(2, service_.chunks.load());
}

TEST_F(remote_process_sql_test, typed_record_batches_cross_dummy_rpc_without_fragmenting_rows) {
    service_.typed_records = true;
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port_), grpc::InsecureChannelCredentials());
    auto stub = wire::DistributedQueryService::NewStub(channel);
    wire::RemoteProcessRequest request{};
    request.mutable_metadata()->mutable_protocol_version()->set_major(1);
    request.mutable_metadata()->set_coordinator_job_id("test"); request.mutable_metadata()->set_remote_execution_id("stream"); request.mutable_metadata()->set_attempt_number(1);
    auto block = dist::remote_execution_block{};
    block.values = {0}; block.stream_variables = {0}; block.external_relations = {0}; block.result_schema = {{0, "int8()", false}};
    dist::remote_operator scan{}; scan.kind = dist::remote_operator_kind::scan; scan.operator_id = 0; scan.output_value_id = 0;
    scan.source = dist::remote_source{}; scan.source->table_name = "T"; scan.source->index_name = "T"; scan.source->definition_id = 1257; scan.source->columns = {{"C0", 0, 0, dist::encode_remote_type(takatori::type::int8{})}};
    dist::remote_operator offer{}; offer.kind = dist::remote_operator_kind::offer; offer.operator_id = 1; offer.input_value_id = 0; offer.offer = dist::remote_offer{0, {{0, 0}}};
    block.operators = {scan, offer}; ASSERT_TRUE(request.mutable_execution_block()->ParseFromString(dist::serialize_remote_execution_block_proto(block)));
    grpc::ClientContext context{}; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    auto reader = stub->ExecuteRemoteProcess(&context, request); wire::RemoteProcessResult response{};
    auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}}, boost::dynamic_bitset<std::uint64_t>(1));
    dist::remote_record_stream_reader stream{*meta, 6};
    std::vector<std::int64_t> values{}; std::size_t batches{};
    while (reader->Read(&response)) {
        EXPECT_EQ(batches++, response.sequence_number());
        ASSERT_EQ(status::ok, stream.accept(response.SerializeAsString(), [&](auto bytes) {
            data::small_record_store record{meta}; std::string error{};
            if (!dist::decode_remote_record(bytes, *meta, record.ref(), error)) return status::err_invalid_argument;
            values.push_back(record.ref().get_value<std::int64_t>(meta->value_offset(0))); return status::ok;
        }));
    }
    EXPECT_TRUE(reader->Finish().ok()); EXPECT_EQ((std::vector<std::int64_t>{10, 20}), values); EXPECT_GT(batches, 1);
}
TEST_F(remote_process_sql_test, response_sequence_gap_fails_sql) {
    service_.sequence_offset = 1;
    auto tx = utils::create_transaction(*db_, true, false); status st{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(st).run();
    EXPECT_EQ(status::err_invalid_argument, st);
}
TEST_F(remote_process_sql_test, response_sequence_duplicate_fails_sql) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = dist::remote_process_handler{*db_}(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        if (write_message(writer, 0, result.payload)) (void) write_message(writer, 0, result.payload);
        return grpc::Status::OK;
    };
    auto tx = utils::create_transaction(*db_, true, false); status st{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(st).run();
    EXPECT_EQ(status::err_invalid_argument, st);
}
TEST_F(remote_process_sql_test, ok_without_result_message_returns_no_rows) {
    service_.raw = [](std::string_view, auto*) { return grpc::Status::OK; };
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    EXPECT_TRUE(rows.empty());
    EXPECT_EQ(1, service_.calls.load());
}
TEST_F(remote_process_sql_test, invalid_record_between_messages_fails_sql) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = dist::remote_process_handler{*db_}(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        std::string error{};
        auto decoded = dist::deserialize_remote_execution_result(result.payload, error);
        if (!decoded || decoded->columns.empty()) return grpc::Status{grpc::StatusCode::INTERNAL, "invalid fixture"};
        auto changed = *decoded;
        changed.encoded_rows = {std::string{1, char{0}}};
        changed.rows.clear();
        if (write_message(writer, 0, result.payload)) {
            (void) write_message(writer, 1, dist::serialize_remote_execution_result(changed));
        }
        return grpc::Status::OK;
    };
    auto tx = utils::create_transaction(*db_, true, false); status st{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(st).run();
    EXPECT_EQ(status::err_invalid_argument, st);
}
TEST_F(remote_process_sql_test, error_after_partial_results_fails_whole_sql) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = dist::remote_process_handler{*db_}(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        (void) write_message(writer, 0, result.payload);
        return grpc::Status{grpc::StatusCode::INTERNAL, "worker failed after partial result"};
    };
    auto tx = utils::create_transaction(*db_, true, false);
    status st{};
    std::shared_ptr<error::error_info> info{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(st).error(info).run();
    EXPECT_EQ(status::err_unknown, st);
    ASSERT_TRUE(info);
    EXPECT_EQ("worker failed after partial result", info->message());
}
TEST_F(remote_process_sql_test, single_worker_blocked_remote_stream_does_not_block_other_sql) {
    unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_SERVER");
    execute_statement("CREATE TABLE L(C0 BIGINT PRIMARY KEY, C1 VARCHAR(*))");
    install_identity("L", 1258);
    std::string value(30000, 'x');
    // About 1.4 MiB of rows, so the worker calls the sink before its scan completes.
    for (int i = 0; i < 48; ++i) execute_statement("INSERT INTO L VALUES (" + std::to_string(i) + ", '" + value + "')");
    refresh_catalog();
    std::mutex mutex{};
    std::condition_variable cv{};
    bool entered{};
    bool released{};
    std::atomic_size_t streams{};
    service_.stream_handler = [&](std::string_view payload, auto const& check, auto const& sink) {
        bool block = streams++ == 0;
        return dist::remote_process_handler{*db_}.execute_stream(payload, check, [&, block](std::string_view bytes) {
            if (block) {
                std::unique_lock lock{mutex};
                entered = true;
                cv.notify_all();
                cv.wait(lock, [&] { return released; });
            }
            return sink(bytes);
        });
    };
    std::atomic_bool blocked_done{};
    std::atomic_bool other_done{};
    std::size_t blocked_rows{};
    std::size_t other_rows{};
    std::thread blocked{};
    std::thread other{};
    struct cleanup {
        std::function<void()> body;
        ~cleanup() { body(); }
    } guard{[&] {
        // Always unblock the remote sink so that both SQL requests can finish even when an assertion fails.
        {
            std::lock_guard lock{mutex};
            released = true;
        }
        cv.notify_all();
        if (blocked.joinable()) blocked.join();
        if (other.joinable()) other.join();
        service_.stream_handler = {};
    }};
    blocked = std::thread{[&] {
        std::vector<mock::basic_record> rows{};
        query("SELECT C0, C1 FROM worker_schema.L ORDER BY C0", rows);
        blocked_rows = rows.size();
        blocked_done = true;
    }};
    {
        std::unique_lock lock{mutex};
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds{20}, [&] { return entered; }));
    }
    other = std::thread{[&] {
        std::vector<mock::basic_record> rows{};
        query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
        other_rows = rows.size();
        other_done = true;
    }};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{20};
    while (!other_done && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    // SQL and RPC share the only worker; the blocked remote stream must not hold it.
    ASSERT_TRUE(other_done.load());
    EXPECT_FALSE(blocked_done.load());
    guard.body();
    guard.body = [] {};
    EXPECT_EQ(2, other_rows);
    EXPECT_TRUE(blocked_done.load());
    EXPECT_EQ(48, blocked_rows);
    EXPECT_EQ(2, streams.load());
}
TEST_F(remote_process_sql_test, select_uses_rpc_and_existing_downstream_exchange) {
    std::vector<mock::basic_record> rows{};
    query("SELECT C0, C1, C2 FROM worker_schema.T WHERE C0 > 0 ORDER BY C0", rows);
    ASSERT_EQ(2, rows.size());
    EXPECT_GT(service_.calls, 0);
    EXPECT_EQ(10, rows[0].ref().get_value<std::int64_t>(rows[0].record_meta()->value_offset(0)));
}
TEST_F(remote_process_sql_test, delayed_rpc_releases_scheduler_worker) {
    service_.delay = std::chrono::milliseconds(100);
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    ASSERT_EQ(2, rows.size());
    EXPECT_GT(service_.calls, 0);
}
}

namespace jogasaki::testing {
TEST_F(remote_process_sql_test, worker_process_helper) {
    auto* ready = std::getenv("JOGASAKI_DQ_TEST_WORKER_READY_FD");
    if (!ready) GTEST_SKIP() << "helper runs only in an explicitly spawned worker process";
    auto port = std::to_string(port_) + "\n";
    ASSERT_EQ(static_cast<ssize_t>(port.size()), write(std::stoi(ready), port.data(), port.size()));
    close(std::stoi(ready));
    auto stop = std::stoi(std::getenv("JOGASAKI_DQ_TEST_WORKER_STOP_FD"));
    char signal{};
    ASSERT_EQ(1, read(stop, &signal, 1));
    close(stop);
}
TEST_F(remote_process_sql_test, two_process_select_returns_worker_values) {
    std::vector<mock::basic_record> rows{};
    query("SELECT C0, C1, C2 FROM worker_schema.T WHERE C0 > 0 ORDER BY C0", rows);
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(900, rows[0].get_value<std::int64_t>(0));
    EXPECT_EQ(1000, rows[1].get_value<std::int64_t>(0));
}
TEST_F(remote_process_sql_test, two_process_find_without_order_uses_worker_values) {
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 AS A, C0 AS B FROM worker_schema.T WHERE C0 = 900", rows);
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(900, rows[0].get_value<std::int64_t>(0));
    EXPECT_EQ(900, rows[0].get_value<std::int64_t>(1));
}
TEST_F(remote_process_sql_test, no_implicit_five_second_deadline) {
    service_.delay = std::chrono::milliseconds(5200);
    unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS");
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    EXPECT_EQ(2, rows.size());
}
TEST_F(remote_process_sql_test, rpc_deadline_fails_sql_and_releases_resources) {
    service_.delay = std::chrono::milliseconds(500);
    setenv("JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS", "100", 1);
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).run();
    EXPECT_EQ(status::err_time_out, result);
}
TEST_F(remote_process_sql_test, sql_error_preserves_code_detail_and_supplemental_text) {
    proto::sql::response::Error remote{};
    remote.set_code(proto::sql::error::VALUE_EVALUATION_EXCEPTION);
    remote.set_detail("worker division by zero");
    remote.set_supplemental_text(R"({"source_file":"worker.cpp:42","additional_text":"remote diagnostic"})");
    service_.injected_status = sql_failure(remote);
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).run();
    EXPECT_EQ(status::err_expression_evaluation_failure, result);
    std::shared_ptr<api::error_info> info{};
    ASSERT_EQ(status::ok, tx->error_info(info));
    ASSERT_TRUE(info);
    EXPECT_EQ(error_code::value_evaluation_exception, info->code());
    EXPECT_EQ(remote.detail(), info->message());
    EXPECT_EQ(remote.supplemental_text(), info->supplemental_text());
}
TEST_F(remote_process_sql_test, resource_error_is_not_reported_as_expression_failure) {
    proto::sql::response::Error remote{};
    remote.set_code(proto::sql::error::SQL_LIMIT_REACHED_EXCEPTION);
    remote.set_detail("worker limit reached");
    service_.injected_status = sql_failure(remote);
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).run();
    EXPECT_EQ(status::err_resource_limit_reached, result);
    std::shared_ptr<api::error_info> info{};
    ASSERT_EQ(status::ok, tx->error_info(info));
    ASSERT_TRUE(info);
    EXPECT_TRUE(info->supplemental_text().empty());
}
TEST_F(remote_process_sql_test, mismatched_sql_status_details_are_protocol_error) {
    proto::sql::response::Error remote{};
    remote.set_code(proto::sql::error::VALUE_EVALUATION_EXCEPTION);
    remote.set_detail("worker error");
    auto valid = sql_failure(remote);
    for (auto const& invalid : std::vector<grpc::Status>{
        {grpc::StatusCode::UNKNOWN, "different outer message", valid.error_details()},
        {grpc::StatusCode::INVALID_ARGUMENT, remote.detail(), valid.error_details()}}) {
        service_.injected_status = invalid;
        auto tx = utils::create_transaction(*db_, true, false);
        status result{};
        builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).run();
        EXPECT_EQ(status::err_invalid_state, result);
        std::shared_ptr<api::error_info> info{};
        ASSERT_EQ(status::ok, tx->error_info(info));
        ASSERT_TRUE(info);
        EXPECT_EQ(error_code::internal_exception, info->code());
    }
}
TEST_F(remote_process_sql_test, malformed_sql_status_details_fall_back_to_transport_error) {
    service_.injected_status = {grpc::StatusCode::UNKNOWN, "original transport error", "invalid protobuf"};
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).run();
    EXPECT_EQ(status::err_unknown, result);
    std::shared_ptr<api::error_info> info{};
    ASSERT_EQ(status::ok, tx->error_info(info));
    ASSERT_TRUE(info);
    EXPECT_EQ("original transport error", info->message());
}
TEST_F(remote_process_sql_test, find_and_group_downstream_use_remote_rows) {
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T WHERE C0 = 10", rows);
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(10, rows[0].get_value<std::int64_t>(0));
    rows.clear();
    query("SELECT SUM(C0) FROM worker_schema.T", rows);
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(30, rows[0].get_value<std::int64_t>(0));
    EXPECT_GE(service_.calls.load(), 2);
}
TEST_F(remote_process_sql_test, unsolicited_remote_cancellation_is_transport_failure) {
    service_.failure = grpc::StatusCode::CANCELLED;
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    std::shared_ptr<error::error_info> info{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).error(info).run();
    EXPECT_EQ(status::err_io_error, result);
    ASSERT_TRUE(info);
    EXPECT_NE(error_code::request_canceled, info->code());
}

TEST_F(remote_process_sql_test, cancellation_is_forwarded_after_handler_entry) {
    service_.delay = std::chrono::seconds(1);
    auto response = std::make_shared<tateyama::api::server::mock::test_response>();
    auto tx = utils::create_transaction(*db_, true, false);
    std::thread cancel{[this, response] {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (service_.calls == 0 && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        response->cancel();
    }};
    status result{};
    std::shared_ptr<error::error_info> info{};
    builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(result).error(info).req_info(request_info{1, {}, response}).run();
    cancel.join();
    EXPECT_EQ(status::request_canceled, result);
    EXPECT_GT(service_.calls, 0);
    ASSERT_TRUE(info);
    EXPECT_EQ(error_code::request_canceled, info->code());
}
TEST_F(remote_process_sql_test, unsupported_small_integer_cast_reports_prepare_error_before_rpc) {
    for (auto const* type : {"TINYINT", "SMALLINT"}) {
        api::statement_handle statement{};
        std::shared_ptr<error::error_info> info{};
        auto result = api::impl::get_impl(*db_).prepare(
            std::string{"SELECT CAST(C0 AS "} + type + ") FROM worker_schema.T", statement, info);
        EXPECT_EQ(status::err_unsupported, result) << type;
        ASSERT_TRUE(info) << type;
        EXPECT_EQ(error_code::unsupported_compiler_feature_exception, info->code());
        EXPECT_NE(std::string::npos, info->message().find("unsupported remote execution type"));
        EXPECT_EQ(0, service_.calls.load());
    }
    auto tx = utils::create_transaction(*db_, true, false);
    status result{};
    builder().text("SELECT C0 FROM worker_schema.T WHERE C0 > 0").tx(*tx).st(result).run();
    EXPECT_EQ(status::ok, result);
    EXPECT_GT(service_.calls.load(), 0);
}

TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_on_success) {
    expect_remote_termination(true);
    EXPECT_EQ(1, service_.calls.load());
}
TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_on_transport_error) {
    service_.failure = grpc::StatusCode::UNAVAILABLE;
    expect_remote_termination(false);
    EXPECT_EQ(1, service_.calls.load());
}
TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_on_protocol_error) {
    service_.sequence_offset = 1;
    expect_remote_termination(false);
    EXPECT_EQ(1, service_.calls.load());
}

TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_on_request_cancellation) {
    service_.delay = std::chrono::seconds(1);
    auto response = std::make_shared<tateyama::api::server::mock::test_response>();
    std::thread cancel{[this, response] {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (service_.calls == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        response->cancel();
    }};
    expect_remote_termination(false, request_info{1, {}, response});
    cancel.join();
    EXPECT_EQ(1, service_.calls.load());
}

TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_after_partial_remote_output) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = service_.handler(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        if (!write_message(writer, 0, result.payload)) {
            return grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
        }
        return grpc::Status{grpc::StatusCode::INTERNAL, "failure after partial remote output"};
    };
    expect_remote_termination(false);
    EXPECT_EQ(1, service_.calls.load());
}
TEST_F(remote_process_sql_test, completion_releases_tasks_and_flows_on_rpc_timeout) {
    service_.delay = std::chrono::seconds(1);
    setenv("JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS", "100", 1);
    expect_remote_termination(false);
    // A deadline includes connection setup, so handler entry is not required.
    EXPECT_LE(service_.calls.load(), 1);
}


TEST_F(remote_process_sql_test, transport_status_mapping_preserves_failure_category) {
    for (auto [transport, expected] : {
            std::pair{grpc::StatusCode::UNAVAILABLE, status::err_io_error},
            std::pair{grpc::StatusCode::RESOURCE_EXHAUSTED, status::err_resource_limit_reached},
            std::pair{grpc::StatusCode::INVALID_ARGUMENT, status::err_invalid_argument},
            std::pair{grpc::StatusCode::DATA_LOSS, status::err_invalid_argument},
            std::pair{grpc::StatusCode::UNIMPLEMENTED, status::err_unsupported},
            std::pair{grpc::StatusCode::NOT_FOUND, status::err_not_found},
            std::pair{grpc::StatusCode::INTERNAL, status::err_unknown}}) {
        SCOPED_TRACE(static_cast<int>(transport));
        service_.failure = transport;
        auto tx = utils::create_transaction(*db_, true, false);
        status actual{};
        builder().text("SELECT C0 FROM worker_schema.T ORDER BY C0").tx(*tx).expect_error(true).st(actual).run();
        EXPECT_EQ(expected, actual);
    }
}


TEST(remote_sql_error_test, rejects_duplicate_envelope_fields_and_duplicate_sql_detail) {
    proto::sql::response::Error error{};
    error.set_code(proto::sql::error::EVALUATION_EXCEPTION);
    error.set_detail("division failed");
    auto original = sql_failure(error);
    for (int duplicate = 0; duplicate != 3; ++duplicate) {
        SCOPED_TRACE(duplicate);
        auto bytes = original.error_details();
        {
            google::protobuf::io::StringOutputStream stream{&bytes};
            google::protobuf::io::CodedOutputStream out{&stream};
            using wire_format = google::protobuf::internal::WireFormatLite;
            if (duplicate == 0) wire_format::WriteEnum(1, 2, &out);
            if (duplicate == 1) wire_format::WriteString(2, error.detail(), &out);
            if (duplicate == 2) {
                google::protobuf::Any detail{};
                detail.set_type_url("type.googleapis.com/jogasaki.proto.sql.response.Error");
                detail.set_value(error.SerializeAsString());
                wire_format::WriteBytes(3, detail.SerializeAsString(), &out);
            }
        }
        bool protocol_error{};
        EXPECT_FALSE(dist::deserialize_remote_sql_error(bytes, 2, error.detail(), protocol_error));
    }
}
TEST(remote_sql_error_test, ignores_unrelated_detail_and_restores_sql_status) {
    proto::sql::response::Error error{};
    error.set_code(proto::sql::error::EVALUATION_EXCEPTION);
    error.set_detail("division failed");
    auto original = sql_failure(error);
    std::string bytes{};
    {
        google::protobuf::io::StringOutputStream stream{&bytes};
        google::protobuf::io::CodedOutputStream out{&stream};
        google::protobuf::Any unrelated{};
        unrelated.set_type_url("type.googleapis.com/example.Trace");
        unrelated.set_value("trace");
        google::protobuf::internal::WireFormatLite::WriteBytes(3, unrelated.SerializeAsString(), &out);
    }
    bytes += original.error_details();
    bool protocol_error{};
    auto decoded = dist::deserialize_remote_sql_error(bytes, 2, error.detail(), protocol_error);
    ASSERT_TRUE(decoded);
    EXPECT_FALSE(protocol_error);
    auto restored = dist::restore_remote_sql_error(*decoded);
    EXPECT_EQ(error_code::evaluation_exception, restored->code());
    EXPECT_EQ(status::err_expression_evaluation_failure, restored->status());
    EXPECT_EQ(error.detail(), restored->message());
}


TEST_F(remote_process_sql_test, many_result_messages_deliver_every_row) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = service_.handler(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        for (std::uint64_t sequence = 0; sequence != 200; ++sequence) {
            if (!write_message(writer, sequence, result.payload)) return grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
        }
        return grpc::Status::OK;
    };
    std::vector<mock::basic_record> rows{};
    query("SELECT C0 FROM worker_schema.T ORDER BY C0", rows);
    ASSERT_EQ(400U, rows.size());
    EXPECT_EQ(10, rows.front().get_value<std::int64_t>(0));
    EXPECT_EQ(20, rows.back().get_value<std::int64_t>(0));
}
TEST_F(remote_process_sql_test, invalid_record_during_many_messages_completes_cleanup) {
    service_.raw = [this](std::string_view payload, auto* writer) {
        auto result = service_.handler(payload);
        if (!result.success) return grpc::Status{grpc::StatusCode::INTERNAL, result.error};
        for (std::uint64_t sequence = 0; sequence != 200; ++sequence) {
            if (sequence == 30) {
                wire::RemoteProcessResult invalid{};
                invalid.set_sequence_number(sequence);
                invalid.add_record_list(std::string(1, '\0'));
                if (!writer->Write(invalid)) return grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
            } else if (!write_message(writer, sequence, result.payload)) {
                return grpc::Status{grpc::StatusCode::CANCELLED, "closed"};
            }
        }
        return grpc::Status::OK;
    };
    expect_remote_termination(false);
}

}
