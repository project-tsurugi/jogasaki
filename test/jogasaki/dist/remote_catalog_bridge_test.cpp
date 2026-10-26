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
#include <chrono>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <grpcpp/grpcpp.h>
#include <takatori/type/primitive.h>
#include <gtest/gtest.h>

#include <tateyama/api/configuration.h>
#include <tateyama/framework/server.h>
#include <tateyama/grpc/distributed_query/service_adapter.h>
#include <tateyama/proto/distributed_query/remote_catalog.grpc.pb.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_execution_block_proto.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_record_stream.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/dist/remote_catalog_decoder.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/utils/runner.h>

namespace jogasaki::testing {
namespace {
class remote_catalog_bridge_test : public ::testing::Test {
protected:
    void SetUp() override {
        temporary_.prepare();
        boost::asio::io_context io{};
        boost::asio::ip::tcp::acceptor reserve{io, {boost::asio::ip::tcp::v4(), 0}};
        endpoint_ = "127.0.0.1:" + std::to_string(reserve.local_endpoint().port());
        reserve.close();
        std::stringstream settings{};
        settings << "[sql]\nthread_pool_size=2\n[distributed_query_remote]\nenabled=true\n"
                 << "[distributed_query_coordinator]\nenabled=false\n[grpc_server]\nenabled=true\nsecure=false\nlisten_address=" << endpoint_ << '\n'
                 << "[system]\npid_directory=" << temporary_.path() << "\ninstance_id=catalog-test\n"
                 << "[session]\nenable_timeout=false\nrefresh_timeout=300\nmax_refresh_timeout=86400\nzone_offset=\nauthentication_timeout=0\n"
                 << "[cc]\nepoch_duration=40000\nwaiting_resolver_threads=2\nmax_concurrent_transactions=\n"
                 << "[authentication]\nenabled=false\nurl=http://localhost:8080/harinoki\nrequest_timeout=0\nadministrators=*\n"
                 << "[glog]\ndummy=\n"
                 << "[datastore]\nlogging_max_parallelism=2\nrecover_max_parallelism=2\nlog_location=" << temporary_.path() << "/log\n"
                 << "[blob_relay]\nenabled=false\n[stream_endpoint]\nenabled=false\n"
                 << "[ipc_endpoint]\ndatabase_name=dq-catalog-" << temporary_.path().substr(temporary_.path().find_last_of('/') + 1)
                 << "\nthreads=1\ndatachannel_buffer_size=1024\nmax_datachannel_buffers=1\nadmin_sessions=1\nallow_blob_privileged=false\n";
        std::stringstream overrides{};
        auto configuration = std::make_shared<tateyama::api::configuration::whole>(overrides, settings.str());
        server_ = std::make_unique<tateyama::framework::server>(tateyama::framework::boot_mode::database_server, configuration);
        tateyama::framework::add_core_components(*server_);
        bridge_ = std::make_shared<api::resource::bridge>();
        server_->add_resource(bridge_);
        ASSERT_TRUE(server_->start());
        ASSERT_TRUE(bridge_->database());
        ASSERT_EQ("", utils::runner{}.db(*bridge_->database()).text("CREATE TABLE T(C0 BIGINT PRIMARY KEY)").run().report());
        // Explicit worker definition identity; ordinary SQL DDL does not assign it yet.
        auto& database = api::impl::get_impl(*bridge_->database());
        auto original = database.tables()->find_index("T");
        ASSERT_TRUE(original);
        auto index = std::make_shared<yugawara::storage::index>(*original);
        index->definition_id(1257);
        database.tables()->add_index(std::move(index), true);
    }
    void TearDown() override {
        if (server_ && !stopped_) EXPECT_TRUE(server_->shutdown());
        bridge_.reset();
        server_.reset();
        temporary_.clean();
    }
    bool stopped_{};
    test::temporary_folder temporary_{};
    std::string endpoint_{};
    std::unique_ptr<tateyama::framework::server> server_{};
    std::shared_ptr<api::resource::bridge> bridge_{};
};

TEST_F(remote_catalog_bridge_test, framework_exposes_sql_catalog_on_registered_rpc) {
    namespace wire = tateyama::grpc::proto;
    using namespace std::chrono_literals;
    auto stub = wire::DistributedQueryService::NewStub(grpc::CreateChannel(endpoint_, grpc::InsecureChannelCredentials()));
    wire::RemoteCatalogRequest request{};
    request.mutable_metadata()->mutable_protocol_version()->set_major(1);
    request.mutable_metadata()->set_request_id("bridge-test");
    grpc::ClientContext context{};
    context.set_deadline(std::chrono::system_clock::now() + 10s);
    auto reader = stub->GetRemoteCatalog(&context, request);
    dist::remote_catalog_fragment_assembler catalog{};
    std::string diagnostic{};
    wire::RemoteCatalogResponse response{};
    while (reader->Read(&response)) ASSERT_EQ(status::ok, catalog.accept(response, diagnostic)) << diagnostic;
    ASSERT_TRUE(reader->Finish().ok());
    dist::remote_catalog_information decoded{};
    ASSERT_EQ(status::ok, catalog.finish(decoded, diagnostic)) << diagnostic;
    auto table = std::find_if(decoded.tables.begin(), decoded.tables.end(), [](auto const& entry) { return entry.name == "T"; });
    ASSERT_NE(decoded.tables.end(), table);
    ASSERT_EQ(1U, table->columns.size());
    ASSERT_LT(table->columns.front(), decoded.columns.size());
    EXPECT_EQ("C0", decoded.columns[table->columns.front()].name);
    EXPECT_FALSE(decoded.functions.empty());
    auto adapter = server_->find_resource<tateyama::grpc::distributed_query::service_adapter>();
    ASSERT_TRUE(adapter);
    ASSERT_TRUE(server_->shutdown());
    stopped_ = true;
    // SQL handler removal precedes database stop and the gRPC listener's shutdown.
}
TEST_F(remote_catalog_bridge_test, framework_executes_native_external_output_without_fixtures) {
    namespace wire = tateyama::grpc::proto;
    ASSERT_EQ("", utils::runner{}.db(*bridge_->database()).text("INSERT INTO T VALUES(42)").run().report());
    auto stub = wire::DistributedQueryService::NewStub(grpc::CreateChannel(endpoint_, grpc::InsecureChannelCredentials()));
    wire::RemoteProcessRequest request{};
    auto* metadata = request.mutable_metadata();
    metadata->mutable_protocol_version()->set_major(1);
    metadata->set_coordinator_job_id("native-external-test");
    metadata->set_remote_execution_id("worker");
    metadata->set_attempt_number(1);
    dist::remote_execution_block block{};
    block.values = {0}; block.stream_variables = {0}; block.external_relations = {73};
    block.result_schema = {{19, "int8()", false}};
    dist::remote_operator scan{};
    scan.kind = dist::remote_operator_kind::scan; scan.operator_id = 0; scan.output_value_id = 0;
    scan.source = dist::remote_source{};
    scan.source->table_name = "T"; scan.source->index_name = "T"; scan.source->definition_id = 1257; scan.source->columns = {{"C0", 0, 0, dist::encode_remote_type(takatori::type::int8{})}};
    dist::remote_operator offer{};
    offer.kind = dist::remote_operator_kind::offer; offer.operator_id = 1; offer.input_value_id = 0;
    offer.offer = dist::remote_offer{73, {{0, 19}}};
    block.operators = {scan, offer};
    ASSERT_TRUE(request.mutable_execution_block()->ParseFromString(dist::serialize_remote_execution_block_proto(block)));
    grpc::ClientContext context{};
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    auto reader = stub->ExecuteRemoteProcess(&context, request);
    auto meta = std::make_shared<meta::record_meta>(std::vector<meta::field_type>{
        meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}}, boost::dynamic_bitset<std::uint64_t>(1));
    dist::remote_record_stream_reader stream{*meta};
    wire::RemoteProcessResult response{};
    std::vector<std::int64_t> values{};
    while (reader->Read(&response)) {
        EXPECT_EQ(status::ok, stream.accept(response.SerializeAsString(), [&](auto bytes) {
            data::small_record_store record{meta};
            std::string error{};
            if (!dist::decode_remote_record(bytes, *meta, record.ref(), error)) return status::err_invalid_argument;
            values.push_back(record.ref().get_value<std::int64_t>(meta->value_offset(0)));
            return status::ok;
        }));
    }
    auto rpc_status = reader->Finish();
    EXPECT_TRUE(rpc_status.ok()) << rpc_status.error_message();
    EXPECT_EQ((std::vector<std::int64_t>{42}), values);
}
} // namespace
} // namespace jogasaki::testing
