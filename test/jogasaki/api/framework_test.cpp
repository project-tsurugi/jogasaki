/*
 * Copyright 2018-2020 tsurugi project.
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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <tateyama/framework/server.h>
#include <tateyama/framework/endpoint_broker.h>
#include <tateyama/api/endpoint/mock/request_response.h>
#include <tateyama/proto/framework/request.pb.h>
#include <tateyama/proto/framework/response.pb.h>
#include <tateyama/utils/protobuf_utils.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "../test_utils/temporary_folder.h"
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/utils/command_utils.h>

namespace jogasaki::api {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class framework_test :
    public ::testing::Test,
    public testing::api_test_base {
public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
    }

    void TearDown() override {
    }

};

using namespace std::string_view_literals;
using namespace tateyama;

TEST_F(framework_test, server_to_start_sql_engine) {
    auto conf = tateyama::api::configuration::create_configuration();
    framework::boot_mode mode = framework::boot_mode::database_server;
    framework::server sv{mode, conf};
    framework::add_core_components(sv);
    auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
    sv.add_resource(sqlres);
    auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
    sv.add_service(sqlsvc);
    sv.setup();
    auto* db = sqlsvc->database();
    db->config()->prepare_benchmark_tables(true);
    sv.start();
    sv.shutdown();
}

class test_endpoint : public framework::endpoint {
public:

    bool setup(framework::environment& env) override {
        broker_ = env.service_repository().find<framework::endpoint_broker>();
        return true;
    }

    bool start(framework::environment&) override {
        return true;
    }

    bool shutdown(framework::environment&) override {
        return true;
    }

    std::string send(std::string_view data) {
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(data);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        (*broker_)(req, res);
        return res->body_;
    }
    std::shared_ptr<framework::endpoint_broker> broker_{};
};

TEST_F(framework_test, send_request_with_header) {
    auto conf = tateyama::api::configuration::create_configuration();
    framework::boot_mode mode = framework::boot_mode::database_server;
    framework::server sv{mode, conf};
    framework::add_core_components(sv);
    auto ep = std::make_shared<test_endpoint>();
    sv.add_endpoint(ep);
    auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
    sv.add_resource(sqlres);
    auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
    sv.add_service(sqlsvc);
    sv.setup();
    auto* db = sqlsvc->database();
    db->config()->prepare_benchmark_tables(true);
    sv.start();
    std::uint64_t handle{};
    {
        auto s = utils::encode_begin(false, false, std::vector<std::string>{});

        std::string result{};
        {
            ::tateyama::proto::framework::request::Header hdr{};
            hdr.set_message_version(1);
            hdr.set_session_id(100);
            hdr.set_service_id(framework::service_id_sql);
            std::stringstream ss{};
            ASSERT_TRUE(tateyama::utils::SerializeDelimitedToOstream(hdr, &ss));
            ASSERT_TRUE(tateyama::utils::PutDelimitedBodyToOstream(s, &ss));
            auto str = ss.str();
            result = ep->send(str);
        }
        {
            ::tateyama::proto::framework::response::Header hdr{};
            google::protobuf::io::ArrayInputStream in(result.data(), result.size());
            ASSERT_TRUE(tateyama::utils::ParseDelimitedFromZeroCopyStream(&hdr, &in, nullptr));
            std::string_view out{};
            ASSERT_TRUE(tateyama::utils::GetDelimitedBodyFromZeroCopyStream(&in, nullptr, out));
            result = out;
        }
        handle = utils::decode_begin(result);
    }
    {
        std::string result{};
        {
            ::tateyama::proto::framework::request::Header hdr{};
            hdr.set_message_version(1);
            hdr.set_session_id(100);
            hdr.set_service_id(framework::service_id_sql);
            std::stringstream ss{};
            ASSERT_TRUE(tateyama::utils::SerializeDelimitedToOstream(hdr, &ss));
            auto s = utils::encode_commit(handle);
            ASSERT_TRUE(tateyama::utils::PutDelimitedBodyToOstream(s, &ss));
            auto str = ss.str();
            result = ep->send(str);
        }
        {
            ::tateyama::proto::framework::response::Header hdr{};
            google::protobuf::io::ArrayInputStream in(result.data(), result.size());
            ASSERT_TRUE(tateyama::utils::ParseDelimitedFromZeroCopyStream(&hdr, &in, nullptr));
            std::string_view out{};
            ASSERT_TRUE(tateyama::utils::GetDelimitedBodyFromZeroCopyStream(&in, nullptr, out));
            result = out;
        }
        auto [success, error] = utils::decode_result_only(result);
        ASSERT_TRUE(success);
    }
    sv.shutdown();
}
}