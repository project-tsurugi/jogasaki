/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/component_ids.h>
#include <tateyama/framework/endpoint.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/repository.h>
#include <tateyama/framework/routing_service.h>
#include <tateyama/framework/server.h>
#include <tateyama/utils/cache_align.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/command_utils.h>

#include "../test_utils/temporary_folder.h"
#include "api_test_base.h"

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
        temporary_.prepare();
    }

    void TearDown() override {
        temporary_.clean();
    }
    std::shared_ptr<tateyama::api::configuration::whole> create_config();
};

using namespace std::string_view_literals;
using namespace tateyama;

static constexpr std::string_view default_configuration {  // NOLINT
    "[sql]\n"
        "thread_pool_size=\n"
        "enable_index_join=false\n"
        "stealing_enabled=true\n"
        "default_partitions=5\n"
        "stealing_wait=1\n"
        "task_polling_wait=0\n"
        "lightweight_job_level=0\n"
        "enable_hybrid_scheduler=true\n"

    "[ipc_endpoint]\n"
        "database_name=tsurugi\n"
        "threads=104\n"
        "datachannel_buffer_size=64\n"
        "max_datachannel_buffers=256\n"
        "admin_sessions=1\n"
        "allow_blob_privileged=true\n"

    "[stream_endpoint]\n"
        "enabled=false\n"
        "port=12345\n"
        "threads=104\n"
        "allow_blob_privileged=false\n"
        "dev_idle_work_interval=1000\n"

    "[cc]\n"
        "epoch_duration=40000\n"
        "waiting_resolver_threads=2\n"

    "[datastore]\n"
        "logging_max_parallelism=112\n"
        "log_location="
};

std::shared_ptr<tateyama::api::configuration::whole> framework_test::create_config() {
    std::stringstream ss{};
    ss << default_configuration;
    ss << path();
    ss << "\n";
    return tateyama::api::configuration::create_configuration("", ss.str());
}

TEST_F(framework_test, server_to_start_sql_engine) {
    auto conf = create_config();
    framework::boot_mode mode = framework::boot_mode::database_server;
    framework::server sv{mode, conf};
    framework::add_core_components(sv);
    auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
    sv.add_resource(sqlres);
    auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
    sv.add_service(sqlsvc);
    sv.setup();
    auto* db = sqlsvc->database();
    sv.start();
    sv.shutdown();
}

class test_endpoint : public framework::endpoint {
public:

    bool setup(framework::environment& env) override {
        router_ = env.service_repository().find<framework::routing_service>();
        return true;
    }

    bool start(framework::environment&) override {
        return true;
    }

    bool shutdown(framework::environment&) override {
        return true;
    }

    std::string send(std::string_view data, std::size_t session_id, std::size_t service_id) {
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(data, session_id, service_id);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        (*router_)(req, res);
        [&](){ ASSERT_TRUE(res->wait_completion()); }();
        return res->body_;
    }

    [[nodiscard]] std::string_view label() const noexcept override {
        return "test_endpoint";
    }
    std::shared_ptr<framework::routing_service> router_{};
};

TEST_F(framework_test, send_request_with_header) {
    auto conf = create_config();
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
    db->config()->skip_smv_check(true);
    sv.start();
    std::uint64_t handle{};
    {
        auto s = utils::encode_begin(false, false, std::vector<std::string>{});
        std::string result = ep->send(s, 100, framework::service_id_sql);
        auto [h, id] = utils::decode_begin(result);
        handle = h;
        (void) id;
    }
    {
        auto s = utils::encode_commit(handle, true);
        std::string result = ep->send(s, 100, framework::service_id_sql);
        auto [success, error] = utils::decode_result_only(result);
        ASSERT_TRUE(success);
    }
    sv.shutdown();
}

TEST_F(framework_test, quiescent_mode) {
    auto conf = create_config();
    framework::boot_mode mode = framework::boot_mode::quiescent_server;
    framework::server sv{mode, conf};
    framework::add_core_components(sv);
    auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
    sv.add_resource(sqlres);
    auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
    sv.add_service(sqlsvc);
    sv.setup();
    sv.start();
    ASSERT_FALSE(sqlsvc->operator()(nullptr, nullptr));
    sv.shutdown();
}
}
