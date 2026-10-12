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
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <tateyama/api/configuration.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::api::resource {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class resource_bridge_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

static constexpr std::string_view default_configuration {  // NOLINT
    "[sql]\n"
        "thread_pool_size=50\n"
        "enable_index_join=false\n"
        "stealing_enabled=true\n"
        "default_partitions=5\n"
        "stealing_wait=1\n"
        "task_polling_wait=0\n"
        "lightweight_job_level=0\n"
        "enable_hybrid_scheduler=true\n"
        "commit_response=PROPAGATED\n"
    "[datastore]\n"
        "log_location=\n"
};

TEST_F(resource_bridge_test, resource_cfg) {
    std::stringstream ss{
        "[sql]\n"
        "thread_pool_size=99\n"
        "busy_worker=true\n"
        "enable_index_join=true\n"
        "[datastore]\n"
        "log_location=LOCATION\n"
    };
    tateyama::api::configuration::whole cfg{ss, default_configuration};

    auto c = api::resource::convert_config(cfg);
    EXPECT_EQ(99, c->thread_pool_size());
    EXPECT_TRUE(c->busy_worker());
    EXPECT_TRUE(c->enable_index_join());

    // convert_config handles config entries necessary for jogasaki db initialization
    EXPECT_EQ("", c->db_location());
}

TEST_F(resource_bridge_test, cfg_default_value) {
    // verify default configuration is applied if input config stream has no entry
    std::stringstream ss{
        "[sql]\n"
    };
    tateyama::api::configuration::whole cfg{ss, default_configuration};
    auto c = api::resource::convert_config(cfg);
    EXPECT_EQ(50, c->thread_pool_size());
}

TEST_F(resource_bridge_test, enum_cfg) {
    {
        std::stringstream ss{
            "[sql]\n"
        };
        tateyama::api::configuration::whole cfg{ss, default_configuration};
        auto c = api::resource::convert_config(cfg);
        ASSERT_TRUE(c);
        EXPECT_EQ(commit_response_kind::propagated, c->default_commit_response());
    }
    {
        std::stringstream ss{
            "[sql]\n"
            "commit_response=AVAILABLE\n"
        };
        tateyama::api::configuration::whole cfg{ss, default_configuration};
        auto c = api::resource::convert_config(cfg);
        ASSERT_TRUE(c);
        EXPECT_EQ(commit_response_kind::available, c->default_commit_response());
    }
    {
        std::stringstream ss{
            "[sql]\n"
            "commit_response=bad_value\n"
        };
        tateyama::api::configuration::whole cfg{ss, default_configuration};
        auto c = api::resource::convert_config(cfg);
        EXPECT_FALSE(c);
    }
}

TEST_F(resource_bridge_test, invalid_entry) {
    // verify detect invalid value and exception caught
    std::stringstream ss{
        "[sql]\n"
        "thread_pool_size=bad_string_value\n"
    };
    tateyama::api::configuration::whole cfg{ss, default_configuration};
    auto c = api::resource::convert_config(cfg);
    EXPECT_FALSE(c);
}

static constexpr std::string_view udf_default_configuration {
    "[udf]\n"
    "endpoint=dns:///localhost:50051\n"
    "secure=false\n"
    "[grpc_server]\n"
    "endpoint=dns:///localhost:52345\n"
    "secure=false\n"
};

TEST_F(resource_bridge_test, udf_secure_default_value) {
    std::stringstream ss{
        "[udf]\n"
        "endpoint=A\n"
    };
    tateyama::api::configuration::whole cfg{ss, udf_default_configuration};

    auto c = api::resource::convert_config(cfg);
    ASSERT_TRUE(c);
    EXPECT_EQ("A", c->endpoint());
    ASSERT_EQ(1, c->secure_values().size());
    EXPECT_FALSE(c->secure_values()[0]);
    EXPECT_FALSE(c->secure());
    EXPECT_EQ("dns:///localhost:52345", c->grpc_server_endpoint());
    EXPECT_FALSE(c->grpc_server_secure());
}

TEST_F(resource_bridge_test, udf_multi_secure_cfg) {
    std::stringstream ss{
        "[udf]\n"
        "endpoint=A | B | C\n"
        "secure=false | true | false\n"
        "[grpc_server]\n"
        "endpoint=X | Y | Z\n"
        "secure=true\n"
    };
    tateyama::api::configuration::whole cfg{ss, udf_default_configuration};

    auto c = api::resource::convert_config(cfg);
    ASSERT_TRUE(c);
    EXPECT_EQ("A | B | C", c->endpoint());
    ASSERT_EQ(3, c->secure_values().size());
    EXPECT_FALSE(c->secure_values()[0]);
    EXPECT_TRUE(c->secure_values()[1]);
    EXPECT_FALSE(c->secure_values()[2]);
    EXPECT_FALSE(c->secure()); // legacy scalar accessor returns the first UDF value
    EXPECT_EQ("X | Y | Z", c->grpc_server_endpoint());
    EXPECT_TRUE(c->grpc_server_secure()); // grpc_server.secure remains scalar
}

TEST_F(resource_bridge_test, invalid_udf_multi_secure_cfg) {
    std::stringstream ss{
        "[udf]\n"
        "secure=false|invalid|true\n"
    };
    tateyama::api::configuration::whole cfg{ss, udf_default_configuration};

    auto c = api::resource::convert_config(cfg);
    EXPECT_FALSE(c);
}

TEST_F(resource_bridge_test, fixture_metadata_does_not_create_local_tables) {
    if (!dist::distributed_query_fixtures_available()) { GTEST_SKIP(); }
    auto path = std::filesystem::temp_directory_path() / ("jogasaki-bridge-fixture-" + std::to_string(::getpid()) + ".json");
    std::ofstream{path} << R"({"version":1,"catalogs":[{"name":"remote_catalog","server":"worker","schemas":[{"name":"public","tables":[{"name":"remote_only","columns":[{"name":"id","type":"bigint","nullable":false}],"primary_key":["id"]}]}]}]})";
    db_teardown();
    auto cfg = std::make_shared<configuration>();
    cfg->distributed_query_coordinator_enabled(true);
    cfg->distributed_query_remote_servers({{"worker", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}});
    cfg->distributed_query_catalog({dist::remote_catalog_source::fixture, path.string()});
    db_setup(cfg);
    std::filesystem::remove(path);
    ASSERT_TRUE(db_impl()->remote_catalogs());
    EXPECT_EQ(1, db_impl()->remote_catalogs()->catalogs().size());
    EXPECT_FALSE(db_impl()->tables()->find_table("remote_only"));
    auto borrowed = db_impl()->remote_catalogs();
    execute_statement("CREATE TABLE local_only (id BIGINT PRIMARY KEY)");
    EXPECT_TRUE(db_impl()->tables()->find_table("local_only"));
    db_teardown();
    EXPECT_EQ(1, borrowed->catalogs().size());
    db_setup();
}


TEST_F(resource_bridge_test, remote_catalog_failure_allows_database_start_retry) {
    ASSERT_EQ(status::ok, db_impl()->stop());
    auto cfg = db_impl()->configuration();
    cfg->distributed_query_coordinator_enabled(true);
    cfg->distributed_query_remote_servers({});
    cfg->distributed_query_catalog({dist::remote_catalog_source::rpc, {}});
    EXPECT_EQ(status::err_invalid_argument, db_impl()->start());
    cfg->distributed_query_remote_servers({{"worker", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}});
    // nothing listens on the worker: catalog_source=rpc starts empty and does not call GetRemoteCatalog
    ASSERT_EQ(status::ok, db_impl()->start());
    auto manager = db_impl()->remote_catalogs_manager();
    ASSERT_TRUE(manager);
    ASSERT_TRUE(manager->entries());
    EXPECT_TRUE(manager->entries()->empty());
    EXPECT_TRUE(db_impl()->remote_server_channels().find_channel("worker"));
    auto channel = db_impl()->remote_server_channels().find_channel("worker");
    ASSERT_EQ(status::ok, db_impl()->stop());
    ASSERT_EQ(status::ok, db_impl()->start());
    EXPECT_NE(channel, db_impl()->remote_server_channels().find_channel("worker"));
}

TEST_F(resource_bridge_test, distributed_query_roles_are_independent) {
    for (bool coordinator : {false, true}) {
        for (bool remote : {false, true}) {
            std::stringstream ss{};
            ss << "[distributed_query_coordinator]\nenabled=" << (coordinator ? "true" : "false")
               << "\n[distributed_query_remote]\nenabled=" << (remote ? "true" : "false")
               << "\n[distributed_query_client]\nname=worker\nuri=dns:///localhost:12345\n";
            tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_remote]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
            auto converted = api::resource::convert_config(cfg);
            ASSERT_TRUE(converted);
            EXPECT_EQ(coordinator, converted->distributed_query_coordinator_enabled());
            EXPECT_EQ(coordinator ? 1U : 0U, converted->distributed_query_remote_servers().size());
        }
    }
}

TEST_F(resource_bridge_test, disabled_coordinator_ignores_invalid_client_settings) {
    for (auto const& role : {"", "[distributed_query_coordinator]\nenabled=false\n"}) {
        std::stringstream ss{std::string{role} + "[distributed_query_client]\nname=worker\nuri=invalid\nsecure=invalid\ncatalog_source=unknown\n"};
        tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_remote]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
        auto converted = api::resource::convert_config(cfg);
        ASSERT_TRUE(converted);
        EXPECT_FALSE(converted->distributed_query_coordinator_enabled());
        EXPECT_TRUE(converted->distributed_query_remote_servers().empty());
        EXPECT_EQ(dist::remote_catalog_source::disabled, converted->distributed_query_catalog().source);
    }
}

TEST_F(resource_bridge_test, enabled_coordinator_requires_remote_servers) {
    for (auto const& client : {"", "[distributed_query_client]\nname=\nuri=\n"}) {
        std::stringstream ss{std::string{"[distributed_query_coordinator]\nenabled=true\n"} + client};
        tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_remote]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
        EXPECT_FALSE(api::resource::convert_config(cfg));
    }
}

TEST_F(resource_bridge_test, invalid_coordinator_flag_is_rejected) {
    std::stringstream ss{"[distributed_query_coordinator]\nenabled=invalid\n"};
    tateyama::api::configuration::whole cfg{ss, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_remote]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
    EXPECT_FALSE(api::resource::convert_config(cfg));
}

TEST_F(resource_bridge_test, disabled_coordinator_does_not_initialize_channels_or_catalogs) {
    ASSERT_EQ(status::ok, db_impl()->stop());
    auto cfg = db_impl()->configuration();
    cfg->distributed_query_coordinator_enabled(false);
    cfg->distributed_query_remote_servers({{"worker", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}});
    cfg->distributed_query_catalog({dist::remote_catalog_source::rpc, {}});
    ASSERT_EQ(status::ok, db_impl()->start());
    EXPECT_FALSE(db_impl()->remote_server_channels().find_channel("worker"));
    EXPECT_FALSE(db_impl()->remote_catalogs());
}

}
