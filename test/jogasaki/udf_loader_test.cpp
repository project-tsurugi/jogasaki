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

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/function/udf_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/test_root.h>
#include <jogasaki/udf/udf_loader.h>

namespace jogasaki::testing {

class udf_loader_test : public test_root {
  protected:
    class test_loader : public ::plugin::udf::udf_loader {
      public:
        using ::plugin::udf::udf_loader::parse_ini;
    };

    void SetUp() override {
        auto const* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        ASSERT_NE(nullptr, test_info);
        ini_path_ = std::filesystem::temp_directory_path()
            / (std::string{"jogasaki_udf_loader_test_"} + test_info->name() + ".ini");
        jogasaki::global::config_pool(std::make_shared<jogasaki::configuration>());
    }

    void TearDown() override {
        jogasaki::global::config_pool(std::make_shared<jogasaki::configuration>());
        std::error_code ec{};
        std::filesystem::remove(ini_path_, ec);
    }

    void set_global_udf_defaults(std::string_view endpoint, bool secure) {
        auto config = std::make_shared<jogasaki::configuration>();
        config->endpoint(endpoint);
        config->secure(secure);
        jogasaki::global::config_pool(config);
    }

    void write_ini(std::string_view contents) {
        std::ofstream out{ini_path_};
        ASSERT_TRUE(out);
        out << contents;
        ASSERT_TRUE(out);
    }

    std::filesystem::path ini_path_{};
};

TEST_F(udf_loader_test, legacy_options_insecure) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=dns:///127.0.0.1:50051\n"
        "secure=false\n"
        "transport=stream\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=dns:///127.0.0.1:40012\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    EXPECT_TRUE(config->enabled());
    EXPECT_EQ("dns:///127.0.0.1:50051", config->endpoint());
    EXPECT_EQ("stream", config->transport());
    EXPECT_FALSE(config->secure());
    ASSERT_TRUE(config->grpc_server_endpoint());
    EXPECT_EQ("dns:///127.0.0.1:40012", *config->grpc_server_endpoint());
}

TEST_F(udf_loader_test, legacy_options_secure) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=dns:///127.0.0.1:50052\n"
        "secure=true\n"
        "transport=stream\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=dns:///127.0.0.1:40013\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ("dns:///127.0.0.1:50052", config->endpoint());
    EXPECT_TRUE(config->secure());
    ASSERT_TRUE(config->grpc_server_endpoint());
    EXPECT_EQ("dns:///127.0.0.1:40013", *config->grpc_server_endpoint());
}

TEST_F(udf_loader_test, legacy_options_without_grpc_server_endpoint) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=dns:///127.0.0.1:50053\n"
        "secure=false\n"
        "transport=stream\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ("dns:///127.0.0.1:50053", config->endpoint());
    EXPECT_FALSE(config->secure());
    EXPECT_FALSE(config->grpc_server_endpoint());
}


TEST_F(udf_loader_test, legacy_options_override_global_defaults) {
    set_global_udf_defaults("dns:///127.0.0.1:51001", false);

    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=dns:///127.0.0.1:52001\n"
        "secure=true\n"
        "transport=stream\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ("dns:///127.0.0.1:52001", config->endpoint());
    EXPECT_TRUE(config->secure());

    EXPECT_EQ("dns:///127.0.0.1:51001", jogasaki::global::config_pool()->endpoint());
    EXPECT_FALSE(jogasaki::global::config_pool()->secure());
}

TEST_F(udf_loader_test, missing_legacy_options_keep_global_defaults) {
    set_global_udf_defaults("dns:///127.0.0.1:51002", true);

    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "transport=stream\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ("dns:///127.0.0.1:51002", config->endpoint());
    EXPECT_TRUE(config->secure());

    EXPECT_EQ("dns:///127.0.0.1:51002", jogasaki::global::config_pool()->endpoint());
    EXPECT_TRUE(jogasaki::global::config_pool()->secure());
}


TEST_F(udf_loader_test, multi_endpoint_options_are_normalized) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B|C\n"
        "secure=false|true|false\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X|Y|Z\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(3, config->servers().size());
    EXPECT_EQ("A", config->servers()[0].endpoint);
    EXPECT_FALSE(config->servers()[0].secure);
    EXPECT_EQ("X", config->servers()[0].tsurugi_endpoint);
    EXPECT_EQ("B", config->servers()[1].endpoint);
    EXPECT_TRUE(config->servers()[1].secure);
    EXPECT_EQ("Y", config->servers()[1].tsurugi_endpoint);
    EXPECT_EQ("C", config->servers()[2].endpoint);
    EXPECT_FALSE(config->servers()[2].secure);
    EXPECT_EQ("Z", config->servers()[2].tsurugi_endpoint);

    EXPECT_EQ("A", config->endpoint());
    EXPECT_FALSE(config->secure());
}

TEST_F(udf_loader_test, grpc_server_endpoints_are_used_for_blob_metadata) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B\n"
        "secure=false\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X|Y\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(2, config->servers().size());

    auto metadata0 =
        ::jogasaki::executor::function::details::make_blob_grpc_metadata(
            100, &*config, 0);
    EXPECT_EQ("X", metadata0.endpoint());

    auto metadata1 =
        ::jogasaki::executor::function::details::make_blob_grpc_metadata(
            101, &*config, 1);
    EXPECT_EQ("Y", metadata1.endpoint());
}

TEST_F(udf_loader_test, single_values_are_broadcast_to_all_endpoints) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B\n"
        "secure=true\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(2, config->servers().size());
    EXPECT_TRUE(config->servers()[0].secure);
    EXPECT_TRUE(config->servers()[1].secure);
    EXPECT_EQ("X", config->servers()[0].tsurugi_endpoint);
    EXPECT_EQ("X", config->servers()[1].tsurugi_endpoint);
}

TEST_F(udf_loader_test, grpc_server_endpoint_is_broadcast_to_all_udf_endpoints) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B\n"
        "secure=false\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(2, config->servers().size());
    EXPECT_EQ("X", config->servers()[0].tsurugi_endpoint);
    EXPECT_EQ("X", config->servers()[1].tsurugi_endpoint);
    ASSERT_TRUE(config->grpc_server_endpoint());
    EXPECT_EQ("X", *config->grpc_server_endpoint());
}

TEST_F(udf_loader_test, global_grpc_server_endpoint_is_used_when_plugin_setting_is_missing) {
    auto global_config = std::make_shared<jogasaki::configuration>();
    global_config->endpoint("GLOBAL_UDF");
    global_config->secure(false);
    global_config->grpc_server_endpoint("GLOBAL_TSURUGI");
    jogasaki::global::config_pool(global_config);

    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B\n"
        "secure=false\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());
    ASSERT_EQ(2, config->servers().size());
    EXPECT_EQ("GLOBAL_TSURUGI", config->servers()[0].tsurugi_endpoint);
    EXPECT_EQ("GLOBAL_TSURUGI", config->servers()[1].tsurugi_endpoint);
    EXPECT_FALSE(config->grpc_server_endpoint());
}

TEST_F(udf_loader_test, secure_count_mismatch_is_rejected) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B|C\n"
        "secure=false|true\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    EXPECT_FALSE(config);
    ASSERT_EQ(1, results.size());
    EXPECT_EQ(::plugin::udf::load_status::ini_invalid, results.front().status());
}

TEST_F(udf_loader_test, grpc_server_endpoint_count_mismatch_is_rejected) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B|C\n"
        "secure=false\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X|Y\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    EXPECT_FALSE(config);
    ASSERT_EQ(1, results.size());
    EXPECT_EQ(::plugin::udf::load_status::ini_invalid, results.front().status());
}

TEST_F(udf_loader_test, empty_endpoint_element_is_rejected) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A||C\n"
        "secure=false\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    EXPECT_FALSE(config);
    ASSERT_EQ(1, results.size());
    EXPECT_EQ(::plugin::udf::load_status::ini_invalid, results.front().status());
}
TEST_F(udf_loader_test, old_udf_tsurugi_endpoint_does_not_override_grpc_server_endpoint) {
    write_ini(
        "[udf]\n"
        "enabled=true\n"
        "endpoint=A|B\n"
        "secure=false\n"
        "tsurugi_endpoint=OLD_X|OLD_Y\n"
        "\n"
        "[grpc_server]\n"
        "endpoint=X|Y\n");

    test_loader loader{};
    std::vector<::plugin::udf::load_result> results{};
    auto config = loader.parse_ini(ini_path_, results);

    ASSERT_TRUE(config);
    EXPECT_TRUE(results.empty());

    ASSERT_EQ(2, config->servers().size());
    EXPECT_EQ("A", config->servers()[0].endpoint);
    EXPECT_EQ("X", config->servers()[0].tsurugi_endpoint);

    EXPECT_EQ("B", config->servers()[1].endpoint);
    EXPECT_EQ("Y", config->servers()[1].tsurugi_endpoint);
}

} // namespace jogasaki::testing
