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

} // namespace jogasaki::testing
