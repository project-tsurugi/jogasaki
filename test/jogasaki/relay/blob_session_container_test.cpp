/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include <tateyama/api/configuration.h>
#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/server.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/relay/blob_session_container.h>

#include "../test_root.h"
#include "../test_utils/create_configuration.h"
#include "../test_utils/temporary_folder.h"

namespace jogasaki::relay {

using namespace tateyama;

class blob_session_container_test : public test_root {
public:
    void SetUp() override {
        temporary_.prepare();

        grpc_port_ = 52345 + (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000);
        auto conf = test_utils::create_configuration(
            path() + "/log_location",
            path() + "/session_store",
            grpc_port_
        );
        framework::boot_mode mode = framework::boot_mode::database_server;
        server_ = std::make_unique<framework::server>(mode, conf);
        framework::add_core_components(*server_);
        auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
        server_->add_resource(sqlres);
        auto sqlsvc = std::make_shared<jogasaki::api::service::bridge>();
        server_->add_service(sqlsvc);
        server_->setup();
        auto* db = sqlsvc->database();
        (void)db;
        server_->start();
    }

    void TearDown() override {
        if (server_) {
            server_->shutdown();
            server_.reset();
        }
        temporary_.clean();
    }

private:
    [[nodiscard]] std::string path() const {
        return temporary_.path();
    }

    std::size_t grpc_port_{};
    jogasaki::test::temporary_folder temporary_{};
    std::unique_ptr<framework::server> server_{};
};

TEST_F(blob_session_container_test, initialize_empty_container_without_transaction) {
    auto relay_svc = global::relay_service();
    ASSERT_TRUE(relay_svc);

    blob_session_container container{std::nullopt};
    EXPECT_TRUE(! container);

    auto* session = container.get_or_create();

    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());
    EXPECT_TRUE(session != nullptr);
    EXPECT_EQ(session, container.get());
}

TEST_F(blob_session_container_test, initialize_empty_container_with_transaction) {
    auto relay_svc = global::relay_service();
    ASSERT_TRUE(relay_svc);

    std::uint64_t txid = 12345;
    blob_session_container container{txid};
    EXPECT_TRUE(! container);

    auto* session = container.get_or_create();

    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());
    EXPECT_TRUE(session != nullptr);
    EXPECT_EQ(session, container.get());
}

TEST_F(blob_session_container_test, no_op_when_container_already_has_session) {
    auto relay_svc = global::relay_service();
    ASSERT_TRUE(relay_svc);

    blob_session_container container{std::nullopt};
    auto* first_session = container.get_or_create();

    ASSERT_TRUE(container);
    EXPECT_TRUE(first_session != nullptr);

    // second call should return the same session
    auto* second_session = container.get_or_create();

    EXPECT_TRUE(container);
    EXPECT_EQ(first_session, second_session);
}

TEST_F(blob_session_container_test, reinitialize_after_dispose) {
    auto relay_svc = global::relay_service();
    ASSERT_TRUE(relay_svc);

    blob_session_container container{std::nullopt};
    auto* first_session = container.get_or_create();

    ASSERT_TRUE(container);
    EXPECT_TRUE(first_session != nullptr);

    container.dispose();
    EXPECT_TRUE(! container);

    auto* second_session = container.get_or_create();

    EXPECT_TRUE(container);
    EXPECT_TRUE(second_session != nullptr);
#ifndef NDEBUG
    // on release builds, this can be equal due to optimization
    EXPECT_TRUE(second_session != first_session);
#endif
}

}
