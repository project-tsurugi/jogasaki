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
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <tateyama/api/configuration.h>
#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/server.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/service/bridge.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/relay/blob_session_container.h>
#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>

#include "../test_root.h"
#include "../test_utils/create_configuration.h"
#include "../test_utils/temporary_folder.h"

namespace jogasaki::relay {

using namespace tateyama;

class blob_session_test : public test_root {
public:
    void SetUp() override {
        temporary_.prepare();

        auto conf = test_utils::create_configuration(
            path() + "/log_location",
            path() + "/session_store"
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

protected:
    [[nodiscard]] std::string path() const {
        return temporary_.path();
    }

    jogasaki::test::temporary_folder temporary_{};
    std::unique_ptr<framework::server> server_{};
};

TEST_F(blob_session_test, basic_usage) {
    executor::process::impl::work_context work_ctx{};

    // access blob_session_container and create session
    auto& container = work_ctx.blob_session_container();
    auto* session = container.get_or_create();

    // verify container now has a session
    EXPECT_TRUE(container);
    EXPECT_TRUE(container.has_session());

    // verify session is valid
    ASSERT_TRUE(session != nullptr);

    // call blob_session API - session_id
    auto session_id = session->session_id();
    EXPECT_TRUE(session_id > 0);

    // call blob_session API - add blob file
    std::filesystem::path blob_file = path() + "/test_blob.dat";
    {
        std::ofstream ofs(blob_file);
        ofs << "test data";
    }
    auto blob_id = session->add(blob_file);
    EXPECT_TRUE(blob_id > 0);

    // call blob_session API - find blob file
    auto found = session->find(blob_id);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(blob_file, found.value());

    // call blob_session API - entries
    auto entries = session->entries();
    EXPECT_EQ(1, entries.size());
    EXPECT_EQ(blob_id, entries[0]);

    // call blob_session API - compute_tag
    auto tag = session->compute_tag(blob_id);
    EXPECT_TRUE(tag > 0);

    // dispose the container
    container.dispose();

    // verify container no longer has a session
    EXPECT_TRUE(! container.has_session());
}

}
