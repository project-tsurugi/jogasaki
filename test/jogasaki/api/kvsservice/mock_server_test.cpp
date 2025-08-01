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
#include <memory>
#include <string>
#include <thread>
#include <gtest/gtest.h>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/server.h>

#include <jogasaki/api/kvsservice/resource.h>
#include <jogasaki/api/kvsservice/service.h>

#include "test_utils.h"

namespace jogasaki::api::kvsservice {

class mock_server_test: public ::testing::Test {
public:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(mock_server_test, DISABLED_start_shutdown) {
    tateyama::framework::server sv {tateyama::framework::boot_mode::database_server,
                                    default_configuration()};
    tateyama::framework::add_core_components(sv);
    sv.add_resource(std::make_shared<jogasaki::api::kvsservice::resource>());
    sv.add_service(std::make_shared<jogasaki::api::kvsservice::service>());
    EXPECT_TRUE(sv.setup());
    EXPECT_TRUE(sv.start());
    std::this_thread::sleep_for(std::chrono::minutes(10)); // FIXME
    EXPECT_TRUE(sv.shutdown());
}

}
