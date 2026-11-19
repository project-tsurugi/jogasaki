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
#include <sstream>
#include <stdexcept>
#include <optional>
#include <utility>
#include <takatori/util/reference_vector.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/table.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/configurable_provider.h>

#include <unistd.h>
#include <gtest/gtest.h>

#include <takatori/type/int.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/plan/compiler_context.h>

namespace jogasaki::testing {
using namespace dist;

class remote_server_channel_manager_test : public ::testing::Test {
protected:
    std::vector<remote_server_configuration> servers{{"worker", "dns:///localhost:12345", false, root_certificate_source::none, {}}};
};

TEST_F(remote_server_channel_manager_test, channel_reuse_and_stop_do_not_invalidate_borrowed_channel) {
    remote_server_channel_manager manager{};
    EXPECT_FALSE(manager.find_channel("worker"));
    ASSERT_EQ(status::ok, manager.start(servers));
    auto channel = manager.find_channel("worker");
    ASSERT_TRUE(channel);
    EXPECT_EQ(channel, manager.find_channel("worker"));
    EXPECT_FALSE(manager.find_channel("unknown"));
    EXPECT_EQ(status::err_invalid_state, manager.start(servers));
    manager.stop(); manager.stop();
    EXPECT_FALSE(manager.find_channel("worker"));
    EXPECT_TRUE(channel);
}

TEST_F(remote_server_channel_manager_test, invalid_channel_configuration_can_be_corrected_before_start) {
    remote_server_channel_manager manager{};
    auto duplicate = servers; duplicate.push_back(servers.front());
    EXPECT_EQ(status::err_invalid_argument, manager.start(duplicate));
    EXPECT_FALSE(manager.find_channel("worker"));
    ASSERT_EQ(status::ok, manager.start(servers));
}

TEST_F(remote_server_channel_manager_test, reserved_local_server_is_rejected_without_changing_channels) {
    dist::remote_server_channel_manager manager{};
    EXPECT_EQ(status::err_invalid_argument, manager.start({
        {"local", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}}));
    EXPECT_FALSE(manager.find_channel("local"));
    EXPECT_EQ(status::ok, manager.start({
        {"worker", "dns:///localhost:12345", false, dist::root_certificate_source::none, {}}}));
    EXPECT_TRUE(manager.find_channel("worker"));
}



} // namespace jogasaki::testing
