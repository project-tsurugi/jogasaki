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

#include <map>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <sharksfin/StorageOptions.h>

namespace jogasaki::api {

class database_start_failure_test : public ::testing::Test {
protected:
    void SetUp() override {
        temporary_.prepare();
    }

    void TearDown() override {
        temporary_.clean();
    }

    jogasaki::test::temporary_folder temporary_{};
};

TEST_F(database_start_failure_test, borrowed_kvs_survives_failed_start_and_retry) {
    auto database_path = temporary_.path() + "/database";
    auto kvs_database = kvs::database::open({{"location", database_path}});
    ASSERT_TRUE(kvs_database);
    ::sharksfin::StorageOptions options{};
    options.payload("invalid storage metadata");
    auto broken_storage = kvs_database->create_storage("BROKEN", options);
    ASSERT_TRUE(broken_storage);

    auto config = std::make_shared<configuration>();
    config->db_location(database_path);
    auto database = create_database(config, kvs_database->handle());

    ASSERT_NE(status::ok, database->start());
    ASSERT_TRUE(impl::get_impl(*database).kvs_db());
    EXPECT_EQ(kvs_database->handle(), impl::get_impl(*database).kvs_db()->handle());

    ASSERT_EQ(status::ok, broken_storage->delete_storage());
    broken_storage.reset();
    ASSERT_EQ(status::ok, database->start());
    EXPECT_EQ(kvs_database->handle(), impl::get_impl(*database).kvs_db()->handle());
    EXPECT_EQ(status::ok, database->stop());
}

TEST_F(database_start_failure_test, owned_kvs_is_reopened_after_failed_start) {
    auto database_path = temporary_.path() + "/database";
    auto kvs_database = kvs::database::open({{"location", database_path}});
    ASSERT_TRUE(kvs_database);
    ::sharksfin::StorageOptions options{};
    options.payload("invalid storage metadata");
    auto broken_storage = kvs_database->create_storage("BROKEN", options);
    ASSERT_TRUE(broken_storage);
    broken_storage.reset();
    ASSERT_TRUE(kvs_database->close());
    kvs_database.reset();

    auto config = std::make_shared<configuration>();
    config->db_location(database_path);
    auto database = create_database(config);
    ASSERT_NE(status::ok, database->start());
    ASSERT_FALSE(impl::get_impl(*database).kvs_db());

    kvs_database = kvs::database::open({{"location", database_path}});
    ASSERT_TRUE(kvs_database);
    broken_storage = kvs_database->get_storage("BROKEN");
    ASSERT_TRUE(broken_storage);
    ASSERT_EQ(status::ok, broken_storage->delete_storage());
    broken_storage.reset();
    ASSERT_TRUE(kvs_database->close());
    kvs_database.reset();

    ASSERT_EQ(status::ok, database->start());
    EXPECT_EQ(status::ok, database->stop());
}

} // namespace jogasaki::api
