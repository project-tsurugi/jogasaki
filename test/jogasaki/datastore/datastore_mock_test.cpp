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
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/configuration.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/create_file.h>

#include "../kvs/kvs_test_base.h"


namespace jogasaki::datastore {

using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

class datastore_mock_test :
    public ::testing::Test,
    public kvs::kvs_test_base {

public:
    void SetUp() override {
        global::config_pool()->mock_datastore(true);
        db_setup();
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(datastore_mock_test, get_datastore_mock) {
    global::config_pool()->mock_datastore(true);
    auto* ds = get_datastore(true);
    ASSERT_TRUE(ds);
    EXPECT_EQ(datastore_kind::mock, ds->kind());
}

TEST_F(datastore_mock_test, get_datastore_prod) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support prod datastore";
    }
    global::config_pool()->mock_datastore(false);
    auto* ds = get_datastore(true);
    ASSERT_TRUE(ds);
    EXPECT_EQ(datastore_kind::production, ds->kind());
}

TEST_F(datastore_mock_test, acquire_blob_pool) {
    auto* ds = get_datastore(true);
    ASSERT_TRUE(ds);
    auto pool = ds->acquire_blob_pool();
    ASSERT_TRUE(pool);
    pool->release();
}

TEST_F(datastore_mock_test, register_file) {
    auto* ds = get_datastore(true);
    ASSERT_TRUE(ds);
    auto pool = ds->acquire_blob_pool();
    ASSERT_TRUE(pool);

    std::string file = path() + "/register_file.dat";
    create_file(path() + "/register_file.dat", "123");
    auto id = pool->register_file(file, false);
    auto ds_file = ds->get_blob_file(id);

    ASSERT_TRUE(ds_file);
    EXPECT_TRUE(! ds_file.path().empty());
    EXPECT_EQ("123", read_file(ds_file.path().string()));

    pool->release();
}

}

