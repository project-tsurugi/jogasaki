/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/kvs/database.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/kvs/id.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/environment.h>
#include "kvs_test_base.h"

namespace jogasaki::kvs {

using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

class kvs_database_test :
    public ::testing::Test,
    public kvs_test_base {

public:
    void SetUp() override {
        db_setup();
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(kvs_database_test, compare_and_print) {
    // cc layer doesn't always support having multiple DBs as it has shared resource such as epoch thread
    if(kvs::implementation_id() != "memory") {
        GTEST_SKIP();
    }
    std::map<std::string, std::string> options{};
    auto db1 = database::open(options);
    std::cout << *db1 << std::endl;
    ASSERT_TRUE(*db1 == *db1);
    ASSERT_TRUE(db1->close());
    auto db2 = database::open(options);
    ASSERT_TRUE(*db1 != *db2);
    ASSERT_TRUE(db2->close());
}

TEST_F(kvs_database_test, create_storage) {
    auto t1 = db_->create_storage("T");
    ASSERT_TRUE(t1);
    auto dup = db_->create_storage("T");
    ASSERT_FALSE(dup); // already exists
    auto t2 = db_->get_storage("T");
    ASSERT_TRUE(t2);
}

TEST_F(kvs_database_test, get_storage) {
    auto ng = db_->get_storage("T");
    ASSERT_FALSE(ng); // no storage exists
    auto t1 = db_->create_storage("T");
    auto t2 = db_->get_storage("T");
    ASSERT_TRUE(t1);
    ASSERT_TRUE(t2);
}

TEST_F(kvs_database_test, get_or_create_storage) {
    auto t1 = db_->get_or_create_storage("T");
    ASSERT_TRUE(t1);
    auto t2 = db_->get_or_create_storage("T");
    ASSERT_TRUE(t2);
}

TEST_F(kvs_database_test, create_transaction) {
    ASSERT_TRUE(db_);
    auto tx = db_->create_transaction();
    ASSERT_TRUE(tx);
    ASSERT_EQ(status::ok, tx->abort());
}

TEST_F(kvs_database_test, create_storage_with_options) {
    sharksfin::StorageOptions opts{100, "option_payload"};
    auto t1 = db_->create_storage("T", opts);
    ASSERT_TRUE(t1);
    auto dup = db_->create_storage("T");
    ASSERT_FALSE(dup); // already exists
    auto t2 = db_->get_storage("T");
    ASSERT_TRUE(t2);
    sharksfin::StorageOptions opt{};
    EXPECT_EQ(status::ok, t2->get_options(opt));
    EXPECT_EQ(100, opt.storage_id());
    EXPECT_EQ("option_payload", opt.payload());
    opt.storage_id(200);
    opt.payload("updated");
    EXPECT_EQ(status::ok, t2->set_options(opt));
    sharksfin::StorageOptions updated{};
    EXPECT_EQ(status::ok, t2->get_options(updated));
    EXPECT_EQ(200, opt.storage_id());
    EXPECT_EQ("updated", opt.payload());
}

}

