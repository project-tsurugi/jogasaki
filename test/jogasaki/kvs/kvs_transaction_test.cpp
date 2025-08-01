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
#include <memory>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/status.h>

#include "kvs_test_base.h"

namespace jogasaki::kvs {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class kvs_transaction_test :
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

TEST_F(kvs_transaction_test, compare_and_print) {
    auto tx1 = db_->create_transaction();
    std::cout << *tx1 << std::endl;
    ASSERT_EQ(status::ok, tx1->commit());
    auto tx2 = db_->create_transaction();
    std::cout << *tx2 << std::endl;
    ASSERT_EQ(status::ok, tx2->commit());

    ASSERT_TRUE(*tx1 == *tx1);
    ASSERT_TRUE(*tx1 != *tx2);
}

TEST_F(kvs_transaction_test, commit) {
    auto t1 = db_->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db_->create_transaction();
        {
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k1", "v1"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view v;
        ASSERT_EQ(status::ok, t1->content_get(*tx, "k1", v));
        EXPECT_EQ("v1", v);
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
}

TEST_F(kvs_transaction_test, abort) {
    auto t10 = db_->create_storage("T10");
    ASSERT_TRUE(t10);
    {
        auto tx = db_->create_transaction();
        {
            ASSERT_EQ(status::ok, t10->content_put(*tx, "k1", "v1"));
        }
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view v;
//        ASSERT_FALSE(t10->content_get(*tx, "k1", v)); // abort/rollback depends on sharksfin implementation
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
}

}

