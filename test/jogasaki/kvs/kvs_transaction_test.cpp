/*
 * Copyright 2018-2020 tsurugi project.
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

#include <jogasaki/test_root.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/environment.h>

namespace jogasaki::kvs {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class kvs_transaction_test : public test_root {
public:
};

TEST_F(kvs_transaction_test, compare_and_print) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto tx1 = db->create_transaction();
    std::cout << *tx1 << std::endl;
    ASSERT_EQ(status::ok, tx1->commit());
    auto tx2 = db->create_transaction();
    std::cout << *tx2 << std::endl;
    ASSERT_EQ(status::ok, tx2->commit());

    ASSERT_TRUE(*tx1 == *tx1);
    ASSERT_TRUE(*tx1 != *tx2);
    ASSERT_TRUE(db->close());
}

TEST_F(kvs_transaction_test, commit) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto t1 = db->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db->create_transaction();
        {
            ASSERT_EQ(status::ok, t1->put(*tx, "k1", "v1"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db->create_transaction();
        std::string_view v;
        ASSERT_EQ(status::ok, t1->get(*tx, "k1", v));
        EXPECT_EQ("v1", v);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_TRUE(db->close());
}

TEST_F(kvs_transaction_test, abort) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto t10 = db->create_storage("T10");
    ASSERT_TRUE(t10);
    {
        auto tx = db->create_transaction();
        {
            ASSERT_EQ(status::ok, t10->put(*tx, "k1", "v1"));
        }
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        auto tx = db->create_transaction();
        std::string_view v;
//        ASSERT_FALSE(t10->get(*tx, "k1", v)); // abort/rollback depends on sharksfin implementation
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_TRUE(db->close());
}

}

