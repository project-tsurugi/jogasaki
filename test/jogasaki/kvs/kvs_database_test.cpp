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

#include <takatori/util/object_creator.h>
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

class kvs_database_test : public test_root {
public:
    static void SetUpTestCase() {}
    kvs_database_test() {}
private:
};

TEST_F(kvs_database_test, open_close) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    ASSERT_TRUE(db);
    ASSERT_TRUE(db->close());
}

TEST_F(kvs_database_test, compare_and_print) {
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
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto t1 = db->create_storage("T");
    ASSERT_TRUE(t1);
    auto dup = db->create_storage("T");
    ASSERT_FALSE(dup); // already exists
    auto t2 = db->get_storage("T");
    ASSERT_TRUE(t2);
    ASSERT_TRUE(db->close());
}

TEST_F(kvs_database_test, create_transaction) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    ASSERT_TRUE(db);
    auto tx = db->create_transaction();
    ASSERT_TRUE(tx);
    ASSERT_EQ(status::ok, tx->abort());
    ASSERT_TRUE(db->close());
}

}

