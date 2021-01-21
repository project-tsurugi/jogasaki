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

class kvs_iterator_test : public test_root {
public:
    static void SetUpTestCase() {}
    kvs_iterator_test() {}
private:
};

TEST_F(kvs_iterator_test, compare_and_print) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto t1 = db->create_storage("T1");
    auto tx = db->create_transaction();
    std::unique_ptr<iterator> it1{};
    std::unique_ptr<iterator> it2{};
    ASSERT_TRUE(t1->scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it1));
    ASSERT_TRUE(t1->scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it2));
    std::cout << *it1 << std::endl;
    std::cout << *it2 << std::endl;
    ASSERT_TRUE(*it1 == *it1);
    ASSERT_TRUE(*it1 != *it2);
    (void)tx->abort();
    ASSERT_TRUE(db->close());
}

TEST_F(kvs_iterator_test, full_scan) {
    std::map<std::string, std::string> options{};
    auto db = database::open(options);
    auto t1 = db->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db->create_transaction();
        {
            ASSERT_TRUE(t1->put(*tx, "k1", "v1"));
            ASSERT_TRUE(t1->put(*tx, "k2", "v2"));
            ASSERT_TRUE(t1->put(*tx, "k3", "v3"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db->create_transaction();
        std::string_view k;
        std::string_view v;
        std::unique_ptr<iterator> it{};
        ASSERT_TRUE(t1->scan(*tx, "", end_point_kind::unbound, "", end_point_kind::unbound, it));
        ASSERT_TRUE(it);
        //ASSERT_FALSE(it->key(k)); // UB until first next() call
        ASSERT_TRUE(it->next());
        ASSERT_TRUE(it->key(k));
        ASSERT_TRUE(it->value(v));
        EXPECT_EQ("k1", k);
        EXPECT_EQ("v1", v);
        ASSERT_TRUE(it->next());
        ASSERT_TRUE(it->key(k));
        ASSERT_TRUE(it->value(v));
        EXPECT_EQ("k2", k);
        EXPECT_EQ("v2", v);
        ASSERT_TRUE(it->next());
        ASSERT_TRUE(it->key(k));
        ASSERT_TRUE(it->value(v));
        EXPECT_EQ("k3", k);
        EXPECT_EQ("v3", v);
        ASSERT_FALSE(it->next());
        ASSERT_EQ(status::ok, tx->commit());
    }
    ASSERT_TRUE(db->close());
}

}

