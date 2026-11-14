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
#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/status.h>

#include "kvs_test_base.h"

namespace jogasaki::kvs {

using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

class kvs_storage_test :
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

TEST_F(kvs_storage_test, delete_storage) {
    auto t = db_->create_storage("T");
    ASSERT_TRUE(t);
    ASSERT_EQ(status::ok, t->delete_storage());
    wait_epochs();
    auto t2 = db_->get_storage("T");
    ASSERT_FALSE(t2);
}

TEST_F(kvs_storage_test, compare_and_print) {
    auto t1 = db_->create_storage("T1");
    auto t2 = db_->create_storage("T2");
    std::cout << *t1 << std::endl;
    std::cout << *t2 << std::endl;
    ASSERT_TRUE(*t1 == *t1);
    ASSERT_TRUE(*t1 != *t2);
}

TEST_F(kvs_storage_test, put_get_remove) {
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
        ASSERT_EQ(status::ok, t1->content_delete(*tx, "k1"));
        ASSERT_EQ(status::ok, tx->commit());
    }
    wait_epochs();
    {
        auto tx = db_->create_transaction();
        std::string_view v;
        ASSERT_EQ(status::not_found, t1->content_get(*tx, "k1", v));
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db_->create_transaction();
        ASSERT_EQ(status::ok, t1->content_put(*tx, "k1", "v2"));
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        std::string_view v;
        auto tx = db_->create_transaction();
        ASSERT_EQ(status::ok, t1->content_get(*tx, "k1", v));
        EXPECT_EQ("v2", v);
        ASSERT_EQ(status::ok, tx->commit());
    }
}

TEST_F(kvs_storage_test, scan_range_inclusive_exclusive) {
    auto t1 = db_->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db_->create_transaction();
        {
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k0", "v0"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k1", "v1"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k2", "v2"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k3", "v3"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k4", "v4"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view k;
        std::string_view v;
        std::unique_ptr<iterator> it{};
        ASSERT_EQ(status::ok, t1->content_scan(*tx, "k1", end_point_kind::inclusive, "k3", end_point_kind::exclusive, it));
        ASSERT_TRUE(it);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k1", k);
        EXPECT_EQ("v1", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k2", k);
        EXPECT_EQ("v2", v);
        ASSERT_EQ(status::not_found, it->next());
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view k;
        std::string_view v;
        std::unique_ptr<iterator> it{};
        ASSERT_EQ(status::ok, t1->content_scan(*tx, "k1", end_point_kind::exclusive, "k3", end_point_kind::inclusive,it));
        ASSERT_TRUE(it);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k2", k);
        EXPECT_EQ("v2", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k3", k);
        EXPECT_EQ("v3", v);
        ASSERT_EQ(status::not_found, it->next());
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
}

TEST_F(kvs_storage_test, scan_range_prefix_inclusive_exclusive) {
    auto t1 = db_->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db_->create_transaction();
        {
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k0", "v0"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k1/0", "v1/0"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k1/1", "v1/1"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k2", "v2"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k3/0", "v3/0"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k3/1", "v3/1"));
            ASSERT_EQ(status::ok, t1->content_put(*tx, "k4", "v4"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view k;
        std::string_view v;
        std::unique_ptr<iterator> it{};
        ASSERT_EQ(status::ok, t1->content_scan(*tx, "k1/", end_point_kind::prefixed_inclusive, "k3/", end_point_kind::prefixed_exclusive, it));
        ASSERT_TRUE(it);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k1/0", k);
        EXPECT_EQ("v1/0", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k1/1", k);
        EXPECT_EQ("v1/1", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k2", k);
        EXPECT_EQ("v2", v);
        ASSERT_EQ(status::not_found, it->next());
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
    {
        auto tx = db_->create_transaction();
        std::string_view k;
        std::string_view v;
        std::unique_ptr<iterator> it{};
        ASSERT_EQ(status::ok, t1->content_scan(*tx, "k1/", end_point_kind::prefixed_exclusive, "k3/", end_point_kind::prefixed_inclusive, it));
        ASSERT_TRUE(it);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k2", k);
        EXPECT_EQ("v2", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k3/0", k);
        EXPECT_EQ("v3/0", v);
        ASSERT_EQ(status::ok, it->next());
        ASSERT_EQ(status::ok, it->read_key(k));
        ASSERT_EQ(status::ok, it->read_value(v));
        EXPECT_EQ("k3/1", k);
        EXPECT_EQ("v3/1", v);
        ASSERT_EQ(status::not_found, it->next());
        ASSERT_EQ(status::ok, tx->abort_transaction());
    }
}

// returns the keys read by the scan with the options
static std::vector<std::string> scan_keys(
    storage& stg,
    std::string_view begin,
    end_point_kind begin_kind,
    std::string_view end,
    end_point_kind end_kind,
    scan_options const& options,
    transaction& tx
) {
    std::vector<std::string> ret{};
    std::unique_ptr<iterator> it{};
    EXPECT_EQ(status::ok, stg.content_scan(tx, begin, begin_kind, end, end_kind, options, it));
    if(! it) {
        return ret;
    }
    while(true) {
        auto st = it->next();
        if(st == status::not_found) {
            break;
        }
        EXPECT_EQ(status::ok, st);
        if(st != status::ok) {
            break;
        }
        std::string_view k{};
        if(it->read_key(k) == status::not_found) {
            continue;
        }
        ret.emplace_back(k);
    }
    return ret;
}

TEST_F(kvs_storage_test, reverse_scan_with_endpoints) {
    // tsurugi-issues#1589 R7 - the reverse scan with iscan reads the same range as the forward scan in reverse order,
    // where the range (begin and end) is given in the order of the key also for the reverse scan
    auto t1 = db_->create_storage("T1");
    ASSERT_TRUE(t1);
    {
        auto tx = db_->create_transaction();
        for(auto&& k : {"k0", "k1/0", "k1/1", "k2", "k3/0", "k3/1", "k4", "k5"}) {
            ASSERT_EQ(status::ok, t1->content_put(*tx, k, "v"));
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        // the deleted entry is skipped
        auto tx = db_->create_transaction();
        ASSERT_EQ(status::ok, t1->content_delete(*tx, "k4"));
        ASSERT_EQ(status::ok, tx->commit());
    }
    std::vector<std::pair<std::string_view, end_point_kind>> begins{
        {"", end_point_kind::unbound},
        {"k1/", end_point_kind::prefixed_inclusive},
        {"k1/", end_point_kind::prefixed_exclusive},
        {"k2", end_point_kind::inclusive},
        {"k2", end_point_kind::exclusive},
    };
    std::vector<std::pair<std::string_view, end_point_kind>> ends{
        {"", end_point_kind::unbound},
        {"k3/", end_point_kind::prefixed_inclusive},
        {"k3/", end_point_kind::prefixed_exclusive},
        {"k4", end_point_kind::inclusive},
        {"k5", end_point_kind::exclusive},
    };
    scan_options forward{};
    forward.legacy_scan_ = false;
    scan_options reverse{};
    reverse.legacy_scan_ = false;
    reverse.reverse_ = true;
    for(auto&& [b, bk] : begins) {
        for(auto&& [e, ek] : ends) {
            std::stringstream trace{};
            trace << b << "(" << bk << ") - " << e << "(" << ek << ")";
            SCOPED_TRACE(trace.str());
            auto tx = db_->create_transaction();
            auto expected = scan_keys(*t1, b, bk, e, ek, forward, *tx);
            std::reverse(expected.begin(), expected.end());
            auto actual = scan_keys(*t1, b, bk, e, ek, reverse, *tx);
            EXPECT_EQ(expected, actual);
            ASSERT_EQ(status::ok, tx->abort_transaction());
        }
    }
}

}
