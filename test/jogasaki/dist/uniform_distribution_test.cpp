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
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/dist/key_range.h>
#include <jogasaki/dist/uniform_key_distribution.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/get_storage_by_index_name.h>

namespace jogasaki::dist {

using namespace std::string_view_literals;

class uniform_distribution_test :
    public ::testing::Test,
    public kvs_test_utils,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

};

TEST_F(uniform_distribution_test, basic) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support uniform key distribution yet";
    }
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1),(2),(3)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = utils::get_storage_by_index_name("t");
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    uniform_key_distribution dist{
        *stg,
        *tctx->object()
    };
    std::string hi{};
    std::string lo{};
    EXPECT_EQ(status::ok, dist.highkey(hi));
    EXPECT_EQ("\x80\x00\x00\x03"sv, hi);
    std::cerr << "highkey: " << utils::binary_printer{hi.data(), hi.size()} << std::endl;
    EXPECT_EQ(status::ok, dist.lowkey(lo));
    EXPECT_EQ("\x80\x00\x00\x01"sv, lo);
    std::cerr << "lowkey: " << utils::binary_printer{lo.data(), lo.size()} << std::endl;
}

TEST_F(uniform_distribution_test, complex_primary_key) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support uniform key distribution yet";
    }
    execute_statement("create table t (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("insert into t values (1,10),(2,20),(3,30)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = utils::get_storage_by_index_name("t");
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    uniform_key_distribution dist{
        *stg,
        *tctx->object()
    };
    std::string hi{};
    std::string lo{};
    EXPECT_EQ(status::ok, dist.highkey(hi));
    std::cerr << "highkey: " << utils::binary_printer{hi.data(), hi.size()} << std::endl;
    EXPECT_EQ("\x80\x00\x00\x03\x80\x00\x00\x1e"sv, hi);
    EXPECT_EQ(status::ok, dist.lowkey(lo));
    std::cerr << "lowkey: " << utils::binary_printer{lo.data(), lo.size()} << std::endl;
    EXPECT_EQ("\x80\x00\x00\x01\x80\x00\x00\x0a"sv, lo);
}

TEST_F(uniform_distribution_test, common_prefix_len) {
    EXPECT_EQ(0, common_prefix_len("", ""));
    EXPECT_EQ(0, common_prefix_len("a", ""));
    EXPECT_EQ(0, common_prefix_len("", "a"));
    EXPECT_EQ(1, common_prefix_len("a", "a"));
    EXPECT_EQ(1, common_prefix_len("a", "ab"));
    EXPECT_EQ(1, common_prefix_len("ab", "a"));
    EXPECT_EQ(2, common_prefix_len("ab", "ab"));
    EXPECT_EQ(2, common_prefix_len("ab", "abc"));
    EXPECT_EQ(2, common_prefix_len("abc", "ab"));
    EXPECT_EQ(3, common_prefix_len("abc", "abc"));
    EXPECT_EQ(3, common_prefix_len("abc", "abcd"));
    EXPECT_EQ(3, common_prefix_len("abcd", "abc"));
}

TEST_F(uniform_distribution_test, generate_strings2_basic) {
    const int n = 15;  // 16 - 1
    auto pivots = generate_strings2(n, "1\x40"sv, "1\x4fzzz"sv);
    ASSERT_EQ(n, pivots.size());
    // diff = "\x00\x0fzzz"; so step (= diff / 16) < "\x00\x01"
    // "1\x40" < p[0] < "1\x41" < p[1] < "1\x42" < ... < "1\x4e" < p[0x0e] < "1\x4f" < "1\x4fzzz"
    for (int i = 0; i < n; i++) {
        EXPECT_GE(pivots[i].size(), 2);
        EXPECT_EQ(pivots[i][0], '1');
        EXPECT_EQ(pivots[i][1], '\x40' + i);
    }
}

TEST_F(uniform_distribution_test, generate_strings2_empty) {
    auto pivots = generate_strings2(9, "0"sv, "0"sv);
    ASSERT_TRUE(pivots.empty());
}

TEST_F(uniform_distribution_test, generate_strings2_invalid_range) {
    auto pivots = generate_strings2(9, "1"sv, "0"sv);
    ASSERT_TRUE(pivots.empty());
}

TEST_F(uniform_distribution_test, generate_strings2_narrow_range_2b) {
    // verify narrow range (fetched from old alogorithm test)
    std::string lkey = "a\x01\xFF";
    std::string rkey = "a\x02";
    auto pivots = generate_strings2(100, lkey, rkey);
    ASSERT_EQ(100, pivots.size());
    ASSERT_LT(lkey, pivots[0]);
    for (int i = 1; i < 100; i++) {
        ASSERT_LT(pivots[i - 1], pivots[i]);
    }
    ASSERT_LT(pivots[99], rkey);
}

TEST_F(uniform_distribution_test, generate_strings2_narrow_range_5b_0) {
    // too narrow range; give up
    auto lkey = "aaa\xff\xff\xff\xff"sv;
    auto rkey = "aab"sv;
    auto pivots = generate_strings2(100, lkey, rkey);
    ASSERT_TRUE(pivots.empty());
}

TEST_F(uniform_distribution_test, generate_strings2_narrow_range_5b_1) {
    // narrow range
    auto lkey = "aaa\xff\xff\xff\xff"sv;
    auto rkey = "aab\x00\x00\x00\x00"sv;
    auto pivots = generate_strings2(100, lkey, rkey);
    ASSERT_EQ(1, pivots.size());
    ASSERT_EQ("aab\x00\x00\x00"sv, pivots[0]);
}

TEST_F(uniform_distribution_test, generate_strings2_narrow_range_5b_2) {
    // narrow range
    auto lkey = "aaa\xff\xff\xfe\xff"sv;
    auto rkey = "aab\x00\x00\x00\x00"sv;
    auto pivots = generate_strings2(100, lkey, rkey);
    ASSERT_LE(1, pivots.size());
    ASSERT_GE(2, pivots.size());
    ASSERT_EQ("aaa\xff\xff\xff"sv, pivots[0]);
    if (pivots.size() == 2) {
        // "aab\x00\x00\x00" is in range, but this is too close to rkey; so may not be in pivots
        ASSERT_EQ("aab\x00\x00\x00"sv, pivots[0]);
    }
}

TEST_F(uniform_distribution_test, compute_pivots) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support uniform key distribution yet";
    }
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1),(2),(3)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = utils::get_storage_by_index_name("t");
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);

    uniform_key_distribution dist{
        *stg,
        *tctx->object()
    };
    std::string hi{};
    std::string lo{};
    EXPECT_EQ(status::ok, dist.highkey(hi));
    std::cerr << "highkey: " << utils::binary_printer{hi.data(), hi.size()} << std::endl;
    EXPECT_EQ(status::ok, dist.lowkey(lo));
    std::cerr << "lowkey: " << utils::binary_printer{lo.data(), lo.size()} << std::endl;

    auto pivots = dist.compute_pivots(10, key_range{"", kvs::end_point_kind::unbound, "", kvs::end_point_kind::unbound});
    EXPECT_EQ(10, pivots.size());
}

}  // namespace jogasaki::dist
