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
#include <iostream>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/api/api_test_base.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/dist/key_range.h>
#include <jogasaki/dist/uniform_key_distribution.h>
#include <jogasaki/kvs_test_utils.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/create_tx.h>

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
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1),(2),(3)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = get_storage(*db, "t");
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
}

TEST_F(uniform_distribution_test, complex_primary_key) {
    execute_statement("create table t (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("insert into t values (1,10),(2,20),(3,30)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = get_storage(*db, "t");
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

TEST_F(uniform_distribution_test, gen_strings) {
    auto res = generate_strings("a1", "a3", 3);
    EXPECT_EQ(6, res.size());
    EXPECT_EQ("a1\x00"sv, res[0]);
    EXPECT_EQ("a1\x01"sv, res[1]);
    EXPECT_EQ("a1\x02"sv, res[2]);
    EXPECT_EQ("a2\x00"sv, res[3]);
    EXPECT_EQ("a2\x01"sv, res[4]);
    EXPECT_EQ("a2\x02"sv, res[5]);
}

TEST_F(uniform_distribution_test, compute_pivots) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1),(2),(3)");

    auto db = api::impl::get_impl(*db_).kvs_db();
    auto stg = get_storage(*db, "t");
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
