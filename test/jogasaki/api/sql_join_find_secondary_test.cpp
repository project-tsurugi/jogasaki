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

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

class sql_join_find_secondary_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    bool has_join_find(std::string_view query);
    bool uses_secondary(std::string_view query);
    bool has_filter(std::string_view query);

    void run_index(std::string const& dir);
    void run_index_with_null(std::string const& dir);
    void run_left_outer(std::string const& dir);
    void run_composite_index(std::string const& c0_dir, std::string const& c1_dir);
};

using namespace std::string_view_literals;

static bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_join_find_secondary_test::has_join_find(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "join_find");
}

bool sql_join_find_secondary_test::uses_secondary(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"i1\"");
}

bool sql_join_find_secondary_test::has_filter(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"filter\"");
}

void sql_join_find_secondary_test::run_index(std::string const& dir) {
    // t0 drives the join; t1 has secondary index on c1 with a NULL row that must not match
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i1 on t1(c1 " + dir + ")");
    execute_statement("INSERT INTO t1 VALUES (10, 1),(11, 1),(12, NULL)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c1";
    EXPECT_TRUE(has_join_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 11, 1)), result[1]);
}

void sql_join_find_secondary_test::run_index_with_null(std::string const& dir) {
    // NULL in either side of the join condition must not produce a match
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (null),(1)");
    execute_statement("CREATE TABLE t1 (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i1 on t1(c1 " + dir + ")");
    execute_statement("INSERT INTO t1 VALUES (10, null),(11, 1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c1";
    EXPECT_TRUE(has_join_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 11, 1)), result[0]);
}

void sql_join_find_secondary_test::run_left_outer(std::string const& dir) {
    // unmatched t0 rows produce null-padded result; NULL in t1 does not match
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i1 on t1(c1 " + dir + ")");
    execute_statement("INSERT INTO t1 VALUES (10, 1),(11, 1),(12, NULL)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 left outer join t1 on t0.c0=t1.c1";
    EXPECT_TRUE(has_join_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 11, 1)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(2, std::nullopt, std::nullopt)), result[2]);
}

void sql_join_find_secondary_test::run_composite_index(
    std::string const& c0_dir, std::string const& c1_dir) {
    // join on both columns of the composite secondary index; rows with NULL in either
    // index column must not match
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 11),(2, 12)");
    execute_statement("CREATE TABLE t1 (pk int primary key, c0 int, c1 int, c2 int)");
    execute_statement("CREATE INDEX i1 on t1(c0 " + c0_dir + ", c1 " + c1_dir + ")");
    execute_statement(
        "INSERT INTO t1 VALUES "
        "(1, 1, 11, 100),(2, 1, 11, 101),(3, 2, 20, 200),(4, NULL, 11, 300),(5, 1, NULL, 400)"
    );

    auto query = "SELECT t0.c0, t0.c1, t1.c0, t1.c1, t1.c2 FROM t0 join t1 on t0.c0=t1.c0 and t0.c1=t1.c1";
    EXPECT_TRUE(has_join_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4, kind::int4>(1, 11, 1, 11, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4, kind::int4>(1, 11, 1, 11, 101)), result[1]);
}

TEST_F(sql_join_find_secondary_test, index) {
    run_index("ASC");
}

TEST_F(sql_join_find_secondary_test, desc) {
    run_index("DESC");
}

TEST_F(sql_join_find_secondary_test, with_null) {
    run_index_with_null("ASC");
}

TEST_F(sql_join_find_secondary_test, with_null_desc) {
    run_index_with_null("DESC");
}

TEST_F(sql_join_find_secondary_test, left_outer) {
    run_left_outer("ASC");
}

TEST_F(sql_join_find_secondary_test, left_outer_desc) {
    run_left_outer("DESC");
}

TEST_F(sql_join_find_secondary_test, composite_key_asc_asc) {
    run_composite_index("ASC", "ASC");
}

TEST_F(sql_join_find_secondary_test, composite_key_asc_desc) {
    run_composite_index("ASC", "DESC");
}

TEST_F(sql_join_find_secondary_test, composite_key_desc_asc) {
    run_composite_index("DESC", "ASC");
}

TEST_F(sql_join_find_secondary_test, composite_key_desc_desc) {
    run_composite_index("DESC", "DESC");
}

TEST_F(sql_join_find_secondary_test, semi_join) {
    execute_statement("CREATE TABLE t0 (c0 INT)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t1(c1 ASC)");
    execute_statement("INSERT INTO t1 VALUES (10, 1),(11, 1),(12, NULL)");

    auto query = "SELECT t0.c0 FROM t0 WHERE t0.c0 IN (SELECT c1 FROM t1)";
    std::string plan{};
    explain_statement(query, plan);
    EXPECT_TRUE(contains(plan, "join_find"));
    EXPECT_TRUE(contains(plan, "\"i1\""));
    EXPECT_TRUE(contains(plan, "semi"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
}

// anti_join is not used for NOT IN and cannot be tested here
// TODO add test for anti_join when supported

} // namespace jogasaki::testing
