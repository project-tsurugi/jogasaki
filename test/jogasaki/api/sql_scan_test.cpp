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

class sql_scan_test :
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

    bool has_scan(std::string_view query);
    bool has_filter(std::string_view query);
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_scan_test::has_scan(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "scan") && ! contains(plan, "join_scan");
}

bool sql_scan_test::has_filter(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"filter\"");
}

TEST_F(sql_scan_test, simple) {
    // basic range scan on primary key
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 <= 4";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[2]);
}

TEST_F(sql_scan_test, simple_range_both_bounded) {
    // verify all four combinations of inclusive/exclusive endpoints on both sides
    // data: c0 in {1,2,3,4,5}
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    {
        // exclusive on both sides: only c0=3
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2 AND c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[0]);
    }

    {
        // inclusive lower, exclusive upper: c0 in {2,3}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[1]);
    }

    {
        // exclusive lower, inclusive upper: c0 in {3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2 AND c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[1]);
    }

    {
        // inclusive on both sides: c0 in {2,3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[2]);
    }
}

TEST_F(sql_scan_test, simple_range_half_open) {
    // verify half-open range scans (only a lower bound or only an upper bound)
    // data: c0 in {1,2,3,4,5}
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    {
        // lower bound exclusive: c0 in {3,4,5}
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(5, 50)), result[2]);
    }

    {
        // lower bound inclusive: c0 in {2,3,4,5}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(5, 50)), result[3]);
    }

    {
        // upper bound exclusive: c0 in {1,2,3}
        auto query = "SELECT c0, c1 FROM t WHERE c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
    }

    {
        // upper bound inclusive: c0 in {1,2,3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[3]);
    }
}

TEST_F(sql_scan_test, composite_pk_simple) {
    // basic range scan on the leading column of a composite primary key
    execute_statement("CREATE TABLE t (c0 INT, c1 INT, c2 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t VALUES (1, 0, 10), (2, 0, 20), (3, 0, 30), (4, 0, 40), (5, 0, 50)");

    auto query = "SELECT c0, c1, c2 FROM t WHERE c0 >= 2 AND c0 <= 4";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 0, 20)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 0, 30)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(4, 0, 40)), result[2]);
}

TEST_F(sql_scan_test, composite_pk_range_both_bounded) {
    // verify all four combinations of inclusive/exclusive endpoints on both sides
    // using a range condition on the leading column of a composite primary key
    // data: c0 in {1,2,3,4,5}
    execute_statement("CREATE TABLE t (c0 INT, c1 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");

    {
        // exclusive on both sides: only c0=3
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2 AND c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[0]);
    }

    {
        // inclusive lower, exclusive upper: c0 in {2,3}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[1]);
    }

    {
        // exclusive lower, inclusive upper: c0 in {3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2 AND c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 4)), result[1]);
    }

    {
        // inclusive on both sides: c0 in {2,3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 4)), result[2]);
    }
}

TEST_F(sql_scan_test, composite_pk_range_half_open) {
    // verify half-open range scans on the leading column of a composite primary key
    // data: c0 in {1,2,3,4,5}
    execute_statement("CREATE TABLE t (c0 INT, c1 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");

    {
        // lower bound exclusive: c0 in {3,4,5}
        auto query = "SELECT c0, c1 FROM t WHERE c0 > 2";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 4)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(5, 5)), result[2]);
    }

    {
        // lower bound inclusive: c0 in {2,3,4,5}
        auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 4)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(5, 5)), result[3]);
    }

    {
        // upper bound exclusive: c0 in {1,2,3}
        auto query = "SELECT c0, c1 FROM t WHERE c0 < 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[2]);
    }

    {
        // upper bound inclusive: c0 in {1,2,3,4}
        auto query = "SELECT c0, c1 FROM t WHERE c0 <= 4";
        EXPECT_TRUE(has_scan(query));
        EXPECT_TRUE(! has_filter(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 3)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 4)), result[3]);
    }
}

TEST_F(sql_scan_test, composite_pk_prefix_condition) {
    // verify that a condition on only the leading column of a composite primary key
    // (c0, c1) causes the planner to emit a scan operator using the primary key index.
    // WHERE c0 = 1 constrains only the prefix of the key, so the planner
    // must perform a range scan (not a find) on the primary key index.
    execute_statement("CREATE TABLE t (c0 INT, c1 INT, c2 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t VALUES (1, 10, 100), (1, 20, 200), (2, 10, 100), (1, 30, 300)");

    auto query = "SELECT c0, c1, c2 FROM t WHERE c0 = 1";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 20, 200)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 30, 300)), result[2]);
}

} // namespace jogasaki::testing
