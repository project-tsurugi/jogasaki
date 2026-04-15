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
    bool uses_secondary(std::string_view query);
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

bool sql_scan_test::uses_secondary(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"i1\"");
}

TEST_F(sql_scan_test, simple) {
    // basic range scan on primary key
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)");

    auto query = "SELECT c0, c1 FROM t WHERE c0 >= 2 AND c0 <= 4";
    EXPECT_TRUE(has_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(3, 30)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(4, 40)), result[2]);
}

TEST_F(sql_scan_test, secondary_index) {
    // range scan targeting a secondary index
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    execute_statement("INSERT INTO t VALUES (10, 1), (20, 2), (30, 3), (40, 1), (50, 2)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 >= 1 AND c1 <= 2";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(4, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(20, 2)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(40, 1)), result[2]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(50, 2)), result[3]);
}

TEST_F(sql_scan_test, secondary_index_with_null) {
    // verify that NULL values stored in a secondary-indexed column are not
    // spuriously returned when performing a range scan via the secondary index
    // (primary key columns cannot be NULL, so secondary index is used here)
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    execute_statement("INSERT INTO t VALUES (10, NULL), (11, 1), (12, 2), (13, NULL)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 >= 1 AND c1 <= 2";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(11, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(12, 2)), result[1]);
}

// commented out due to existing issue
TEST_F(sql_scan_test, DISABLED_secondary_index_with_null_less_than) {
    // verify that NULL values are not matched by an upper-open range scan
    // (NULL < 1 is UNKNOWN in SQL, so NULL rows must not appear in the result)
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    execute_statement("INSERT INTO t VALUES (10, NULL), (11, 0), (12, 1), (13, NULL)");

    auto query = "SELECT c1 FROM t WHERE c1 < 1";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(11, 0)), result[0]);
}

TEST_F(sql_scan_test, secondary_index_prefix_condition) {
    // verify that a condition on only the leading column of a composite secondary index
    // (c1, c2) causes the planner to emit a scan operator using that index.
    // WHERE c1 = 1 constrains only the prefix of the index key, so the planner
    // must perform a range scan (not a find) on i1.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1, c2)");
    execute_statement("INSERT INTO t VALUES (10, 1, 100), (11, 1, 200), (12, 2, 100), (13, 1, 300)");

    auto query = "SELECT c0, c1, c2 FROM t WHERE c1 = 1";
    EXPECT_TRUE(has_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(11, 1, 200)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(13, 1, 300)), result[2]);
}

} // namespace jogasaki::testing
