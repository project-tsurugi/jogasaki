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

class sql_find_test :
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

    bool has_find(std::string_view query);
    bool uses_secondary(std::string_view query);
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_find_test::has_find(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "find") && ! contains(plan, "join_find");
}

bool sql_find_test::uses_secondary(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"i1\"");
}

TEST_F(sql_find_test, simple) {
    // basic find by single-column primary key
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");

    auto query = "SELECT c0, c1 FROM t WHERE c0 = 2";
    EXPECT_TRUE(has_find(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[0]);
}

TEST_F(sql_find_test, composite_key) {
    // find by composite primary key — full key equality
    execute_statement("CREATE TABLE t (c0 INT, c1 INT, c2 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t VALUES (1, 10, 100), (1, 20, 200), (2, 10, 300)");

    auto query = "SELECT c0, c1, c2 FROM t WHERE c0 = 1 AND c1 = 10";
    EXPECT_TRUE(has_find(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
}

TEST_F(sql_find_test, secondary_index) {
    // find via secondary index — equality condition on secondary index column
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    execute_statement("INSERT INTO t VALUES (10, 1), (20, 2), (30, 1)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 = 1";
    EXPECT_TRUE(has_find(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(30, 1)), result[1]);
}

TEST_F(sql_find_test, secondary_index_with_null) {
    // verify that NULL values stored in a secondary-indexed column are not
    // incorrectly matched when performing a find via the secondary index
    // (primary key columns cannot be NULL, so secondary index is used here)
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    execute_statement("INSERT INTO t VALUES (10, NULL), (11, 1), (12, NULL)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 = 1";
    EXPECT_TRUE(has_find(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(11, 1)), result[0]);
}

} // namespace jogasaki::testing
