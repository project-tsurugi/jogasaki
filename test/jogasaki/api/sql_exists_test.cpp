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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief Tests for EXISTS and NOT EXISTS predicates (tsurugi-issues #868).
 *
 * Covers two plan-level categories:
 *
 * **Separable** : EXISTS/NOT EXISTS used directly as a
 * WHERE-clause filter.  The optimizer lifts the predicate to a semi/anti join,
 * so the execution plan shows `"operator_kind":"semi"` or `"operator_kind":"anti"`.
 *
 * **Non-separable** : EXISTS/NOT EXISTS used as a scalar Boolean
 * value (e.g., in the SELECT column list).  The optimizer cannot convert it
 * directly to a semi/anti join and instead emits a left-outer join, so the
 * execution plan shows `"operator_kind":"left_outer"` (or the
 * `"left_outer_at_most_one"` variant).
 */
class sql_exists_test :
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

    /**
     * @brief Check whether the execution plan for the given query contains a specific substring.
     * @param query the SQL query to inspect
     * @param op_name the substring to look for in the plan JSON
     * @return true if the plan contains op_name
     */
    bool has_operator(std::string_view query, std::string_view op_name) {
        std::string plan{};
        explain_statement(query, plan);
        return plan.find(op_name) != std::string::npos;
    }
};

using namespace std::string_view_literals;

// ---------------------------------------------------------------------------
// Separable EXISTS cases: WHERE EXISTS → semi join
// ---------------------------------------------------------------------------

/**
 * @brief Separable EXISTS with a non-empty subquery returns all outer rows.
 *
 * @details When EXISTS is used as a top-level WHERE filter and the subquery
 * returns at least one row, every row from the outer table is included.
 * The plan must use a semi join ("operator_kind":"semi").
 */
TEST_F(sql_exists_test, separable_exists_non_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");

    auto query = "SELECT c0, c1 FROM t0 WHERE EXISTS (SELECT * FROM t1) ORDER BY c0";
    EXPECT_TRUE(has_operator(query, R"("operator_kind":"semi")"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
}

/**
 * @brief Separable EXISTS with an empty subquery returns no rows.
 *
 * @details When EXISTS is used as a top-level WHERE filter and the subquery
 * returns no rows, no outer row passes the filter.
 * The plan must use a semi join ("operator_kind":"semi").
 */
TEST_F(sql_exists_test, separable_exists_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    // t1 is intentionally left empty

    auto query = "SELECT c0, c1 FROM t0 WHERE EXISTS (SELECT * FROM t1) ORDER BY c0";
    EXPECT_TRUE(has_operator(query, R"("operator_kind":"semi")"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(0, result.size());
}


// ---------------------------------------------------------------------------
// Separable NOT EXISTS cases: WHERE NOT EXISTS → anti join
// ---------------------------------------------------------------------------

/**
 * @brief Separable NOT EXISTS with a non-empty subquery returns no rows.
 *
 * @details When NOT EXISTS is used as a top-level WHERE filter and the
 * subquery returns at least one row, no outer row passes the filter.
 * The plan must use an anti join ("operator_kind":"anti").
 */
TEST_F(sql_exists_test, separable_not_exists_non_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");

    auto query = "SELECT c0, c1 FROM t0 WHERE NOT EXISTS (SELECT * FROM t1) ORDER BY c0";
    EXPECT_TRUE(has_operator(query, R"("operator_kind":"anti")"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(0, result.size());
}

/**
 * @brief Separable NOT EXISTS with an empty subquery returns all outer rows.
 *
 * @details When NOT EXISTS is used as a top-level WHERE filter and the
 * subquery returns no rows, every outer row passes the filter.
 * The plan must use an anti join ("operator_kind":"anti").
 */
TEST_F(sql_exists_test, separable_not_exists_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    // t1 is intentionally left empty

    auto query = "SELECT c0, c1 FROM t0 WHERE NOT EXISTS (SELECT * FROM t1) ORDER BY c0";
    EXPECT_TRUE(has_operator(query, R"("operator_kind":"anti")"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
}

// ---------------------------------------------------------------------------
// Non-separable EXISTS cases: EXISTS as Boolean value → left_outer join
// ---------------------------------------------------------------------------

/**
 * @brief Non-separable EXISTS as a Boolean output column returns true for each
 * outer row when the subquery is non-empty.
 *
 * @details When EXISTS is used as a scalar value in the SELECT column list it
 * cannot be lifted to a semi join.  The optimizer emits a left-outer join
 * (join kind "left_outer" or "left_outer_at_most_one"), so the plan must
 * contain the string "left_outer".
 */
TEST_F(sql_exists_test, nonseparable_exists_boolean_non_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");

    auto query = "SELECT c0, c1, EXISTS (SELECT * FROM t1) FROM t0 ORDER BY c0";
    EXPECT_TRUE(has_operator(query, "left_outer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(1, 10, true)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(2, 20, true)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(3, 30, true)), result[2]);
}

/**
 * @brief Non-separable EXISTS as a Boolean output column returns false for each
 * outer row when the subquery is empty.
 *
 * @details EXISTS on an empty subquery yields false for every outer row.
 * The plan must contain "left_outer".
 */
TEST_F(sql_exists_test, nonseparable_exists_boolean_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    // t1 is intentionally left empty

    auto query = "SELECT c0, c1, EXISTS (SELECT * FROM t1) FROM t0 ORDER BY c0";
    EXPECT_TRUE(has_operator(query, "left_outer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(1, 10, false)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(2, 20, false)), result[1]);
}

/**
 * @brief Non-separable NOT EXISTS as a Boolean output column returns false for
 * each outer row when the subquery is non-empty.
 *
 * @details The plan must contain "left_outer".
 */
TEST_F(sql_exists_test, nonseparable_not_exists_boolean_non_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");

    auto query = "SELECT c0, c1, NOT EXISTS (SELECT * FROM t1) FROM t0 ORDER BY c0";
    EXPECT_TRUE(has_operator(query, "left_outer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(1, 10, false)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(2, 20, false)), result[1]);
}

/**
 * @brief Non-separable NOT EXISTS as a Boolean output column returns true for
 * each outer row when the subquery is empty.
 *
 * @details The plan must contain "left_outer".
 */
TEST_F(sql_exists_test, nonseparable_not_exists_boolean_empty_subquery) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    // t1 is intentionally left empty

    auto query = "SELECT c0, c1, NOT EXISTS (SELECT * FROM t1) FROM t0 ORDER BY c0";
    EXPECT_TRUE(has_operator(query, "left_outer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(1, 10, true)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(2, 20, true)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::boolean>(3, 30, true)), result[2]);
}

}  // namespace jogasaki::testing
