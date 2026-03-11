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
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;

/**
 * @brief test for scalar subquery producing left_outer_at_most_one join kind.
 *
 * @details These tests verify that when a correlated scalar subquery returns
 * more than one row, ScalarSubqueryEvaluationException (SQL-02012) is raised.
 * Three join operator variants are covered:
 *   - join      : merge-style join (right side has no index usable for lookup)
 *   - join_find : index join with single-column primary key lookup
 *   - join_scan : index join with composite primary key range scan
 */
class sql_scalar_subquery_test :
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
     * @brief check whether the given query's execution plan contains a particular operator name.
     * @param query the SQL query to inspect
     * @param op_name the operator name string to look for (e.g. "join_group", "join_find", "join_scan")
     * @return true if the plan contains the operator name
     */
    bool has_operator(std::string_view query, std::string_view op_name) {
        std::string plan{};
        explain_statement(query, plan);
        return plan.find(op_name) != std::string::npos;
    }
};

using namespace std::string_view_literals;

/**
 * @brief verify that a scalar subquery directly in the SELECT list raises an error when it returns multiple rows.
 *
 * @details The scalar subquery result is placed directly in the output column list,
 * so the planner emits a "join" (group exchange) operator with
 * join_kind::left_outer_at_most_one.  Two rows in t1 cause the subquery to
 * return more than one row, which must trigger scalar_subquery_evaluation_exception.
 */
TEST_F(sql_scalar_subquery_test, join_multiple_rows_error) {
    execute_statement("CREATE TABLE t0 (c0 INT)");
    execute_statement("INSERT INTO t0 VALUES (1)");
    execute_statement("CREATE TABLE t1 (c0 INT, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 10)");

    // scalar subquery directly in SELECT list -> join (join_group in plan)
    auto query = "SELECT t0.c0, (SELECT t1.c1 FROM t1) FROM t0";
    EXPECT_TRUE(has_operator(query, "join_group"));

    // with one row in t1 the subquery returns exactly one value: query must succeed
    {
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    }

    // add a second row so the subquery now returns two values: must raise an error
    execute_statement("INSERT INTO t1 VALUES (2, 20)");
    test_stmt_err(query, error_code::scalar_subquery_evaluation_exception);
}

/**
 * @brief verify that a scalar subquery used as a full primary-key lookup raises an error when it returns multiple rows.
 *
 * @details The scalar subquery result is used as the complete primary key value
 * for looking up rows in t2 (single-column PK c0).  The planner emits a
 * join_find operator for the t2 lookup.  Two rows in t1 cause the subquery
 * to return more than one row, which must trigger scalar_subquery_evaluation_exception.
 *
 */
TEST_F(sql_scalar_subquery_test, DISABLED_join_find_multiple_rows_error) {
    execute_statement("CREATE TABLE t1 (c0 INT, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 10)");
    execute_statement("INSERT INTO t1 VALUES (2, 20)");  // two rows: subquery returns 2 values
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t2 VALUES (10, 100), (20, 200)");

    // subquery result used as full (single-column) PK key to look up t2 -> join_find
    auto query = "SELECT t2.c1 FROM t2 WHERE t2.c0 = (SELECT t1.c1 FROM t1)";
    EXPECT_TRUE(has_operator(query, "join_find"));
    test_stmt_err(query, error_code::scalar_subquery_evaluation_exception);
}

/**
 * @brief verify that a scalar subquery used as a partial primary-key lookup raises an error when it returns multiple rows.
 *
 * @details The scalar subquery result is used as only the leading column of the
 * composite primary key (c0, c1) of t2, so the planner emits a join_scan
 * operator for the t2 lookup.  Two rows in t1 cause the subquery to return
 * more than one row, which must trigger scalar_subquery_evaluation_exception.
 *
 */
TEST_F(sql_scalar_subquery_test, DISABLED_join_scan_multiple_rows_error) {
    execute_statement("CREATE TABLE t1 (c0 INT, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 10)");
    execute_statement("INSERT INTO t1 VALUES (2, 20)");  // two rows: subquery returns 2 values
    // t2 has composite PK (c0, c1); matching only c0 yields a range scan
    execute_statement("CREATE TABLE t2 (c0 INT, c1 INT, c2 INT, PRIMARY KEY(c0, c1))");
    execute_statement("INSERT INTO t2 VALUES (10, 1, 100), (20, 2, 200)");

    // subquery result used as partial (leading column only) PK key to look up t2 -> join_scan
    auto query = "SELECT t2.c2 FROM t2 WHERE t2.c0 = (SELECT t1.c1 FROM t1)";
    EXPECT_TRUE(has_operator(query, "join_scan"));
    test_stmt_err(query, error_code::scalar_subquery_evaluation_exception);
}

}  // namespace jogasaki::testing
