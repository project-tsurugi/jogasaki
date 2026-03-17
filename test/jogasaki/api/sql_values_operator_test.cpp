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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;
using triple = takatori::decimal::triple;

/**
 * @brief Tests for the new `values` operator (tsurugi-issues #872).
 *
 * The values operator takes over VALUES-clause evaluation from write_statement
 * so that scalar subqueries inside VALUES can be supported.
 *
 * Test policy (from values-operator.md):
 * - Verify that `SELECT 1` style queries are compiled via the values operator
 *   and return correct results.
 * - Verify that INSERT statements with scalar subqueries inside VALUES complete
 *   correctly.
 *
 * These tests are expected to FAIL until the values operator is implemented and
 * registered in operator_builder.cpp.
 */
class sql_values_operator_test :
    public ::testing::Test,
    public api_test_base {

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

using namespace std::string_view_literals;

// ---------------------------------------------------------------------------
// SELECT with literal values (no FROM clause) — compiled via values operator
// ---------------------------------------------------------------------------

TEST_F(sql_values_operator_test, query_select_integer_literal) {
    // Integer literals are typed as INT8 (BIGINT) by the SQL compiler when
    // prefer_small_integer_literals is false (the default).
    std::vector<mock::basic_record> result{};
    execute_query("SELECT 1", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[0]);
}

TEST_F(sql_values_operator_test, query_select_multiple_literals) {
    // Verify that multiple literal columns produce one row with all values set.
    std::vector<mock::basic_record> result{};
    execute_query("SELECT 1, 2", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 2)), result[0]);
}

TEST_F(sql_values_operator_test, query_select_arithmetic_expression) {
    // Verify that arithmetic expressions in a literal SELECT are evaluated.
    std::vector<mock::basic_record> result{};
    execute_query("SELECT 1 + 2", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8>(3)), result[0]);
}

// ---------------------------------------------------------------------------
// INSERT with scalar subquery in VALUES — requires values operator
// ---------------------------------------------------------------------------

TEST_F(sql_values_operator_test, insert_with_scalar_subquery) {
    // verify that a scalar subquery in VALUES is evaluated and inserted correctly.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t2 VALUES (1, (SELECT c1 FROM t1 WHERE c0 = 1))");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0, c1 FROM t2", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
}

TEST_F(sql_values_operator_test, insert_with_scalar_subquery_no_match) {
    // Verify that a scalar subquery returning no rows inserts NULL for that column.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t2 VALUES (1, (SELECT c1 FROM t1 WHERE c0 = 999))");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0, c1 FROM t2", result);
    ASSERT_EQ(1, result.size());
    // c0 = 1 (not null), c1 = NULL
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, std::nullopt)), result[0]);
}

TEST_F(sql_values_operator_test, insert_with_scalar_subquery_no_match_not_null_column) {
    // Verify that a scalar subquery returning no rows inserts NULL but the column is not nullable.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT NOT NULL)");
    test_stmt_err("INSERT INTO t2 VALUES (1, (SELECT c1 FROM t1 WHERE c0 = 999))", error_code::not_null_constraint_violation_exception);
}

TEST_F(sql_values_operator_test, insert_with_scalar_subquery_multiple_rows_error) {
    // Verify that a scalar subquery returning more than one row causes an error.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");
    execute_statement("INSERT INTO t1 VALUES (2, 200)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT)");
    test_stmt_err(
        "INSERT INTO t2 VALUES (1, (SELECT c1 FROM t1))",
        error_code::scalar_subquery_evaluation_exception
    );
}

TEST_F(sql_values_operator_test, query_select_host_variable) {
    // Verify that a host variable reference in a FROM-less SELECT returns the bound value.
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"hv", api::field_type_kind::int8}
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("hv", 42);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT :hv", variables, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8>(42)), result[0]);
}

TEST_F(sql_values_operator_test, query_select_builtin_function) {
    // Verify that a built-in function call in a FROM-less SELECT is evaluated.
    // abs(-2): integer literal; abs returns INT8 2.
    std::vector<mock::basic_record> result{};
    execute_query("SELECT abs(-2)", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8>(2)), result[0]);
}

TEST_F(sql_values_operator_test, insert_with_scalar_subquery_multiple_expressions) {
    // Verify that multiple columns in VALUES can each contain a scalar subquery.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");
    execute_statement("INSERT INTO t1 VALUES (2, 200)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement(
        "INSERT INTO t2 VALUES ("
        "  1,"
        "  (SELECT c1 FROM t1 WHERE c0 = 1),"
        "  (SELECT c1 FROM t1 WHERE c0 = 2)"
        ")"
    );
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0, c1, c2 FROM t2", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 100, 200)), result[0]);
}

TEST_F(sql_values_operator_test, query_select_values_multiple_rows) {
    // Verify that a VALUES table expression with multiple tuples returns all rows.
    // The values operator iterates over all tuples and emits one row per tuple.
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0, c1, c2 FROM (VALUES (1, 2, 3), (4, 5, 6)) AS t(c0, c1, c2)", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int8>(1, 2, 3)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int8>(4, 5, 6)), result[1]);
}

TEST_F(sql_values_operator_test, query_values_multiple_rows_standalone) {
    // Verify that a bare VALUES clause (without SELECT ... FROM wrapper) returns all rows.
    // This exercises the same values operator code path as query_select_values_multiple_rows
    // but via the standalone VALUES query form.
    std::vector<mock::basic_record> result{};
    execute_query("VALUES (1, 2, 3), (4, 5, 6)", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int8>(1, 2, 3)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int8>(4, 5, 6)), result[1]);
}

TEST_F(sql_values_operator_test, query_values_multiple_rows_standalone_different_types) {
    // Verify that a bare VALUES clause with mixed literal types returns all rows correctly.
    // Columns 2-3 have type DECIMAL (inferred from 2.0 / 6.0), while column 1 is INT8.
    // The values operator must coerce INT8 literals (5, 3) to DECIMAL when writing.
    std::vector<mock::basic_record> result{};
    execute_query("VALUES (1, 2.0, 3), (4, 5, 6.0)", result);
    ASSERT_EQ(2, result.size());
    // columns 2-3: decimal(*, *) — unlimited precision/scale; values are whole numbers
    EXPECT_EQ((mock::typed_nullable_record<kind::int8, kind::decimal, kind::decimal>(
        std::tuple{int8_type(), decimal_type(), decimal_type()},
        1, triple{1, 0, 2, 0}, triple{1, 0, 3, 0}
    )), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::int8, kind::decimal, kind::decimal>(
        std::tuple{int8_type(), decimal_type(), decimal_type()},
        4, triple{1, 0, 5, 0}, triple{1, 0, 6, 0}
    )), result[1]);
}

TEST_F(sql_values_operator_test, insert_multiple_rows_with_different_scalar_subqueries) {
    // Verify that a multi-row INSERT with a different scalar subquery in each tuple works.
    // Each VALUES tuple is evaluated independently by the values operator.
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t1 VALUES (1, 100)");
    execute_statement("INSERT INTO t1 VALUES (2, 200)");
    execute_statement("CREATE TABLE t2 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement(
        "INSERT INTO t2 VALUES "
        "  (1, (SELECT c1 FROM t1 WHERE c0 = 1)),"
        "  (2, (SELECT c1 FROM t1 WHERE c0 = 2))"
    );
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0, c1 FROM t2 ORDER BY c0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 200)), result[1]);
}

} // namespace jogasaki::testing
