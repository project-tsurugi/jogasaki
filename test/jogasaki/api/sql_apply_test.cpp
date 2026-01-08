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
#include <vector>

#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief test for APPLY operator (CROSS APPLY / OUTER APPLY)
 * @details this test uses mock table-valued functions to test the APPLY operator.
 */
class sql_apply_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        // enable mock table-valued functions for testing
        cfg->mock_table_valued_functions(true);
        db_setup(cfg);

        // create test table
        execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(sql_apply_test, cross_apply_basic) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // CROSS APPLY with mock_table_func_fixed
    // mock_table_func_fixed(multiplier) returns:
    //   (1 * multiplier, 100 * multiplier)
    //   (2 * multiplier, 200 * multiplier)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected output:
    // T.C0=1, T.C1=100 × 2 rows from function = 2 rows
    // T.C0=2, T.C1=200 × 2 rows from function = 2 rows
    // total: 4 rows
    ASSERT_EQ(4, result.size());

    std::sort(result.begin(), result.end());

    // first input row (1, 100) × function output (1*1, 100*1) and (2*1, 200*1)
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 2, 200)), result[1]);

    // second input row (2, 200) × function output (1*2, 100*2) and (2*2, 200*2)
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(2, 200, 2, 200)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(2, 200, 4, 400)), result[3]);
}

TEST_F(sql_apply_test, cross_apply_empty_input) {
    // no data in table T
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected: empty output
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_apply_test, cross_apply_empty_right) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // mock_table_func_empty() returns empty result
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_empty() AS R(c1, c2)",
        result
    );

    // expected: empty output (CROSS APPLY eliminates rows when right side is empty)
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_apply_test, outer_apply_empty_right) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // mock_table_func_empty() returns empty result
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T OUTER APPLY mock_table_func_empty() AS R(c1, c2)",
        result
    );

    // expected: 2 rows with NULL for R.c1 and R.c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
        std::tuple{1, 100, 0, 0}, {false, false, true, true})), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
        std::tuple{2, 200, 0, 0}, {false, false, true, true})), result[1]);
}

TEST_F(sql_apply_test, cross_apply_multiple_rows) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // mock_table_func_generate(count) returns N rows: (1, 10), (2, 20), ..., (N, N*10)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1, R.c2 FROM T CROSS APPLY mock_table_func_generate(3::int) AS R(c1, c2)",
        result
    );

    // expected: 1 input row × 3 rows from function = 3 rows
    ASSERT_EQ(3, result.size());

    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 3, 30)), result[2]);
}

TEST_F(sql_apply_test, cross_apply_with_where) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");
    execute_statement("INSERT INTO T VALUES (3, 300)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2) "
        "WHERE T.C0 = 2",
        result
    );

    // expected: only rows for T.C0 = 2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 4)), result[1]);
}

TEST_F(sql_apply_test, outer_apply_basic) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T OUTER APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected: same as CROSS APPLY when right side is not empty
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_parameter_from_function) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (5, 100)");

    // use function result as parameter to another APPLY
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_generate(T.C0) AS R(c1, c2)",
        result
    );

    // expected: 1 input row (C0=5) × 5 rows from function = 5 rows
    ASSERT_EQ(5, result.size());

    std::sort(result.begin(), result.end());
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(5, i + 1)), result[i]);
    }
}

TEST_F(sql_apply_test, cross_apply_twice) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // CROSS APPLY twice: first APPLY generates rows, second APPLY uses those rows
    // mock_table_func_fixed(multiplier) returns (multiplier, 100*multiplier), (2*multiplier, 200*multiplier)
    // Then mock_table_func_generate(count) returns N rows: (1, 10), (2, 20), ..., (N, N*10)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R1.c1, R2.c1, R2.c2 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R1 "
        "CROSS APPLY mock_table_func_generate(R1.c1) AS R2(c1, c2)",
        result
    );

    // expected: complex nested result
    // For T.C0=1: R1 has (1,100) and (2,200), then for each R1.c1, generate R1.c1 rows
    // For (1,100): generate 1 row -> (1,10)
    // For (2,200): generate 2 rows -> (1,10), (2,20)
    // For T.C0=2: R1 has (2,200) and (4,400), then for each R1.c1, generate R1.c1 rows
    // For (2,200): generate 2 rows -> (1,10), (2,20)
    // For (4,400): generate 4 rows -> (1,10), (2,20), (3,30), (4,40)
    // Total: 1 + 2 + 2 + 4 = 9 rows
    ASSERT_EQ(9, result.size());

    std::sort(result.begin(), result.end());

    // Expected rows after sorting
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 1, 1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 2, 1, 10)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 2, 2, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 2, 1, 10)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 2, 2, 20)), result[4]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 1, 10)), result[5]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 2, 20)), result[6]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 3, 30)), result[7]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 4, 40)), result[8]);
}

TEST_F(sql_apply_test, cross_apply_column_alias) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Function returns (c1, c2), but SQL specifies AS R(c2, c1) - column names are swapped
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c2, R.c1 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c2, c1)",
        result
    );

    // mock_table_func_fixed(1) returns (1, 100), (2, 200)
    // With AS R(c2, c1), R.c2 gets function's c1, R.c1 gets function's c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c2=function.c1=1, R.c1=function.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100)), result[0]);
    // Second row: T.C0=1, R.c2=function.c1=2, R.c1=function.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_column_alias_different_names) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Function returns (c1, c2), but SQL specifies AS R(c10, c20) - completely different names
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c10, R.c20 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c10, c20)",
        result
    );

    // mock_table_func_fixed(1) returns (1, 100), (2, 200)
    // With AS R(c10, c20), R.c10 gets function's c1, R.c20 gets function's c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c10=function.c1=1, R.c20=function.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100)), result[0]);
    // Second row: T.C0=1, R.c10=function.c1=2, R.c20=function.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_unused_columns) {
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Function returns 3 columns (c1, c2, c3), but SQL only uses c2 - other columns should be discarded
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c2 "
        "FROM T CROSS APPLY mock_table_func_three_columns(T.C0) AS R(c1, c2, c3)",
        result
    );

    // mock_table_func_three_columns(1) returns (1, 100, 1000), (2, 200, 2000)
    // Only c2 is used in SELECT, c1 and c3 are discarded
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(1, 100)), result[0]);
    // Second row: T.C0=1, R.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(1, 200)), result[1]);
}

} // namespace jogasaki::testing
