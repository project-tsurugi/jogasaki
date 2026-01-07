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

    auto& rec1 = result[0];
    EXPECT_FALSE(rec1.is_null(0));  // T.C0
    EXPECT_EQ(1, rec1.get_value<std::int32_t>(0));
    EXPECT_FALSE(rec1.is_null(1));  // T.C1
    EXPECT_EQ(100, rec1.get_value<std::int64_t>(1));
    EXPECT_TRUE(rec1.is_null(2));   // R.c1 (NULL)
    EXPECT_TRUE(rec1.is_null(3));   // R.c2 (NULL)

    auto& rec2 = result[1];
    EXPECT_FALSE(rec2.is_null(0));  // T.C0
    EXPECT_EQ(2, rec2.get_value<std::int32_t>(0));
    EXPECT_FALSE(rec2.is_null(1));  // T.C1
    EXPECT_EQ(200, rec2.get_value<std::int64_t>(1));
    EXPECT_TRUE(rec2.is_null(2));   // R.c1 (NULL)
    EXPECT_TRUE(rec2.is_null(3));   // R.c2 (NULL)
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
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(5, i + 1)), result[i]);
    }
}

} // namespace jogasaki::testing
