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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

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
 * @brief test for APPLY operator with type matrix.
 * @details this test verifies that all data types can be used as arguments and return values
 *          for table-valued functions in APPLY operations.
 * Actually, APPLY operator has not very type-specific logic, but as the end-to-end test, we verify all types here.
 */
class sql_apply_type_matrix_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(sql_apply_type_matrix_test, int4_type) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (100)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 101)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, int8_type) {
    execute_statement("CREATE TABLE T (C0 BIGINT)");
    execute_statement("INSERT INTO T VALUES (1000000000)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int8_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1000000000, 1000000000)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1000000000, 1000000001)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, float4_type) {
    execute_statement("CREATE TABLE T (C0 REAL)");
    execute_statement("INSERT INTO T VALUES (1.5)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_float4_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::float4, kind::float4>(1.5F, 1.5F)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::float4, kind::float4>(1.5F, 2.5F)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, float8_type) {
    execute_statement("CREATE TABLE T (C0 DOUBLE)");
    execute_statement("INSERT INTO T VALUES (2.5)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_float8_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::float8, kind::float8>(2.5, 2.5)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::float8>(2.5, 3.5)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, decimal_type) {
    execute_statement("CREATE TABLE T (C0 DECIMAL(10, 2))");
    execute_statement("INSERT INTO T VALUES (123.45)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_decimal_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto value = takatori::decimal::triple{1, 0, 12345, -2};
    auto value_plus_one = takatori::decimal::triple{
        value.sign(),
        value.coefficient_high(),
        value.coefficient_low() + 1,
        value.exponent()
    };

    // Note: T.C0 is DECIMAL(10,2) from table, but R.c1 is DECIMAL(*,*) from function
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
        std::tuple{meta::decimal_type(10, 2), meta::decimal_type()},
        std::forward_as_tuple(value, value),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
        std::tuple{meta::decimal_type(10, 2), meta::decimal_type()},
        std::forward_as_tuple(value, value_plus_one),
        {false, false})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, character_type) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(100))");
    execute_statement("INSERT INTO T VALUES ('this_is_a_test_string_with_more_than_thirty_characters')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_character_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    memory::page_pool pool{};
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&pool);
    auto text1 = accessor::text{resource.get(), "this_is_a_test_string_with_more_than_thirty_characters"s};
    auto text2 = accessor::text{resource.get(),"this_is_a_test_string_with_more_than_thirty_charactersX"s};

    // Note: T.C0 is CHARACTER VARYING(100), R.c1 is CHARACTER VARYING(*)
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{meta::character_type(true, 100), meta::character_type(true)},
        std::forward_as_tuple(text1, text1),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{meta::character_type(true, 100), meta::character_type(true)},
        std::forward_as_tuple(text1, text2),
        {false, false})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, date_type) {
    execute_statement("CREATE TABLE T (C0 DATE)");
    execute_statement("INSERT INTO T VALUES (DATE'2024-01-15')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_date_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto date = takatori::datetime::date{2024, 1, 15};
    EXPECT_EQ((create_nullable_record<kind::date, kind::date>(date, date)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::date, kind::date>(date, date + 1)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, time_of_day_type) {
    execute_statement("CREATE TABLE T (C0 TIME)");
    execute_statement("INSERT INTO T VALUES (TIME'12:34:56')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_time_of_day_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto time = takatori::datetime::time_of_day{12, 34, 56};
    EXPECT_EQ((create_nullable_record<kind::time_of_day, kind::time_of_day>(time, time)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::time_of_day, kind::time_of_day>(time, time + std::chrono::seconds{1})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, time_point_type) {
    execute_statement("CREATE TABLE T (C0 TIMESTAMP)");
    execute_statement("INSERT INTO T VALUES (TIMESTAMP'2024-01-15 12:34:56')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_time_point_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto timestamp = takatori::datetime::time_point{
        takatori::datetime::date{2024, 1, 15},
        takatori::datetime::time_of_day{12, 34, 56}
    };
    EXPECT_EQ((create_nullable_record<kind::time_point, kind::time_point>(timestamp, timestamp)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::time_point, kind::time_point>(timestamp, timestamp + std::chrono::seconds{1})), result[1]);
}

// boolean type not supported yet
TEST_F(sql_apply_type_matrix_test, DISABLED_boolean_type) {
    execute_statement("CREATE TABLE T (C0 BOOLEAN)");
    execute_statement("INSERT INTO T VALUES (TRUE)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_boolean_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::boolean, kind::boolean>(true, false)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::boolean, kind::boolean>(true, true)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, binary_type) {
    execute_statement("CREATE TABLE T (C0 VARBINARY(100))");
    execute_statement("INSERT INTO T VALUES (X'0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_binary_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    memory::page_pool pool{};
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&pool);
    std::string binary_data{"\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF", 32};
    std::string binary_data_extended = binary_data + "\xFF";
    auto bin1 = accessor::binary{resource.get(), binary_data.data(), binary_data.size()};
    auto bin2 = accessor::binary{resource.get(), binary_data_extended.data(), binary_data_extended.size()};

    // Note: T.C0 is OCTET VARYING(100), R.c1 is OCTET VARYING(*)
    EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(true, 100), meta::octet_type(true)},
        std::forward_as_tuple(bin1, bin1),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(true, 100), meta::octet_type(true)},
        std::forward_as_tuple(bin1, bin2),
        {false, false})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, multiple_types_in_single_query) {
    execute_statement("CREATE TABLE T (C0 INT, C1 VARCHAR(100), C2 DATE)");
    execute_statement("INSERT INTO T VALUES (42, 'hello', DATE'2024-01-01')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R1.c1, R2.c1, R3.c1 "
        "FROM T "
        "CROSS APPLY mock_table_func_int4_type(T.C0) AS R1(c1) "
        "CROSS APPLY mock_table_func_character_type(T.C1) AS R2(c1) "
        "CROSS APPLY mock_table_func_date_type(T.C2) AS R3(c1)",
        result
    );

    // 1 input row × 2 rows from R1 × 2 rows from R2 × 2 rows from R3 = 8 rows
    ASSERT_EQ(8, result.size());

    std::sort(result.begin(), result.end());

    auto date = takatori::datetime::date{2024, 1, 1};

    // check first and last rows
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::character, kind::date>(
        42, 42, accessor::text{"hello"s}, date)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::character, kind::date>(
        42, 43, accessor::text{"helloX"s}, date + 1)), result[7]);
}

TEST_F(sql_apply_type_matrix_test, outer_apply_with_various_types) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (100)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T OUTER APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    // same as CROSS APPLY when right side is not empty
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 101)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, null_values) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (NULL)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    // null value is passed as 0 to the function
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(
        std::tuple{0, 0}, {true, false})), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(
        std::tuple{0, 1}, {true, false})), result[1]);
}

} // namespace jogasaki::testing
