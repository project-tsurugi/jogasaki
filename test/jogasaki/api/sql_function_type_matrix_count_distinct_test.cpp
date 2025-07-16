/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "sql_function_type_matrix_test.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

using namespace std::string_view_literals;

//////////////////
// count distinct
//////////////////

TEST_F(sql_function_type_matrix_test, count_distinct_boolean) {
    db_impl()->configuration()->support_boolean(true);
    test_function_with_type<kind::int8>("count(distinct ", "BOOLEAN", "(true),(false),(true),(null)", 2);
}

// smallint, tinyint are not supported. Kept just for reference.

TEST_F(sql_function_type_matrix_test, DISABLED_count_distinct_tinyint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int8>("count(distinct ", "TINYINT", "(1),(2),(1),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, DISABLED_count_distinct_smallint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int8>("count(distinct ", "SMALLINT", "(1),(2),(1),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_int) {
    test_function_with_type<kind::int8>("count(distinct ", "INT", "(1),(2),(1),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_bigint) {
    test_function_with_type<kind::int8>("count(distinct ", "BIGINT", "(1),(2),(1),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_real) {
    test_function_with_type<kind::int8>("count(distinct ", "real", "(1.0e0),(2.0e0),(1.0e0),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_double) {
    test_function_with_type<kind::int8>("count(distinct ", "double", "(1.0e0),(2.0e0),(1.0e0),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_decimal) {
    test_function_with_type<kind::int8>("count(distinct ", "decimal", "(1.0),(2.0),(1.0),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_varchar) {
    test_function_with_type<kind::int8>("count(distinct ", "VARCHAR", "('AAA'),('BBB'),('AAA'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_char) {
    test_function_with_type<kind::int8>("count(distinct ", "CHAR(3)", "('AAA'),('BBB'),('AAA'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_char_20) {
    test_function_with_type<kind::int8>("count(distinct ", "CHAR(20)", "('AAA'),('BBB'),('AAA'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_varbinary) {
    test_function_with_type<kind::int8>("count(distinct ", "VARBINARY(3)", "('010101'),('020202'),('010101'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_binary) {
    test_function_with_type<kind::int8>("count(distinct ", "BINARY(3)", "('010101'),('020202'),('010101'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_binary_20) {
    test_function_with_type<kind::int8>("count(distinct ", "BINARY(20)", "('010101'),('020202'),('010101'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_date) {
    test_function_with_type<kind::int8>("count(distinct ", "DATE", "(DATE'2000-01-01'),(DATE'2000-01-02'),(DATE'2000-01-01'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_time) {
    test_function_with_type<kind::int8>("count(distinct ", "TIME", "(TIME'00:00:01'),(TIME'00:00:02'),(TIME'00:00:01'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_timestamp) {
    test_function_with_type<kind::int8>("count(distinct ", "TIMESTAMP", "(TIMESTAMP'2000-01-01 00:00:01'),(TIMESTAMP'2000-01-01 00:00:02'),(TIMESTAMP'2000-01-01 00:00:01'),(null)", 2);
}

TEST_F(sql_function_type_matrix_test, count_distinct_timestamptz) {
    test_function_with_type<kind::int8>("count(distinct ", "TIMESTAMP WITH TIME ZONE", "(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:01+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:02+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:01+09:00'),(null)", 2);
}

}  // namespace jogasaki::testing
