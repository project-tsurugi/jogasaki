/*
 * Copyright 2018-2023 Project Tsurugi.
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
// count
//////////////////

TEST_F(sql_function_type_matrix_test, count_boolean) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int8>("count(", "BOOLEAN", "(true),(false),(true)", 3);
}

// smallint, tinyint are not supported. Kept just for reference.

TEST_F(sql_function_type_matrix_test, DISABLED_count_tinyint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int8>("count(", "TINYINT", "(1),(2),(3)", 3);
}

TEST_F(sql_function_type_matrix_test, DISABLED_count_smallint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int8>("count(", "SMALLINT", "(1),(2),(3)", 3);
}

TEST_F(sql_function_type_matrix_test, count_int) {
    test_function_with_type<kind::int8>("count(", "INT", "(1),(2),(3)", 3);
}

TEST_F(sql_function_type_matrix_test, count_bigint) {
    test_function_with_type<kind::int8>("count(", "BIGINT", "(1),(2),(3)", 3);
}

TEST_F(sql_function_type_matrix_test, count_real) {
    test_function_with_type<kind::int8>("count(", "real", "(1.0e0),(2.0e0),(3.0e0)", 3);
}

TEST_F(sql_function_type_matrix_test, count_double) {
    test_function_with_type<kind::int8>("count(", "double", "(1.0e0),(2.0e0),(3.0e0)", 3);
}

TEST_F(sql_function_type_matrix_test, count_varchar) {
    test_function_with_type<kind::int8>("count(", "VARCHAR", "('AAA'),('BBB'),('CCC')", 3);
}

TEST_F(sql_function_type_matrix_test, count_char) {
    test_function_with_type<kind::int8>("count(", "CHAR(3)", "('AAA'),('BBB'),('CCC')", 3);
}

TEST_F(sql_function_type_matrix_test, count_varbinary) {
    db_impl()->configuration()->support_octet(true);
    test_function_with_type<kind::int8>("count(", "VARBINARY", "('010101'),('020202'),('030303')", 3);
}

TEST_F(sql_function_type_matrix_test, count_binary) {
    db_impl()->configuration()->support_octet(true);
    test_function_with_type<kind::int8>("count(", "BINARY(3)", "('010101'),('020202'),('030303')", 3);
}

TEST_F(sql_function_type_matrix_test, count_date) {
    test_function_with_type<kind::int8>("count(", "DATE", "(DATE'2000-01-01'),(DATE'2000-01-02'),(DATE'2000-01-03')", 3);
}

TEST_F(sql_function_type_matrix_test, count_time) {
    test_function_with_type<kind::int8>("count(", "TIME", "(TIME'00:00:01'),(TIME'00:00:02'),(TIME'00:00:03')", 3);
}

TEST_F(sql_function_type_matrix_test, count_timestamp) {
    test_function_with_type<kind::int8>("count(", "TIMESTAMP", "(TIMESTAMP'2000-01-01 00:00:01'),(TIMESTAMP'2000-01-01 00:00:02'),(TIMESTAMP'2000-01-01 00:00:03')", 3);
}

TEST_F(sql_function_type_matrix_test, count_timestamptz) {
    test_function_with_type<kind::int8>("count(", "TIMESTAMP WITH TIME ZONE", "(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:01+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:02+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:03+09:00')", 3);
}

}  // namespace jogasaki::testing
