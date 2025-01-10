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

// smallint, tinyint are not supported. Kept just for reference.

TEST_F(sql_function_type_matrix_test, DISABLED_max_tinyint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("max(", "TINYINT", "(1),(2),(3),(null)", 3);
}

TEST_F(sql_function_type_matrix_test, DISABLED_max_smallint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("max(", "SMALLINT", "(1),(2),(3),(null)", 3);
}

TEST_F(sql_function_type_matrix_test, max_int) {
    test_function_with_type<kind::int4>("max(", "INT", "(1),(2),(3),(null)", 3);
}

TEST_F(sql_function_type_matrix_test, max_bigint) {
    test_function_with_type<kind::int8>("max(", "BIGINT", "(1),(2),(3),(null)", 3);
}

TEST_F(sql_function_type_matrix_test, max_real) {
    test_function_with_type<kind::float4>("max(", "real", "(1.0e0),(2.0e0),(3.0e0),(null)", 3.0);
}

TEST_F(sql_function_type_matrix_test, max_double) {
    test_function_with_type<kind::float8>("max(", "double", "(1.0e0),(2.0e0),(3.0e0),(null)", 3);
}

TEST_F(sql_function_type_matrix_test, max_decimal) {
    test_function_with_type<kind::decimal>("max(", "decimal", "(1.0),(2.0),(3.0),(null)", 3, meta::decimal_type());
}

TEST_F(sql_function_type_matrix_test, max_varchar) {
    test_function_with_type<kind::character>("max(", "VARCHAR", "('AAA'),('BBB'),('CCC'),(null)", accessor::text{"CCC"});
}

TEST_F(sql_function_type_matrix_test, max_char) {
    test_function_with_type<kind::character>("max(", "CHAR(3)", "('AAA'),('BBB'),('CCC'),(null)", accessor::text{"CCC"});
}

TEST_F(sql_function_type_matrix_test, max_char_20) {
    test_function_with_type<kind::character>("max(", "CHAR(20)", "('AAA'),('BBB'),('CCC'),(null)", accessor::text{"CCC                 "});
}

TEST_F(sql_function_type_matrix_test, max_varbinary) {
    test_function_with_type<kind::octet>("max(", "VARBINARY(3)", "('010101'),('020202'),('030303'),(null)", accessor::binary{"\x03\x03\x03"});
}

TEST_F(sql_function_type_matrix_test, max_binary) {
    test_function_with_type<kind::octet>("max(", "BINARY(3)", "('010101'),('020202'),('030303'),(null)", accessor::binary{"\x03\x03\x03"});
}

TEST_F(sql_function_type_matrix_test, max_binary_20) {
    test_function_with_type<kind::octet>("max(", "BINARY(20)", "('010101'),('020202'),('030303'),(null)", accessor::binary{"\x03\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"});
}

TEST_F(sql_function_type_matrix_test, max_date) {
    test_function_with_type<kind::date>("max(", "DATE", "(DATE'2000-01-01'),(DATE'2000-01-02'),(DATE'2000-01-03'),(null)", date{2000, 1, 3});
}

TEST_F(sql_function_type_matrix_test, max_time) {
    test_function_with_type<kind::time_of_day>("max(", "TIME", "(TIME'00:00:01'),(TIME'00:00:02'),(TIME'00:00:03'),(null)", time_of_day{0, 0, 3});
}

TEST_F(sql_function_type_matrix_test, max_timestamp) {
    test_function_with_type<kind::time_point>(
        "max(",
        "TIMESTAMP",
        "(TIMESTAMP'2000-01-01 00:00:01'),(TIMESTAMP'2000-01-01 00:00:02'),(TIMESTAMP'2000-01-01 00:00:03'),(null)",
        time_point{date{2000, 1, 1}, time_of_day{0, 0, 3}}
    );
}

TEST_F(sql_function_type_matrix_test, max_timestamptz) {
    test_function_with_type<kind::time_point>(
        "max(",
        "TIMESTAMP WITH TIME ZONE",
        "(TIMESTAMP WITH TIME ZONE'2000-01-01 09:00:01+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 "
        "09:00:02+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 09:00:03+09:00'),(null)",
        time_point{date{2000, 1, 1}, time_of_day{0, 0, 3}},
        meta::time_point_type(true)
    );
}

}  // namespace jogasaki::testing
