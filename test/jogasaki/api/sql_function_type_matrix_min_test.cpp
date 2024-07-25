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

// smallint, tinyint are not supported. Kept just for reference.

TEST_F(sql_function_type_matrix_test, DISABLED_min_tinyint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("min(", "TINYINT", "(1),(2),(3)", 1);
}

TEST_F(sql_function_type_matrix_test, DISABLED_min_smallint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("min(", "SMALLINT", "(1),(2),(3)", 1);
}

TEST_F(sql_function_type_matrix_test, min_int) {
    test_function_with_type<kind::int4>("min(", "INT", "(1),(2),(3)", 1);
}

TEST_F(sql_function_type_matrix_test, min_bigint) {
    test_function_with_type<kind::int8>("min(", "BIGINT", "(1),(2),(3)", 1);
}

TEST_F(sql_function_type_matrix_test, min_real) {
    test_function_with_type<kind::float4>("min(", "real", "(1.0e0),(2.0e0),(3.0e0)", 1.0);
}

TEST_F(sql_function_type_matrix_test, min_double) {
    test_function_with_type<kind::float8>("min(", "double", "(1.0e0),(2.0e0),(3.0e0)", 1);
}

TEST_F(sql_function_type_matrix_test, min_decimal) {
    test_function_with_type<kind::decimal>("min(", "decimal", "(1.0),(2.0),(3.0)", 1, meta::decimal_type());
}

TEST_F(sql_function_type_matrix_test, min_varchar) {
    test_function_with_type<kind::character>("min(", "VARCHAR", "('AAA'),('BBB'),('CCC')", accessor::text{"AAA"});
}

TEST_F(sql_function_type_matrix_test, min_char) {
    test_function_with_type<kind::character>("min(", "CHAR(3)", "('AAA'),('BBB'),('CCC')", accessor::text{"AAA"});
}

TEST_F(sql_function_type_matrix_test, min_varbinary) {
    db_impl()->configuration()->support_octet(true);
    test_function_with_type<kind::octet>("min(", "VARBINARY(3)", "('010101'),('020202'),('030303')", accessor::binary{"\x01\x01\x01"});
}

TEST_F(sql_function_type_matrix_test, min_binary) {
    db_impl()->configuration()->support_octet(true);
    test_function_with_type<kind::octet>("min(", "BINARY(3)", "('010101'),('020202'),('030303')", accessor::binary{"\x01\x01\x01"});
}

TEST_F(sql_function_type_matrix_test, min_date) {
    test_function_with_type<kind::date>("min(", "DATE", "(DATE'2000-01-01'),(DATE'2000-01-02'),(DATE'2000-01-03')", date{2000, 1, 1});
}

TEST_F(sql_function_type_matrix_test, min_time) {
    test_function_with_type<kind::time_of_day>("min(", "TIME", "(TIME'00:00:01'),(TIME'00:00:02'),(TIME'00:00:03')", time_of_day{0, 0, 1});
}

TEST_F(sql_function_type_matrix_test, min_timestamp) {
    test_function_with_type<kind::time_point>(
        "min(",
        "TIMESTAMP",
        "(TIMESTAMP'2000-01-01 00:00:01'),(TIMESTAMP'2000-01-01 00:00:02'),(TIMESTAMP'2000-01-01 00:00:03')",
        time_point{date{2000, 1, 1}, time_of_day{0, 0, 1}}
    );
}

TEST_F(sql_function_type_matrix_test, min_timestamptz) {
    test_function_with_type<kind::time_point>(
        "min(",
        "TIMESTAMP WITH TIME ZONE",
        "(TIMESTAMP WITH TIME ZONE'2000-01-01 09:00:01+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 "
        "09:00:02+09:00'),(TIMESTAMP WITH TIME ZONE'2000-01-01 09:00:03+09:00')",
        time_point{date{2000, 1, 1}, time_of_day{0, 0, 1}},
        meta::time_point_type(true)
    );
}

}  // namespace jogasaki::testing
