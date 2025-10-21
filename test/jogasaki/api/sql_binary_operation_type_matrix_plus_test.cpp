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
#include "sql_binary_operation_type_matrix_test.h"

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

// int4 op ...

TEST_F(sql_binary_operation_type_matrix_test, add_int4_int4) {
    test_binary_operation_with_type<kind::int4>("c0+c1", "INT", "INT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int4_int8) {
    test_binary_operation_with_type<kind::int8>("c0+c1", "INT", "BIGINT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int4_dec5) {
    test_binary_operation_with_type<kind::decimal>("c0+c1", "INT", "DECIMAL(5)", "(3,2)", 5, decimal_type(std::nullopt, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_int4_float4) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "INT", "REAL", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int4_float8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "INT", "DOUBLE", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int4_unknown) {
    test_binary_operation_with_type<kind::int4>("c0+null", "INT", "INT", "(3,2)", {});  // c1 is created as INT, but not used
}

// int8 op ...

TEST_F(sql_binary_operation_type_matrix_test, add_int8_int4) {
    test_binary_operation_with_type<kind::int8>("c0+c1", "BIGINT", "INT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int8_int8) {
    test_binary_operation_with_type<kind::int8>("c0+c1", "BIGINT", "BIGINT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int8_dec5) {
    test_binary_operation_with_type<kind::decimal>("c0+c1", "BIGINT", "DECIMAL(5)", "(3,2)", 5, decimal_type(std::nullopt, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_int8_float4) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "BIGINT", "REAL", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int8_float8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "BIGINT", "DOUBLE", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_int8_unknown) {
    test_binary_operation_with_type<kind::int8>("c0+null", "BIGINT", "INT", "(3,2)", {});  // c1 is created as INT, but not used
}

// decimal op ...

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_int4) {
    // DECIMAL(5) v.s. INT = DECIMAL(5) v.s. DECIMAL(10) -> DECIMAL(*, 0)
    test_binary_operation_with_type<kind::decimal>("c0+c1", "DECIMAL(5)", "INT", "(3,2)", 5, decimal_type(std::nullopt, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_int8) {
    // DECIMAL(5) v.s. BIGINT = DECIMAL(5) v.s. DECIMAL(19) -> DECIMAL(*, 0)
    test_binary_operation_with_type<kind::decimal>("c0+c1", "DECIMAL(5)", "BIGINT", "(3,2)", 5, decimal_type(std::nullopt, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_dec5) {
    // DECIMAL(5) v.s. DECIMAL(5) -> DECIMAL(*, 0)
    test_binary_operation_with_type<kind::decimal>("c0+c1", "DECIMAL(5)", "DECIMAL(5)", "(3,2)", 5, decimal_type(std::nullopt, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_float4) {
    // DECIMAL(5) v.s. REAL -> DOUBLE
    test_binary_operation_with_type<kind::float8>("c0+c1", "DECIMAL(5)", "REAL", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_float8) {
    // DECIMAL(5) v.s. DOUBLE -> DOUBLE
    test_binary_operation_with_type<kind::float8>("c0+c1", "DECIMAL(5)", "DOUBLE", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_decimal_unknown) {
    // DECIMAL(5) v.s. UNKNOWN -> DECIMAL(5) : type is preserved as is
    test_binary_operation_with_type<kind::decimal>("c0+null", "DECIMAL(5)", "INT", "(3,2)", {}, decimal_type(5, 0));  // c1 is created as INT, but not used
}

// float4 op ...

TEST_F(sql_binary_operation_type_matrix_test, add_float4_int4) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "REAL", "INT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float4_int8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "REAL", "BIGINT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float4_dec5) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "REAL", "DECIMAL(5)", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float4_float4) {
    test_binary_operation_with_type<kind::float4>("c0+c1", "REAL", "REAL", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float4_float8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "REAL", "DOUBLE", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float4_unknown) {
    test_binary_operation_with_type<kind::float4>("c0+null", "REAL", "INT", "(3,2)", {});  // c1 is created as INT, but not used
}

// float8 op ...

TEST_F(sql_binary_operation_type_matrix_test, add_float8_int4) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "DOUBLE", "INT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float8_int8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "DOUBLE", "BIGINT", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float8_dec5) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "DOUBLE", "DECIMAL(5)", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float8_float4) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "DOUBLE", "REAL", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float8_float8) {
    test_binary_operation_with_type<kind::float8>("c0+c1", "DOUBLE", "DOUBLE", "(3,2)", 5);
}

TEST_F(sql_binary_operation_type_matrix_test, add_float8_unknown) {
    test_binary_operation_with_type<kind::float8>("c0+null", "DOUBLE", "INT", "(3,2)", {});  // c1 is created as INT, but not used
}

// unknown op ...

TEST_F(sql_binary_operation_type_matrix_test, add_unknown_int4) {
    test_binary_operation_with_type<kind::int4>("null+c1", "INT", "INT", "(3,2)", {});
}

TEST_F(sql_binary_operation_type_matrix_test, add_unknown_int8) {
    test_binary_operation_with_type<kind::int8>("null+c1", "INT", "BIGINT", "(3,2)", {});
}

TEST_F(sql_binary_operation_type_matrix_test, add_unknown_dec5) {
    test_binary_operation_with_type<kind::decimal>("null+c1", "INT", "DECIMAL(5)", "(3,2)", {}, decimal_type(5, 0));
}

TEST_F(sql_binary_operation_type_matrix_test, add_unknown_float4) {
    test_binary_operation_with_type<kind::float4>("null+c1", "INT", "REAL", "(3,2)", {});
}

TEST_F(sql_binary_operation_type_matrix_test, add_unknown_float8) {
    test_binary_operation_with_type<kind::float8>("null+c1", "INT", "DOUBLE", "(3,2)", {});
}

// binary operations with both left/right are unknown type result in compile error, so this case cannot be executed
// TEST_F(sql_binary_operation_type_matrix_test, add_unknown_unknown) {
//     test_binary_operation_with_type<kind::int4>("null+null", "INT", "INT", "(3,2)", {});  // c1 is created as INT, but not used
// }

}  // namespace jogasaki::testing
