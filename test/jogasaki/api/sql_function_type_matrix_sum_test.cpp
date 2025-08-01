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

// smallint, tinyint are not supported. Kept just for reference.

TEST_F(sql_function_type_matrix_test, DISABLED_sum_tinyint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("sum(", "TINYINT", "(1),(2),(3),(null)", 6);
}

TEST_F(sql_function_type_matrix_test, DISABLED_sum_smallint) {
    db_impl()->configuration()->support_smallint(true);
    test_function_with_type<kind::int4>("sum(", "SMALLINT", "(1),(2),(3),(null)", 6);
}

TEST_F(sql_function_type_matrix_test, sum_int) {
    test_function_with_type<kind::int4>("sum(", "INT", "(1),(2),(3),(null)", 6);
}

TEST_F(sql_function_type_matrix_test, sum_bigint) {
    test_function_with_type<kind::int8>("sum(", "BIGINT", "(1),(2),(3),(null)", 6);
}

TEST_F(sql_function_type_matrix_test, sum_real) {
    test_function_with_type<kind::float4>("sum(", "real", "(1.0e0),(2.0e0),(3.0e0),(null)", 6.0);
}

TEST_F(sql_function_type_matrix_test, sum_double) {
    test_function_with_type<kind::float8>("sum(", "double", "(1.0e0),(2.0e0),(3.0e0),(null)", 6);
}

TEST_F(sql_function_type_matrix_test, sum_decimal) {
    test_function_with_type<kind::decimal>("sum(", "decimal", "(1.0),(2.0),(3.0),(null)", 6, meta::decimal_type());
}

}  // namespace jogasaki::testing
