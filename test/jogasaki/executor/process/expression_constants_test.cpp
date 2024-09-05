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
#pragma STDC FENV_ACCESS ON
#include <cfenv>
#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/executor/expr/details/constants.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::expr {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;

using namespace testing;

using kind = meta::field_type_kind;

class expression_constants_test : public test_root {
public:

};

template <kind Float>
typename meta::field_type_traits<Float>::runtime_type round_to_float(double in);

template <>
typename meta::field_type_traits<kind::float4>::runtime_type round_to_float<kind::float4>(double in) {
    return std::rintf(in);
}

template <>
typename meta::field_type_traits<kind::float8>::runtime_type round_to_float<kind::float8>(double in) {
    return std::rint(in);
}

// actually this can be the definition of max_integral_float_convertible_to_int_source, but rounding functions are not
// constexpr, so we hardcode the constants and test them here.
template <kind Int, kind Float>
void test_max_integral_float_convertible_to_int() {
    auto mode = std::fegetround();
    std::fesetround(FE_TOWARDZERO);
    auto f = round_to_float<Float>(std::numeric_limits<typename meta::field_type_traits<Int>::value_range>::max());
    std::fesetround(mode);
    auto exp = static_cast<typename meta::field_type_traits<Int>::runtime_type>(f);
    EXPECT_EQ(
        exp,
        (details::max_integral_float_convertible_to_int_source<Int, Float>)
    );
};

TEST_F(expression_constants_test, max_integral_float4_convertible_to_int1) {
    test_max_integral_float_convertible_to_int<kind::int1, kind::float4>();
}

TEST_F(expression_constants_test, max_integral_float4_convertible_to_int2) {
    test_max_integral_float_convertible_to_int<kind::int2, kind::float4>();
}

TEST_F(expression_constants_test, max_integral_float4_convertible_to_int4) {
    test_max_integral_float_convertible_to_int<kind::int4, kind::float4>();
}

TEST_F(expression_constants_test, max_integral_float4_convertible_to_int8) {
    test_max_integral_float_convertible_to_int<kind::int8, kind::float4>();
}

TEST_F(expression_constants_test, max_integral_float8_convertible_to_int1) {
    test_max_integral_float_convertible_to_int<kind::int1, kind::float8>();
}

TEST_F(expression_constants_test, max_integral_float8_convertible_to_int2) {
    test_max_integral_float_convertible_to_int<kind::int2, kind::float8>();
}

TEST_F(expression_constants_test, max_integral_float8_convertible_to_int4) {
    test_max_integral_float_convertible_to_int<kind::int4, kind::float8>();
}

TEST_F(expression_constants_test, max_integral_float8_convertible_to_int8) {
    test_max_integral_float_convertible_to_int<kind::int8, kind::float8>();
}
}

