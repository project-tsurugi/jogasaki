/*
 * Copyright 2018-2024 Project Tsurugi.
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

// the largest integer that is representable in Float and fits in the value range of Int.
// Float keeps only the upper `digits` bits of an integer, so the integer is obtained by clearing the lower
// (value bits of Int - digits of Float) bits of the Int max.
// We compute the expected value at compile time instead of rounding at runtime, because runtime rounding under
// fesetround() is not reliable on optimized builds (the compiler folds it assuming the default rounding mode).
template <kind Int, kind Float>
constexpr typename meta::field_type_traits<Int>::value_range expected_max_integral_float_convertible_to_int() {
    using value_range = typename meta::field_type_traits<Int>::value_range;
    using float_type = typename meta::field_type_traits<Float>::runtime_type;
    static_assert(std::numeric_limits<float_type>::is_iec559);
    constexpr auto int_digits = std::numeric_limits<value_range>::digits;
    constexpr auto float_digits = std::numeric_limits<float_type>::digits;
    constexpr auto max = std::numeric_limits<value_range>::max();
    if constexpr (float_digits >= int_digits) {
        return max;
    } else {
        return max - ((value_range{1} << (int_digits - float_digits)) - 1);
    }
}

template <kind Int, kind Float>
void test_max_integral_float_convertible_to_int() {
    constexpr auto exp = expected_max_integral_float_convertible_to_int<Int, Float>();
    constexpr auto c = details::max_integral_float_convertible_to_int_source<Int, Float>;
    static_assert(exp == c);
    // the constant survives the round-trip through Float
    static_assert(static_cast<decltype(c)>(static_cast<typename meta::field_type_traits<Float>::runtime_type>(c)) == c);
    EXPECT_EQ(exp, c);
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

