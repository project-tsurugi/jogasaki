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
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/executor/equal_to.h>
#include <jogasaki/executor/value_hash.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor {

class value_hash_test : public test_root {};

template <class T>
void expect_same_hash(T x, T y) {
    ASSERT_TRUE(equal_to(x, y));
    EXPECT_EQ(value_hash<T>{}(x), value_hash<T>{}(y));
}

TEST_F(value_hash_test, float8_nan) {
    // NaNs with different bit patterns
    double nan0 = std::numeric_limits<double>::quiet_NaN();
    double nan1 = -std::numeric_limits<double>::quiet_NaN();
    ASSERT_TRUE(std::signbit(nan1));
    expect_same_hash(nan0, nan1);
}

TEST_F(value_hash_test, float4_nan) {
    float nan0 = std::numeric_limits<float>::quiet_NaN();
    float nan1 = -std::numeric_limits<float>::quiet_NaN();
    ASSERT_TRUE(std::signbit(nan1));
    expect_same_hash(nan0, nan1);
}

TEST_F(value_hash_test, float8_nan_different_payloads) {
    // IEEE 754 quiet NaNs with the same sign and different payloads.
    std::uint64_t bits0 = 0x7ff8000000000001ULL;
    std::uint64_t bits1 = 0x7ff8000000000002ULL;
    double nan0{};
    double nan1{};
    static_assert(sizeof(nan0) == sizeof(bits0));
    std::memcpy(&nan0, &bits0, sizeof(nan0));
    std::memcpy(&nan1, &bits1, sizeof(nan1));
    ASSERT_TRUE(std::isnan(nan0));
    ASSERT_TRUE(std::isnan(nan1));
    expect_same_hash(nan0, nan1);
    expect_same_hash(nan0, std::numeric_limits<double>::quiet_NaN());
}

TEST_F(value_hash_test, float4_nan_different_payloads) {
    std::uint32_t bits0 = 0x7fc00001U;
    std::uint32_t bits1 = 0x7fc00002U;
    float nan0{};
    float nan1{};
    static_assert(sizeof(nan0) == sizeof(bits0));
    std::memcpy(&nan0, &bits0, sizeof(nan0));
    std::memcpy(&nan1, &bits1, sizeof(nan1));
    ASSERT_TRUE(std::isnan(nan0));
    ASSERT_TRUE(std::isnan(nan1));
    expect_same_hash(nan0, nan1);
    expect_same_hash(nan0, std::numeric_limits<float>::quiet_NaN());
}

TEST_F(value_hash_test, float_zero) {
    expect_same_hash(0.0, -0.0);
    expect_same_hash(0.0F, -0.0F);
}

TEST_F(value_hash_test, decimal_different_representations) {
    using triple = takatori::decimal::triple;
    expect_same_hash(triple{1, 0, 1, 0}, triple{1, 0, 10, -1});
    expect_same_hash(triple{1, 0, 1, 0}, triple{1, 0, 100, -2});
    expect_same_hash(triple{0, 0, 0, 0}, triple{0, 0, 0, -3});
}

}  // namespace jogasaki::executor
