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
#include <jogasaki/utils/decimal.h>

#include <gtest/gtest.h>

namespace jogasaki::testing {

class decimal_utils_test : public ::testing::Test {};

TEST_F(decimal_utils_test, zero) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 0, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000000UL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 1, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000001UL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, two) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 2, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000002UL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, minus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 1, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, minus_two) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 2, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFEUL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_1_byte) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 0x7FUL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x000000000000007FUL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_1_byte_negated) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x7FUL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFF81UL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_1_byte_plus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 0x80UL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000080UL, lo);
    EXPECT_EQ(2, sz);
}

TEST_F(decimal_utils_test, negative_min_representable_by_1_byte) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x80UL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFF80UL, lo);
    EXPECT_EQ(1, sz);
}

TEST_F(decimal_utils_test, negative_min_representable_by_1_byte_minus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x81UL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFF7FUL, lo);
    EXPECT_EQ(2, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_8_bytes) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 0x7FFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x7FFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(8, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_8_bytes_negated) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x7FFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0x8000000000000001UL, lo);
    EXPECT_EQ(8, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_8_bytes_plus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0, 0x8000000000000000UL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x8000000000000000UL, lo);
    EXPECT_EQ(9, sz);
}

TEST_F(decimal_utils_test, negative_min_represented_by_8_bytes) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x8000000000000000UL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0x8000000000000000UL, lo);
    EXPECT_EQ(8, sz);
}

TEST_F(decimal_utils_test, negative_max_represented_by_8_bytes_minus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0, 0x8000000000000001UL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0x7FFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(9, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_16_bytes) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0x7FFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(16, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_16_bytes_negated) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0x8000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000001UL, lo);
    EXPECT_EQ(16, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_16_bytes_plus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0x8000000000000000UL, 0x0000000000000000UL, 0});
    EXPECT_EQ(0x8000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000000UL, lo);
    EXPECT_EQ(17, sz);
}

TEST_F(decimal_utils_test, negative_min_represented_by_16_bytes) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0x8000000000000000UL, 0x0000000000000000UL, 0});
    EXPECT_EQ(0x8000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000000UL, lo);
    EXPECT_EQ(16, sz);
}

TEST_F(decimal_utils_test, negative_max_represented_by_16_bytes_minus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0x8000000000000000UL, 0x0000000000000001UL, 0});
    EXPECT_EQ(0x7FFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(17, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_triple) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, lo);
    EXPECT_EQ(17, sz);
}

TEST_F(decimal_utils_test, positive_max_representable_by_triple_minus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0});
    EXPECT_EQ(0xFFFFFFFFFFFFFFFFUL, hi);
    EXPECT_EQ(0xFFFFFFFFFFFFFFFEUL, lo);
    EXPECT_EQ(17, sz);
}

TEST_F(decimal_utils_test, negative_min_representable_by_triple) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000001UL, lo);
    EXPECT_EQ(17, sz);
}

TEST_F(decimal_utils_test, negative_min_representable_by_triple_plus_one) {
    auto [hi, lo, sz] = utils::make_signed_coefficient_full({-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFEUL, 0});
    EXPECT_EQ(0x0000000000000000UL, hi);
    EXPECT_EQ(0x0000000000000002UL, lo);
    EXPECT_EQ(17, sz);
}

}  // namespace jogasaki::testing
