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
#include <cstdint>
#include <memory>
#include <ostream>
#include <gtest/gtest.h>

#include <jogasaki/utils/hex.h>

namespace jogasaki::utils {

class hex_printer_test : public ::testing::Test {};

TEST_F(hex_printer_test, simple) {
    std::stringstream ss{};
    std::int16_t i16{1};
    std::int32_t i32{1};
    std::int64_t i64{1};
    ss << hex(i16) << ", " << hex(i32) << ", " << hex(i64) << ", " << i16;
    EXPECT_EQ("0001, 00000001, 0000000000000001, 1", ss.str());
}

TEST_F(hex_printer_test, verify_lowercase) {
    std::stringstream ss{};
    ss << hex(10) << ", " << hex(11) << ", " << hex(12);
    EXPECT_EQ("0000000a, 0000000b, 0000000c", ss.str());
}

TEST_F(hex_printer_test, max) {
    std::stringstream ss{};
    ss << hex(-1) << ", " << hex(-1UL);
    EXPECT_EQ("ffffffff, ffffffffffffffff", ss.str());
}
}

