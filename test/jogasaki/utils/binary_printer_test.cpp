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
#include <jogasaki/utils/binary_printer.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

class binary_printer_test : public ::testing::Test {};

TEST_F(binary_printer_test, simple) {
    std::string abc("ABC");
    std::stringstream ss{};
    ss << binary_printer{abc.data(), abc.size()};
    EXPECT_EQ("41-42-43", ss.str());
}

TEST_F(binary_printer_test, small_values) {
    std::string str("\x00\x01\x02\xFF", 4);
    std::stringstream ss{};
    ss << binary_printer{str.data(), str.size()};
    EXPECT_EQ("00-01-02-ff", ss.str());
}

TEST_F(binary_printer_test, format_unchanged) {
    std::stringstream ss1{};
    ss1 << 65;
    EXPECT_EQ("65", ss1.str());

    std::string str("A");
    std::stringstream ss2{};
    ss2 << binary_printer{str.data(), str.size()};
    ss2 << 65;
    EXPECT_EQ("4165", ss2.str());
}

}

