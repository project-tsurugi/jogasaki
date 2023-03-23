/*
 * Copyright 2018-2020 tsurugi project.
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
#include <jogasaki/utils/convert_control_characters.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

using namespace std::string_literals;

class convert_control_characters_test : public ::testing::Test {};

TEST_F(convert_control_characters_test, simple) {
    EXPECT_EQ("", convert_control_characters(""));
    EXPECT_EQ("ABC", convert_control_characters("ABC"));
    EXPECT_EQ("111!222", convert_control_characters("111\n222"));
    EXPECT_EQ("111 222 333", convert_control_characters("111 222 333"));
}

TEST_F(convert_control_characters_test, min_max) {
    EXPECT_EQ("!!!!", convert_control_characters("\x00\x01\x1E\x1F"s));
    EXPECT_EQ("!A!B!C!", convert_control_characters(std::string{"\x00" "A" "\x01" "B" "\x1E" "C" "\x1F", 7}));
    EXPECT_EQ("\x7E\x7F", convert_control_characters("\x7E\x7F\x80\x81"s));
}
}

