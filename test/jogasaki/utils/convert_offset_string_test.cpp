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
#include <jogasaki/utils/convert_offset_string.h>

#include <iostream>
#include <string>
#include <gtest/gtest.h>

namespace jogasaki::utils {

class convert_offset_string_test : public ::testing::Test {};


std::int32_t convert(std::string_view in) {
    std::int32_t offset_min{};
    if(! convert_offset_string(in, offset_min)) {
        ADD_FAILURE() << "failed with " << in;
    }
    return offset_min;
}

TEST_F(convert_offset_string_test, basic) {
    EXPECT_EQ(0, convert("+00:00"));
    EXPECT_EQ(60, convert("+01:00"));
    EXPECT_EQ(-60, convert("-01:00"));
    EXPECT_EQ(720, convert("+12:00"));
    EXPECT_EQ(-720, convert("-12:00"));
    EXPECT_EQ(570, convert("+09:30"));
    EXPECT_EQ(0, convert("Z"));
    EXPECT_EQ(0, convert(""));  // empty string means UTC
    EXPECT_EQ(540, convert("+09"));
    EXPECT_EQ(-540, convert("-09"));

    EXPECT_EQ(0, convert("-00:00"));
    EXPECT_EQ(0, convert("-00"));
    EXPECT_EQ(0, convert("-0000"));
    EXPECT_EQ(540, convert("+9"));
    EXPECT_EQ(-540, convert("-9"));
}

TEST_F(convert_offset_string_test, errornous) {
    std::int32_t offset_min{};
    EXPECT_TRUE(! convert_offset_string(" ", offset_min));
    EXPECT_TRUE(! convert_offset_string(" +09:00", offset_min));
    EXPECT_TRUE(! convert_offset_string("09:00", offset_min));
    EXPECT_TRUE(! convert_offset_string("+0900", offset_min));
    EXPECT_TRUE(! convert_offset_string("-0900", offset_min));
    EXPECT_TRUE(! convert_offset_string("z", offset_min));
}
}  // namespace jogasaki::utils
