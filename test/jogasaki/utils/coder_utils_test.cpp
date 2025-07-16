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
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/utils/coder.h>


namespace jogasaki::testing {

class coder_utils_test : public ::testing::Test {};

TEST_F(coder_utils_test, bytes_for_digits) {
    // bytes : range : digits representable
    // 1     : [-128, 127] : 2
    // 2     : [-32768, 32767] : 4
    // 3     : [-8388608, 8388607] : 6
    // 4     : [-2147483648, 2147483647] : 9
    // 5     : [-549755813888, 549755813887] : 11
    // 6     : [-140737488355328, 140737488355327] : 14
    // 7     : [-36028797018963968, 36028797018963967] : 16
    // 8     : [-9223372036854775808, 9223372036854775807] : 18

    EXPECT_EQ(0, utils::bytes_required_for_digits(0));
    EXPECT_EQ(1, utils::bytes_required_for_digits(1));
    EXPECT_EQ(1, utils::bytes_required_for_digits(2));
    EXPECT_EQ(2, utils::bytes_required_for_digits(3));
    EXPECT_EQ(2, utils::bytes_required_for_digits(4));
    EXPECT_EQ(3, utils::bytes_required_for_digits(5));
    EXPECT_EQ(3, utils::bytes_required_for_digits(6));
    EXPECT_EQ(4, utils::bytes_required_for_digits(7));
    EXPECT_EQ(4, utils::bytes_required_for_digits(8));
    EXPECT_EQ(4, utils::bytes_required_for_digits(9));
    EXPECT_EQ(5, utils::bytes_required_for_digits(10));
    EXPECT_EQ(5, utils::bytes_required_for_digits(11));
    EXPECT_EQ(6, utils::bytes_required_for_digits(12));
    EXPECT_EQ(6, utils::bytes_required_for_digits(13));
    EXPECT_EQ(6, utils::bytes_required_for_digits(14));
    EXPECT_EQ(7, utils::bytes_required_for_digits(15));
    EXPECT_EQ(7, utils::bytes_required_for_digits(16));
    EXPECT_EQ(8, utils::bytes_required_for_digits(17));
    EXPECT_EQ(8, utils::bytes_required_for_digits(18));

    // 9     : [-2361183241434822606848, 2361183241434822606847] : 21
    // 10     : [-604462909807314587353088, 604462909807314587353087] : 23
    // 11     : [-154742504910672534362390528, 154742504910672534362390527] : 26
    // 12     : [-39614081257132168796771975168, 39614081257132168796771975167] : 28
    // 13     : [-10141204801825835211973625643008, 10141204801825835211973625643007] : 31
    // 14     : [-2596148429267413814265248164610048, 2596148429267413814265248164610047] : 33

    EXPECT_EQ(9, utils::bytes_required_for_digits(19));
    EXPECT_EQ(9, utils::bytes_required_for_digits(20));
    EXPECT_EQ(9, utils::bytes_required_for_digits(21));
    EXPECT_EQ(10, utils::bytes_required_for_digits(22));
    EXPECT_EQ(10, utils::bytes_required_for_digits(23));
    EXPECT_EQ(11, utils::bytes_required_for_digits(24));
    EXPECT_EQ(11, utils::bytes_required_for_digits(24));
    EXPECT_EQ(11, utils::bytes_required_for_digits(25));
    EXPECT_EQ(11, utils::bytes_required_for_digits(26));
    EXPECT_EQ(12, utils::bytes_required_for_digits(27));
    EXPECT_EQ(12, utils::bytes_required_for_digits(28));
    EXPECT_EQ(13, utils::bytes_required_for_digits(29));
    EXPECT_EQ(13, utils::bytes_required_for_digits(30));
    EXPECT_EQ(13, utils::bytes_required_for_digits(31));
    EXPECT_EQ(14, utils::bytes_required_for_digits(32));
    EXPECT_EQ(14, utils::bytes_required_for_digits(33));

    // 15     : [-664613997892457936451903530140172288, 664613997892457936451903530140172287] : 35
    // 16     : [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727] : 38

    EXPECT_EQ(15, utils::bytes_required_for_digits(34));
    EXPECT_EQ(15, utils::bytes_required_for_digits(35));
    EXPECT_EQ(16, utils::bytes_required_for_digits(36));
    EXPECT_EQ(16, utils::bytes_required_for_digits(37));
    EXPECT_EQ(16, utils::bytes_required_for_digits(38));
}

}

