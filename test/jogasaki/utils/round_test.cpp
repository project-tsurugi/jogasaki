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
#include <jogasaki/utils/round.h>

#include <gtest/gtest.h>

namespace jogasaki::testing {


class round_test : public ::testing::Test {};


TEST_F(round_test, simple) {
    EXPECT_EQ(0, utils::round_down_to_power_of_two(0));
    EXPECT_EQ(1, utils::round_down_to_power_of_two(1));
    EXPECT_EQ(2, utils::round_down_to_power_of_two(2));
    EXPECT_EQ(2, utils::round_down_to_power_of_two(3));
    EXPECT_EQ(4, utils::round_down_to_power_of_two(4));
    EXPECT_EQ(4, utils::round_down_to_power_of_two(5));
    EXPECT_EQ(4, utils::round_down_to_power_of_two(7));
    EXPECT_EQ(8, utils::round_down_to_power_of_two(8));
    EXPECT_EQ(8, utils::round_down_to_power_of_two(9));

    std::size_t two_mega = 2*1024*1024;
    EXPECT_EQ(two_mega/2, utils::round_down_to_power_of_two(two_mega-1));
    EXPECT_EQ(two_mega, utils::round_down_to_power_of_two(two_mega));
    EXPECT_EQ(two_mega, utils::round_down_to_power_of_two(two_mega+1));
}

}

