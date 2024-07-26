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
#include <memory>
#include <chrono>
#include <gtest/gtest.h>

#include <jogasaki/utils/convert_offset.h>

namespace jogasaki::testing {

using namespace std::chrono_literals;

using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;
using utils::time_point_tz;
using utils::time_of_day_tz;

class convert_offset_test : public ::testing::Test {};

TEST_F(convert_offset_test, timestamptz_to_timestamp) {
    EXPECT_EQ(
        (time_point{date{2001, 1, 1}, time_of_day{3, 34, 56, 789ms}}),
        utils::remove_offset(time_point_tz{time_point{date{2001, 1, 1}, time_of_day{12, 34, 56, 789ms}}, 9*60})
    );
    EXPECT_EQ(
        (time_point{date{2000, 12, 31}, time_of_day{23, 34, 56, 789ms}}),
        utils::remove_offset(time_point_tz{time_point{date{2001, 1, 1}, time_of_day{12, 34, 56, 789ms}}, 13*60})
    );
}

TEST_F(convert_offset_test, timestamp_to_timestamptz) {
    EXPECT_EQ(
        (time_point_tz{time_point{date{2001, 1, 1}, time_of_day{12, 34, 56, 789ms}}, 9*60}),
        utils::add_offset(time_point{date{2001, 1, 1}, time_of_day{3, 34, 56, 789ms}}, 9*60)
    );
    EXPECT_EQ(
        (time_point_tz{time_point{date{2001, 1, 1}, time_of_day{1, 34, 56, 789ms}}, 13*60}),
        utils::add_offset(time_point{date{2000, 12, 31}, time_of_day{12, 34, 56, 789ms}}, 13*60)
    );
}

TEST_F(convert_offset_test, timetz_to_time) {
    EXPECT_EQ(
        (time_of_day{3, 34, 56, 789ms}),
        utils::remove_offset(time_of_day_tz{time_of_day{12, 34, 56, 789ms}, 9*60})
    );
    EXPECT_EQ(
        (time_of_day{23, 34, 56, 789ms}),
        utils::remove_offset(time_of_day_tz{time_of_day{12, 34, 56, 789ms}, 13*60})
    );
}

TEST_F(convert_offset_test, time_to_timetz) {
    EXPECT_EQ(
        (time_of_day_tz{time_of_day{12, 34, 56, 789ms}, 9*60}),
        utils::add_offset(time_of_day{3, 34, 56, 789ms}, 9*60)
    );
    EXPECT_EQ(
        (time_of_day_tz{time_of_day{1, 34, 56, 789ms}, 13*60}),
        utils::add_offset(time_of_day{12, 34, 56, 789ms}, 13*60)
    );
}
}

