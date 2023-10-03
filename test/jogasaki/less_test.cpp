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
#include <jogasaki/executor/less.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::executor {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace meta;
using namespace takatori::util;
using namespace jogasaki::mock;

class less_test : public ::testing::Test {};

using kind = field_type_kind;
using text = accessor::text;

TEST_F(less_test, simple) {
    EXPECT_TRUE(less<rtype<ft::int8>>(100, 200));
    EXPECT_FALSE(less<rtype<ft::int8>>(200, 100));
    EXPECT_FALSE(less<rtype<ft::int8>>(0, 0));
    EXPECT_FALSE(less<rtype<ft::int8>>(1, 1));
    EXPECT_FALSE(less<rtype<ft::int8>>(-1, -1));
}

TEST_F(less_test, simple_types) {
    EXPECT_TRUE(less<rtype<ft::int4>>(-2, -1));
    EXPECT_TRUE(less<rtype<ft::int4>>(-1, 0));
    EXPECT_TRUE(less<rtype<ft::int4>>(0, 1));
    EXPECT_TRUE(less<rtype<ft::int4>>(1, 2));

    EXPECT_TRUE(less<rtype<ft::int8>>(-2, -1));
    EXPECT_TRUE(less<rtype<ft::int8>>(-1, 0));
    EXPECT_TRUE(less<rtype<ft::int8>>(0, 1));
    EXPECT_TRUE(less<rtype<ft::int8>>(1, 2));

    EXPECT_TRUE(less<rtype<ft::float4>>(-2, -1));
    EXPECT_TRUE(less<rtype<ft::float4>>(-1, 0));
    EXPECT_TRUE(less<rtype<ft::float4>>(0, 1));
    EXPECT_TRUE(less<rtype<ft::float4>>(1, 2));

    EXPECT_TRUE(less<rtype<ft::float8>>(-2, -1));
    EXPECT_TRUE(less<rtype<ft::float8>>(-1, 0));
    EXPECT_TRUE(less<rtype<ft::float8>>(0, 1));
    EXPECT_TRUE(less<rtype<ft::float8>>(1, 2));
}

TEST_F(less_test, character) {
    EXPECT_TRUE(less<rtype<ft::character>>(text(""), text("A")));
    EXPECT_TRUE(less<rtype<ft::character>>(text("A"), text("B")));
    EXPECT_TRUE(less<rtype<ft::character>>(text("B"), text("BA")));
    EXPECT_TRUE(less<rtype<ft::character>>(text("BA"), text("BB")));
}

TEST_F(less_test, date) {
    using date = rtype<ft::date>;
    EXPECT_TRUE(less<date>(date{-2}, date{-1}));
    EXPECT_TRUE(less<date>(date{-1}, date{0}));
    EXPECT_TRUE(less<date>(date{0}, date{1}));
    EXPECT_TRUE(less<date>(date{1}, date{2}));
}

TEST_F(less_test, time_of_day) {
    using time_of_day = rtype<ft::time_of_day>;
    auto const eod = 1ns*(1000L*1000*1000*24*60*60-1);
    EXPECT_TRUE(less<time_of_day>(time_of_day{0ns}, time_of_day{1ns}));
    EXPECT_TRUE(less<time_of_day>(time_of_day{1ns}, time_of_day{2ns}));
    EXPECT_TRUE(less<time_of_day>(time_of_day{2ns}, time_of_day{eod}));
}

TEST_F(less_test, time_point) {
    using time_point = rtype<ft::time_point>;
    EXPECT_TRUE(less<time_point>(time_point{-2ns}, time_point{-1ns}));
    EXPECT_TRUE(less<time_point>(time_point{-1ns}, time_point{0ns}));
    EXPECT_TRUE(less<time_point>(time_point{0ns}, time_point{1ns}));
    EXPECT_TRUE(less<time_point>(time_point{1ns}, time_point{2ns}));

    EXPECT_TRUE(less<time_point>(time_point{-1s, 100ms}, time_point{-1s, 200ms}));
    EXPECT_TRUE(less<time_point>(time_point{-1s, 200ms}, time_point{-1s, 900ms}));
    EXPECT_TRUE(less<time_point>(time_point{-1s, 900ms}, time_point{0s, 0ms}));
    EXPECT_TRUE(less<time_point>(time_point{0s, 0ms}, time_point{0s, 100ms}));

    EXPECT_TRUE(less<time_point>(time_point{0s, 100ms}, time_point{1s, 100ms}));
    EXPECT_TRUE(less<time_point>(time_point{1s, 100ms}, time_point{2s,  50ms}));

    EXPECT_TRUE(less<time_point>(time_point{2s,  50ms}, time_point{2s, 100ms}));
}

}

