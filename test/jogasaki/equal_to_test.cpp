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
#include <jogasaki/executor/equal_to.h>

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

class equal_to_test : public ::testing::Test {
public:

    template <class Float>
    void test_float_values();
};

using kind = field_type_kind;
using text = accessor::text;

TEST_F(equal_to_test, simple) {
    EXPECT_FALSE(equal_to<rtype<ft::int8>>(100, 200));
    EXPECT_FALSE(equal_to<rtype<ft::int8>>(200, 100));
    EXPECT_TRUE(equal_to<rtype<ft::int8>>(0, 0));
    EXPECT_TRUE(equal_to<rtype<ft::int8>>(1, 1));
    EXPECT_TRUE(equal_to<rtype<ft::int8>>(-1, -1));
}

TEST_F(equal_to_test, simple_types) {
    EXPECT_TRUE(equal_to<rtype<ft::int4>>(-1, -1));
    EXPECT_FALSE(equal_to<rtype<ft::int4>>(-1, 0));
    EXPECT_FALSE(equal_to<rtype<ft::int4>>(0, 1));
    EXPECT_TRUE(equal_to<rtype<ft::int4>>(1, 1));

    EXPECT_TRUE(equal_to<rtype<ft::int8>>(-1, -1));
    EXPECT_FALSE(equal_to<rtype<ft::int8>>(-1, 0));
    EXPECT_FALSE(equal_to<rtype<ft::int8>>(0, 1));
    EXPECT_TRUE(equal_to<rtype<ft::int8>>(1, 1));

    EXPECT_TRUE(equal_to<rtype<ft::float4>>(-1, -1));
    EXPECT_FALSE(equal_to<rtype<ft::float4>>(-1, 0));
    EXPECT_FALSE(equal_to<rtype<ft::float4>>(0, 1));
    EXPECT_TRUE(equal_to<rtype<ft::float4>>(1, 1));

    EXPECT_TRUE(equal_to<rtype<ft::float8>>(-1, -1));
    EXPECT_FALSE(equal_to<rtype<ft::float8>>(-1, 0));
    EXPECT_FALSE(equal_to<rtype<ft::float8>>(0, 1));
    EXPECT_TRUE(equal_to<rtype<ft::float8>>(1, 1));
}

template <class Float>
void equal_to_test::test_float_values() {
    auto pinf = std::numeric_limits<Float>::infinity();
    auto pnan = std::numeric_limits<Float>::quiet_NaN();
    auto nnan = -std::numeric_limits<Float>::quiet_NaN();
    auto pzero = static_cast<Float>(0.0);
    auto nzero = static_cast<Float>(-0.0);
    auto ninf = -std::numeric_limits<Float>::infinity();
    // ninf < nzero = pzero < pinf < nnan = pnan

    EXPECT_FALSE(equal_to<Float>(1.0F, pinf));
    EXPECT_FALSE(equal_to<Float>(ninf, 1.0F));

    EXPECT_FALSE(equal_to<Float>(pnan, 1.0F));
    EXPECT_FALSE(equal_to<Float>(1.0F, pnan));

    EXPECT_TRUE(equal_to<Float>(pnan, pnan));
    EXPECT_TRUE(equal_to<Float>(nnan, nnan));

    EXPECT_TRUE(equal_to<Float>(nnan, pnan));
    EXPECT_TRUE(equal_to<Float>(pnan, nnan));

    EXPECT_TRUE(equal_to<Float>(nzero, pzero));
    EXPECT_TRUE(equal_to<Float>(pzero, nzero));
}

TEST_F(equal_to_test, float4_values) {
    test_float_values<float>();
}

TEST_F(equal_to_test, float8_values) {
    test_float_values<double>();
}

TEST_F(equal_to_test, character) {
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text(""), text("A")));
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text("A"), text("B")));
    EXPECT_TRUE(equal_to<rtype<ft::character>>(text("A"), text("A")));
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text("A"), text("A ")));
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text("A  "), text("A ")));
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text("B"), text("BA")));
    EXPECT_FALSE(equal_to<rtype<ft::character>>(text("BA"), text("BB")));
}

TEST_F(equal_to_test, date) {
    using date = rtype<ft::date>;
    EXPECT_FALSE(equal_to<date>(date{-2}, date{-1}));
    EXPECT_TRUE(equal_to<date>(date{0}, date{0}));
    EXPECT_TRUE(equal_to<date>(date{1}, date{1}));
    EXPECT_FALSE(equal_to<date>(date{1}, date{2}));
}

TEST_F(equal_to_test, time_of_day) {
    using time_of_day = rtype<ft::time_of_day>;
    auto const eod = 1ns*(1000L*1000*1000*24*60*60-1);
    EXPECT_TRUE(equal_to<time_of_day>(time_of_day{0ns}, time_of_day{0ns}));
    EXPECT_TRUE(equal_to<time_of_day>(time_of_day{1ns}, time_of_day{1ns}));
    EXPECT_FALSE(equal_to<time_of_day>(time_of_day{0ns}, time_of_day{1ns}));
    EXPECT_FALSE(equal_to<time_of_day>(time_of_day{1ns}, time_of_day{2ns}));
    EXPECT_FALSE(equal_to<time_of_day>(time_of_day{2ns}, time_of_day{eod}));
}

TEST_F(equal_to_test, time_point) {
    using time_point = rtype<ft::time_point>;
    EXPECT_TRUE(equal_to<time_point>(time_point{0ns}, time_point{0ns}));
    EXPECT_FALSE(equal_to<time_point>(time_point{-2ns}, time_point{-1ns}));
    EXPECT_FALSE(equal_to<time_point>(time_point{-1ns}, time_point{0ns}));
    EXPECT_FALSE(equal_to<time_point>(time_point{0ns}, time_point{1ns}));
    EXPECT_FALSE(equal_to<time_point>(time_point{1ns}, time_point{2ns}));

    EXPECT_TRUE(equal_to<time_point>(time_point{-1s, 100ms}, time_point{-1s, 100ms}));
    EXPECT_FALSE(equal_to<time_point>(time_point{-1s, 100ms}, time_point{-1s, 200ms}));
    EXPECT_FALSE(equal_to<time_point>(time_point{-1s, 100ms}, time_point{0s, 100ms}));
    EXPECT_FALSE(equal_to<time_point>(time_point{-1s, 900ms}, time_point{0s, 0ms}));
    EXPECT_FALSE(equal_to<time_point>(time_point{0s, 0ms}, time_point{0s, 100ms}));
}

}

