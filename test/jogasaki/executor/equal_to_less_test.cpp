/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/equal_to.h>
#include <jogasaki/executor/less.h>
#include <jogasaki/executor/value_hash.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor {

/**
 * @brief tests that `equal_to`, `less` and `value_hash` define the same equivalence
 * @details `equal_to` is used for SQL comparison and COUNT(DISTINCT), while `less` (via `comparator`) is used for
 * sorting, grouping and join. They must agree, i.e. `equal_to(x, y)` iff `!less(x, y) && !less(y, x)`.
 * Also the values equal by `equal_to` must have the same `value_hash`.
 */
class equal_to_less_test : public test_root {};

template <class T>
void verify_consistency(std::vector<T> const& values) {
    for(std::size_t i = 0; i < values.size(); ++i) {
        for(std::size_t j = 0; j < values.size(); ++j) {
            auto& x = values[i];
            auto& y = values[j];
            bool eq = equal_to(x, y);
            bool equivalent = ! less{}(x, y) && ! less{}(y, x);
            EXPECT_EQ(eq, equivalent) << "i=" << i << " j=" << j;
            if(eq) {
                EXPECT_EQ(value_hash<T>{}(x), value_hash<T>{}(y)) << "i=" << i << " j=" << j;
            }
        }
    }
}

TEST_F(equal_to_less_test, int4) {
    verify_consistency<std::int32_t>({
        0, 1, -1,
        std::numeric_limits<std::int32_t>::max(),
        std::numeric_limits<std::int32_t>::min(),
    });
}

TEST_F(equal_to_less_test, int8) {
    verify_consistency<std::int64_t>({
        0, 1, -1,
        std::numeric_limits<std::int64_t>::max(),
        std::numeric_limits<std::int64_t>::min(),
    });
}

template <class T>
std::vector<T> float_values() {
    using limits = std::numeric_limits<T>;
    return {
        static_cast<T>(0), -static_cast<T>(0),
        static_cast<T>(1), static_cast<T>(-1), static_cast<T>(1.5),
        limits::max(), limits::lowest(), limits::min(), limits::denorm_min(),
        limits::infinity(), -limits::infinity(),
        // NaNs with different bit patterns
        limits::quiet_NaN(), -limits::quiet_NaN(), limits::signaling_NaN(),
    };
}

TEST_F(equal_to_less_test, float4) {
    verify_consistency(float_values<float>());
}

TEST_F(equal_to_less_test, float8) {
    verify_consistency(float_values<double>());
}

TEST_F(equal_to_less_test, decimal) {
    using triple = takatori::decimal::triple;
    verify_consistency<triple>({
        // zeros with different exponents
        triple{0, 0, 0, 0}, triple{0, 0, 0, -3}, triple{0, 0, 0, 5},
        // 1 with different representations
        triple{1, 0, 1, 0}, triple{1, 0, 10, -1}, triple{1, 0, 100, -2},
        // -1 with different representations
        triple{-1, 0, 1, 0}, triple{-1, 0, 10, -1},
        // 1.5 and 15
        triple{1, 0, 15, -1}, triple{1, 0, 150, -2}, triple{1, 0, 15, 0},
        // coefficient spanning upper 64-bits (3 * 10^20 * 10^-20 = 3)
        triple{1, 0x10UL, 0x43561A8829300000UL, -20}, triple{1, 0, 3, 0},
    });
}

TEST_F(equal_to_less_test, character) {
    using text = accessor::text;
    verify_consistency<text>({
        text{""}, text{"a"}, text{"a "}, text{"ab"}, text{"b"},
        // long text not inlined
        text{"0123456789012345678901234567890123456789"},
        text{"0123456789012345678901234567890123456789"},
    });
}

TEST_F(equal_to_less_test, octet) {
    using binary = accessor::binary;
    verify_consistency<binary>({
        binary{""}, binary{"\x01"}, binary{"\x01\x00"}, binary{"\x02"}, binary{"\xff"},
    });
}

TEST_F(equal_to_less_test, date) {
    using takatori::datetime::date;
    verify_consistency<date>({
        date{0}, date{1}, date{-1}, date{2024, 1, 1}, date{2024, 1, 1},
    });
}

TEST_F(equal_to_less_test, time_of_day) {
    using takatori::datetime::time_of_day;
    verify_consistency<time_of_day>({
        time_of_day{std::chrono::nanoseconds{0}},
        time_of_day{std::chrono::nanoseconds{1}},
        time_of_day{12, 0, 0},
        time_of_day{12, 0, 0},
    });
}

TEST_F(equal_to_less_test, time_point) {
    using takatori::datetime::time_point;
    using std::chrono::seconds;
    using std::chrono::nanoseconds;
    verify_consistency<time_point>({
        time_point{seconds{0}},
        time_point{seconds{0}, nanoseconds{1}},
        time_point{seconds{1}},
        time_point{seconds{-1}},
        time_point{seconds{-1}, nanoseconds{999'999'999}},
        time_point{seconds{0}, nanoseconds{1'000'000'000}},
    });
}

}  // namespace jogasaki::executor
