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

#include <gtest/gtest.h>

#include <jogasaki/udf/round_robin_selector.h>

namespace jogasaki::testing {

TEST(udf_round_robin_selector_test, selects_in_round_robin_order) {
    plugin::udf::round_robin_selector selector{};

    EXPECT_EQ(0, selector.next(3));
    EXPECT_EQ(1, selector.next(3));
    EXPECT_EQ(2, selector.next(3));
    EXPECT_EQ(0, selector.next(3));
    EXPECT_EQ(1, selector.next(3));
    EXPECT_EQ(2, selector.next(3));
}

TEST(udf_round_robin_selector_test, single_endpoint_always_selects_zero) {
    plugin::udf::round_robin_selector selector{};

    EXPECT_EQ(0, selector.next(1));
    EXPECT_EQ(0, selector.next(1));
    EXPECT_EQ(0, selector.next(1));
}

} // namespace jogasaki::testing
