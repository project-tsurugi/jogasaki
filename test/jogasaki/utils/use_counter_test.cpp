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
#include <chrono>
#include <utility>
#include <gtest/gtest.h>

#include <jogasaki/utils/use_counter.h>


namespace jogasaki::utils {

using namespace std::chrono_literals;

class use_counter_test : public ::testing::Test {};

TEST_F(use_counter_test, simple) {
    use_counter c{};
    EXPECT_EQ(1, c.count());
    {
        auto copy = c;
        EXPECT_EQ(2, c.count());
        EXPECT_EQ(2, copy.count());
    }
    EXPECT_EQ(1, c.count());
}

TEST_F(use_counter_test, assign) {
    use_counter c{};
    EXPECT_EQ(1, c.count());
    {
        use_counter copy{};
        copy = c;
        EXPECT_EQ(2, c.count());
        EXPECT_EQ(2, copy.count());
    }
    EXPECT_EQ(1, c.count());
}

TEST_F(use_counter_test, reset) {
    use_counter c{};
    EXPECT_EQ(1, c.count());
    auto copy = c;
    EXPECT_EQ(2, c.count());
    copy.reset();
    EXPECT_EQ(1, c.count());
}

TEST_F(use_counter_test, move) {
    use_counter c{};
    EXPECT_EQ(1, c.count());
    {
        auto copy = c;
        EXPECT_EQ(2, c.count());
        {
            auto moved = std::move(c); // c becomes empty - different counter from copy
            EXPECT_EQ(1, c.count());
            EXPECT_EQ(2, moved.count());
            copy.reset();
            EXPECT_EQ(1, moved.count());
        }
        EXPECT_EQ(1, c.count());
    }
    use_counter c2{};
    c = c2;
    EXPECT_EQ(2, c.count());
    EXPECT_EQ(2, c2.count());
}

}

