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
#include <decimal.hh>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/executor/expr/details/decimal_context_guard.h>
#include <jogasaki/test_root.h>


namespace jogasaki::executor::expr::details {

using namespace std::string_literals;
using namespace std::string_view_literals;

class decimal_context_guard_test : public test_root {
public:
    void SetUp() override {}
};

TEST_F(decimal_context_guard_test, simple) {
    decimal::context.round(MPD_ROUND_DOWN);
    EXPECT_EQ(MPD_ROUND_DOWN, decimal::context.round());
    {
        decimal_context_guard guard{};
        guard.round(MPD_ROUND_HALF_UP);
        EXPECT_EQ(MPD_ROUND_HALF_UP, decimal::context.round());
    }
    EXPECT_EQ(MPD_ROUND_DOWN, decimal::context.round());
}

TEST_F(decimal_context_guard_test, exception) {
    decimal::context.round(MPD_ROUND_DOWN);
    EXPECT_EQ(MPD_ROUND_DOWN, decimal::context.round());
    bool caught = false;
    try {
        decimal_context_guard guard{};
        guard.round(MPD_ROUND_HALF_UP);
        EXPECT_EQ(MPD_ROUND_HALF_UP, decimal::context.round());
        throw std::runtime_error("test");
    } catch(std::exception const& e) {
        caught = true;
    }
    EXPECT_TRUE(caught);
    EXPECT_EQ(MPD_ROUND_DOWN, decimal::context.round());
}
}  // namespace jogasaki::executor::expr
