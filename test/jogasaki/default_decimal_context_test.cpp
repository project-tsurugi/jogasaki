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
#include <cstdlib>

#include <gtest/gtest.h>

#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>

namespace jogasaki::testing {

class default_decimal_context_test : public ::testing::Test {};

TEST_F(default_decimal_context_test, simple) {
    EXPECT_NE(executor::process::impl::expression::details::standard_decimal_context(), decimal::context);
    EXPECT_EQ(16, decimal::context.prec());  // default precision is too small for our purpose
    std::cerr << "decimal default context:" << decimal::context << std::endl;

    executor::process::impl::expression::details::ensure_decimal_context();
    EXPECT_EQ(executor::process::impl::expression::details::standard_decimal_context(), decimal::context);
    EXPECT_EQ(38, decimal::context.prec());
    std::cerr << "standard default context:" << decimal::context << std::endl;

    {
        // verify ensure_decimal_context is no-op after the first call
        decimal::context = decimal::IEEEContext(32);
        auto prec = decimal::context.prec();
        executor::process::impl::expression::details::ensure_decimal_context(); // no-op
        EXPECT_EQ(prec, decimal::context.prec());

    }
}

}  // namespace jogasaki::testing
