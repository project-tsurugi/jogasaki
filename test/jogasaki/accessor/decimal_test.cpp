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
#include <decimal.hh>
#include <takatori/value/decimal.h>

#include <gtest/gtest.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;

class decimal_test : public ::testing::Test {};

TEST_F(decimal_test, build) {
    decimal::Context ctx{decimal::IEEEContext(128)};
    decimal::Decimal d{"3.14", ctx};
    decimal::Decimal exp{mpd_uint128_triple_t{MPD_TRIPLE_NORMAL, 0, 0, 314, -2} }; // sign=0 means positive
    ASSERT_EQ(exp, d);
}

TEST_F(decimal_test, from_triple) {
    auto zero = static_cast<::decimal::Decimal>(takatori::decimal::triple{0, 0, 0, 0});
    EXPECT_EQ(0, zero);

    auto ten0 = static_cast<::decimal::Decimal>(takatori::decimal::triple{1, 0, 10, 0});
    auto ten1 = static_cast<::decimal::Decimal>(takatori::decimal::triple{1, 0, 1, 1});
    EXPECT_EQ(10, ten0);
    EXPECT_EQ(10, ten1);
    EXPECT_EQ(ten0, ten1);

    auto nten0 = static_cast<::decimal::Decimal>(takatori::decimal::triple{-1, 0, 10, 0});
    auto nten1 = static_cast<::decimal::Decimal>(takatori::decimal::triple{-1, 0, 1, 1});
    EXPECT_EQ(-10, nten0);
    EXPECT_EQ(-10, nten1);
    EXPECT_EQ(nten0, nten1);
}


}

