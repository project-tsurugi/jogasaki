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
#include <boost/dynamic_bitset.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <type_traits>

#include <decimal.hh>

namespace jogasaki::executor::process::impl::expression {

using details::reset_decimal_status;

class nan_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        executor::process::impl::expression::details::ensure_decimal_context();
    }
};

template <class T>
void test_nan() {
    static_assert(std::numeric_limits<T>::is_iec559);

    static_assert(std::numeric_limits<T>::has_quiet_NaN);
    auto qn = std::numeric_limits<T>::quiet_NaN();
    EXPECT_FALSE(qn == qn);
    EXPECT_TRUE(qn != qn);
    EXPECT_FALSE(qn < qn);
    EXPECT_FALSE(qn <= qn);
    EXPECT_FALSE(qn > qn);
    EXPECT_FALSE(qn >= qn);

    EXPECT_FALSE(qn > static_cast<T>(0.0));
    EXPECT_FALSE(qn < static_cast<T>(0.0));
    EXPECT_FALSE(qn >= static_cast<T>(0.0));
    EXPECT_FALSE(qn <= static_cast<T>(0.0));

    // we don't use signaling NaN in production code, but it's here for comparison testing
    static_assert(std::numeric_limits<T>::has_signaling_NaN);
    auto sn = std::numeric_limits<T>::signaling_NaN();
    EXPECT_FALSE(sn == sn);
    EXPECT_TRUE(sn != sn);
    EXPECT_FALSE(sn < sn);
    EXPECT_FALSE(sn <= sn);
    EXPECT_FALSE(sn > sn);
    EXPECT_FALSE(sn >= sn);

    EXPECT_FALSE(sn > static_cast<T>(0.0));
    EXPECT_FALSE(sn < static_cast<T>(0.0));
    EXPECT_FALSE(sn >= static_cast<T>(0.0));
    EXPECT_FALSE(sn <= static_cast<T>(0.0));

    // compare different nans
    EXPECT_FALSE(qn == sn);
    EXPECT_TRUE(qn != sn);
    EXPECT_FALSE(qn < sn);
    EXPECT_FALSE(qn <= sn);
    EXPECT_FALSE(qn > sn);
    EXPECT_FALSE(qn >= sn);

    EXPECT_FALSE(sn == qn);
    EXPECT_TRUE(sn != qn);
    EXPECT_FALSE(sn < qn);
    EXPECT_FALSE(sn <= qn);
    EXPECT_FALSE(sn > qn);
    EXPECT_FALSE(sn >= qn);

    // compare with negative
    auto nqn = -qn;
    EXPECT_FALSE(nqn == qn);
    EXPECT_TRUE(nqn != qn);
    EXPECT_FALSE(nqn < qn);
    EXPECT_FALSE(nqn <= qn);
    EXPECT_FALSE(nqn > qn);
    EXPECT_FALSE(nqn >= qn);

    EXPECT_FALSE(qn == nqn);
    EXPECT_TRUE(qn != nqn);
    EXPECT_FALSE(qn < nqn);
    EXPECT_FALSE(qn <= nqn);
    EXPECT_FALSE(qn > nqn);
    EXPECT_FALSE(qn >= nqn);
}

TEST_F(nan_test, float_nan) {
    test_nan<float>();
}

TEST_F(nan_test, double_nan) {
    test_nan<double>();
}

void check_status_and_reset(std::uint32_t status = 0) {
    EXPECT_EQ(status, reset_decimal_status());
}

TEST_F(nan_test, decimal_nan) {
    reset_decimal_status();
    decimal::Decimal qn{"NaN"};
    check_status_and_reset();
    ASSERT_TRUE(qn.isqnan());

    reset_decimal_status();
    EXPECT_FALSE(qn == qn);
    check_status_and_reset();
    EXPECT_TRUE(qn != qn);
    check_status_and_reset();

    // order related operation raises exception even with quiet NaN
    reset_decimal_status();
    EXPECT_FALSE(qn < qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn <= qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn > qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn >= qn);
    check_status_and_reset(MPD_Invalid_operation);

    reset_decimal_status();
    EXPECT_TRUE(qn.compare(qn).isqnan());
    check_status_and_reset();
    EXPECT_FALSE(qn < decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn <= decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn > decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn >= decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);

    // we don't use signaling NaN in production code, but it's here for comparison testing
    reset_decimal_status();
    decimal::Decimal sn{};
    check_status_and_reset();
    ASSERT_TRUE(sn.issnan());

    reset_decimal_status();
    EXPECT_FALSE(sn == sn);
    check_status_and_reset(MPD_Invalid_operation); // snan sets MPD_Invalid_operation even for ==/!=
    EXPECT_TRUE(sn != sn);
    check_status_and_reset(MPD_Invalid_operation); // snan sets MPD_Invalid_operation even for ==/!=
    EXPECT_TRUE(sn.compare(sn).isqnan());
    check_status_and_reset(MPD_Invalid_operation);

    EXPECT_FALSE(sn < decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn <= decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn > decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn >= decimal::Decimal{0});
    check_status_and_reset(MPD_Invalid_operation);

    EXPECT_FALSE(sn < qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn <= qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn > qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(sn >= qn);
    check_status_and_reset(MPD_Invalid_operation);

    EXPECT_FALSE(qn < sn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn <= sn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn > sn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn >= sn);
    check_status_and_reset(MPD_Invalid_operation);

    // compare with negative
    auto nqn = -qn;
    check_status_and_reset();
    EXPECT_FALSE(nqn < qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(nqn <= qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(nqn > qn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(nqn >= qn);
    check_status_and_reset(MPD_Invalid_operation);

    EXPECT_FALSE(qn < nqn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn <= nqn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn > nqn);
    check_status_and_reset(MPD_Invalid_operation);
    EXPECT_FALSE(qn >= nqn);
    check_status_and_reset(MPD_Invalid_operation);
}

}  // namespace jogasaki::executor::process::impl::expression
