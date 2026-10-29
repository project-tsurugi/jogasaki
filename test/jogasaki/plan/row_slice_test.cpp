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
#include <cstddef>
#include <optional>
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/relation/details/row_slice.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/scan_direction.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/error_code.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/plan_exception.h>
#include <jogasaki/status.h>

namespace jogasaki::plan {

using row_slice = takatori::relation::details::row_slice;
using takatori::relation::scan;
using takatori::relation::scan_direction;

class row_slice_test : public ::testing::Test {
public:
    template <class Fn>
    void expect_unsupported(Fn&& fn) {
        try {
            fn();
            FAIL() << "plan_exception is not thrown";
        } catch (plan_exception const& e) {
            ASSERT_TRUE(e.info());
            EXPECT_EQ(error_code::unsupported_runtime_feature_exception, e.info()->code());
            EXPECT_EQ(status::err_unsupported, e.info()->status());
        }
    }

    scan create_scan(scan::direction_type direction, row_slice slice) {
        return scan{
            bindings_(takatori::plan::forward{}),
            std::vector<scan::column>{},
            {},
            {},
            direction,
            std::move(slice),
        };
    }

    yugawara::binding::factory bindings_{};
};

TEST_F(row_slice_test, whole_relation) {
    EXPECT_EQ(std::nullopt, impl::row_slice_to_limit(row_slice{}));
}

TEST_F(row_slice_test, count_only) {
    EXPECT_EQ(std::optional<std::size_t>{10}, impl::row_slice_to_limit(row_slice{{}, 10}));
}

TEST_F(row_slice_test, count_zero) {
    EXPECT_EQ(std::optional<std::size_t>{0}, impl::row_slice_to_limit(row_slice{{}, 0}));
}

TEST_F(row_slice_test, start_zero) {
    EXPECT_EQ(std::nullopt, impl::row_slice_to_limit(row_slice{0, {}}));
    EXPECT_EQ(std::optional<std::size_t>{10}, impl::row_slice_to_limit(row_slice{0, 10}));
}

TEST_F(row_slice_test, start_non_zero) {
    expect_unsupported([] { (void) impl::row_slice_to_limit(row_slice{1, 10}); });
    expect_unsupported([] { (void) impl::row_slice_to_limit(row_slice{1, {}}); });
}

TEST_F(row_slice_test, start_variable) {
    expect_unsupported([&] { (void) impl::row_slice_to_limit(row_slice{bindings_.stream_variable("s"), 10}); });
}

TEST_F(row_slice_test, count_variable) {
    expect_unsupported([&] { (void) impl::row_slice_to_limit(row_slice{{}, bindings_.stream_variable("c")}); });
}

TEST_F(row_slice_test, scan_supported) {
    EXPECT_NO_THROW(impl::validate_scan(create_scan({}, {})));
    EXPECT_NO_THROW(impl::validate_scan(create_scan(scan_direction::forward, {})));
    // row slice taking whole relation is supported
    EXPECT_NO_THROW(impl::validate_scan(create_scan({}, row_slice{0, {}})));
}

TEST_F(row_slice_test, scan_with_row_slice) {
    expect_unsupported([&] { impl::validate_scan(create_scan({}, row_slice{{}, 10})); });
    expect_unsupported([&] { impl::validate_scan(create_scan({}, row_slice{1, {}})); });
}

TEST_F(row_slice_test, scan_backward) {
    expect_unsupported([&] { impl::validate_scan(create_scan(scan_direction::backward, {})); });
}

}  // namespace jogasaki::plan
