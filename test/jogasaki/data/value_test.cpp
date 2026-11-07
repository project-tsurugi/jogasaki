/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/data/value.h>
#include <jogasaki/executor/process/impl/variable_table.h>

namespace jogasaki::data {
TEST(value_test, simple) {
    value v;
    EXPECT_FALSE(v); EXPECT_TRUE(v.empty());
    v = value::copy(any{std::in_place_type<std::int32_t>, 1});
    EXPECT_TRUE(v); EXPECT_FALSE(v.empty()); EXPECT_EQ(v.view().to<std::int32_t>(), 1);
}
TEST(value_test, fail_on_type_mismatch) {
    value v;
    EXPECT_THROW((void)v.view().to<std::int32_t>(), std::logic_error);
    v = value::copy(any{std::in_place_type<std::int64_t>, 1});
    EXPECT_THROW((void)v.view().to<std::int32_t>(), std::logic_error);
}
TEST(value_test, bool) {
    for (bool input : {false, true}) {
        auto v = value::copy(any{std::in_place_type<bool>, input});
        EXPECT_TRUE(v); EXPECT_FALSE(v.empty());
        EXPECT_EQ(v.view().to<bool>(), input);
        EXPECT_EQ(v.view().to<std::int8_t>(), input ? 1 : 0);
    }
}
TEST(value_test, string) {
    for (auto length : {0U, 3U, 4096U}) {
        std::string input(length, 'a');
        auto v = value::copy(any{std::in_place_type<accessor::text>, accessor::text{std::string_view{input}}});
        input.assign(length, 'x');
        auto copy = v;
        v = {};
        auto t = copy.view().to<accessor::text>();
        EXPECT_EQ(std::string_view(t), std::string(length, 'a'));
    }
}
TEST(value_test, binary) {
    std::string input(4096, '\0');
    auto v = value::copy(any{std::in_place_type<accessor::binary>, accessor::binary{std::string_view{input}}});
    input.assign(4096, 'x');
    auto moved = std::move(v);
    EXPECT_TRUE(v.empty());
    auto bytes = moved.view().to<accessor::binary>();
    EXPECT_EQ(std::string_view(bytes), std::string(4096, '\0'));
    moved = std::move(moved);
    EXPECT_FALSE(moved.empty());
}
TEST(value_test, execution_variables_retain_parameter_ownership) {
    executor::process::impl::variable_table table;
    auto owned = value::copy(any{std::in_place_type<accessor::text>, accessor::text{std::string(4096, 'a')}});
    auto borrowed = table.retain(owned);
    owned = {};
    for (int i = 0; i < 100; ++i) (void)table.retain(value::copy(any{std::in_place_type<std::int64_t>, i}));
    auto text = borrowed.to<accessor::text>();
    EXPECT_EQ(std::string_view(text), std::string(4096, 'a'));
}
TEST(value_test, native_decimal_preserves_exponent) {
    auto original = takatori::decimal::triple{1, 0, 123, 9};
    auto v = value::copy(any{std::in_place_type<takatori::decimal::triple>, original});
    EXPECT_EQ(v.view().to<takatori::decimal::triple>(), original);
}
TEST(value_test, lob_locator_lifetime_is_shared_with_execution) {
    executor::process::impl::variable_table table;
    auto owner = value::from_blob(lob::blob_locator{});
    auto borrowed = table.retain(owner);
    auto locator = borrowed.to<lob::blob_reference>().locator();
    ASSERT_NE(locator, nullptr);
    owner = {};
    auto copied = value::copy(borrowed);
    EXPECT_NE(copied.view().to<lob::blob_reference>().locator(), locator);
    EXPECT_EQ(copied.view().to<lob::blob_reference>().kind(), lob::lob_reference_kind::provided);
}
}
