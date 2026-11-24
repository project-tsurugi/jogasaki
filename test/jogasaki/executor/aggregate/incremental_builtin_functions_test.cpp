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
#include <cstdint>
#include <thread>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::function::incremental {

using kind = meta::field_type_kind;
using triple = takatori::decimal::triple;

class incremental_builtin_functions_test : public ::testing::Test {};

static field_locator locator(maybe_shared_ptr<meta::record_meta> const& meta, std::size_t index) {
    return field_locator{
        meta->at(index),
        true,
        meta->value_offset(index),
        meta->nullity_offset(index),
    };
}

// run on a new thread whose thread-local decimal::context has never been initialized by jogasaki
template <class F>
static void run_on_new_thread(F&& f) {
    std::thread t{std::forward<F>(f)};
    t.join();
}

TEST_F(incremental_builtin_functions_test, sum_decimal_keeps_precision_beyond_16_digits) {
    // regression test - the sum used mpdecimal default context (16 digits) on the threads that never ran expression evaluation
    auto dec = meta::decimal_type();
    auto target = mock::typed_nullable_record<kind::decimal>(std::tuple{dec}, triple{"12345678901234567890123"});
    auto source = mock::typed_nullable_record<kind::decimal>(std::tuple{dec}, triple{"1"});
    auto target_meta = target.record_meta();
    auto source_meta = source.record_meta();
    run_on_new_thread([&]() {
        auto arg = locator(source_meta, 0);
        builtin::sum(target.ref(), locator(target_meta, 0), false, source.ref(), sequence_view<field_locator const>{&arg}, nullptr);
    });
    EXPECT_EQ(
        decimal::Decimal{"12345678901234567890124"},
        static_cast<decimal::Decimal>(target.get_value<runtime_t<kind::decimal>>(0))
    );
}

TEST_F(incremental_builtin_functions_test, avg_post_decimal_keeps_precision_beyond_16_digits) {
    // regression test - the division used mpdecimal default context (16 digits) on the threads that never ran expression evaluation
    auto dec = meta::decimal_type();
    auto target = mock::typed_nullable_record<kind::decimal>(std::tuple{dec}, triple{"0"});
    auto source = mock::typed_nullable_record<kind::decimal, kind::int8>(
        std::tuple{dec, meta::field_type{meta::field_enum_tag<kind::int8>}},
        triple{"12345678901234567890124"},
        std::int64_t{2}
    );
    auto target_meta = target.record_meta();
    auto source_meta = source.record_meta();
    run_on_new_thread([&]() {
        std::array<field_locator, 2> args{locator(source_meta, 0), locator(source_meta, 1)};
        builtin::avg_post(target.ref(), locator(target_meta, 0), false, source.ref(), sequence_view<field_locator const>{args}, nullptr);
    });
    EXPECT_EQ(
        decimal::Decimal{"6172839450617283945062"},
        static_cast<decimal::Decimal>(target.get_value<runtime_t<kind::decimal>>(0))
    );
}

}  // namespace jogasaki::executor::function::incremental
