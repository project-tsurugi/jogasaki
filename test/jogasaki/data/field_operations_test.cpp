/*
 * Copyright 2026 Project Tsurugi.
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
#include <cmath>
#include <limits>
#include <gtest/gtest.h>
#include <takatori/type/int.h>
#include <jogasaki/data/convert_value.h>
#include <jogasaki/data/field_operations.h>
#include <jogasaki/executor/conv/record_assignment.h>
#include <jogasaki/request_context.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::data {
using k = meta::field_type_kind;
TEST(field_operations_test, same_plan_reads_writes_and_copies) {
    auto source = mock::create_record<k::int4>(42);
    auto target = mock::create_record<k::int4>(99);
    auto os = source.record_meta()->value_offset(0);
    auto plan = field_operations::for_type(source.record_meta()->at(0));
    any input;
    plan.read(source.ref(), os, input);
    EXPECT_EQ(input.to<std::int32_t>(), 42);
    plan.copy(target.ref(), os, input);
    EXPECT_EQ(source, target);
    target = mock::create_record<k::int4>(77);
    plan.copy(target.ref(), os, source.ref(), os);
    EXPECT_EQ(source, target);
}
TEST(field_operations_test, failed_write_preserves_value_and_nullity) {
    alignas(std::int64_t) char buffer[16]{};
    accessor::record_ref target{buffer, sizeof buffer};
    target.set_value<std::int32_t>(0, 99);
    target.set_null(64, true);
    auto plan = field_operations::for_type(meta::field_type{meta::field_enum_tag<k::int4>});
    auto wrong = any{std::in_place_type<double>, 4.5};
    EXPECT_THROW(plan.copy_nullable(target, 0, 64, wrong), std::logic_error);
    EXPECT_EQ(target.get_value<std::int32_t>(0), 99);
    EXPECT_TRUE(target.is_null(64));
    plan.copy_nullable(target, 0, 64, any{std::in_place_type<std::int32_t>, 42});
    EXPECT_FALSE(target.is_null(64));
    plan.copy_nullable(target, 0, 64, any{});
    EXPECT_TRUE(target.is_null(64));
}
TEST(field_operations_test, int1_and_int2_use_actual_record_storage) {
    for (auto kind : {k::int1, k::int2}) {
        auto type = kind == k::int1 ? meta::field_type{meta::field_enum_tag<k::int1>}
                                  : meta::field_type{meta::field_enum_tag<k::int2>};
        auto plan = field_operations::for_type(type);
        EXPECT_TRUE(plan.accepts(any{std::in_place_type<std::int32_t>, 42}));
        EXPECT_FALSE(plan.accepts(any{std::in_place_type<std::int8_t>, 42}));
    }
}
TEST(field_operations_test, numeric_conversion_retains_range_and_rounding) {
    using order = value_conversion::ordering;
    auto to_int8 = value_conversion::prepare(meta::field_type{meta::field_enum_tag<k::int8>});
    auto to_int4 = value_conversion::prepare(meta::field_type{meta::field_enum_tag<k::int4>});
    for (auto value : {std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN(), std::ldexp(1.0, 63)}) {
        auto input = any{std::in_place_type<double>, value};
        EXPECT_EQ(to_int8.apply(input).order, order::unsupported);
        EXPECT_EQ(input.type_index(), any::index<double>);
    }
    auto limit = any{std::in_place_type<std::int64_t>, std::numeric_limits<std::int64_t>::max()};
    auto high = to_int4.apply(limit);
    EXPECT_EQ(high.order, order::above_maximum);
    EXPECT_EQ(high.value.to<std::int32_t>(), std::numeric_limits<std::int32_t>::max());
    EXPECT_EQ(limit.to<std::int64_t>(), std::numeric_limits<std::int64_t>::max());
    auto minimum = any{std::in_place_type<std::int64_t>, std::numeric_limits<std::int64_t>::min()};
    auto low = to_int4.apply(minimum);
    EXPECT_EQ(low.order, order::below_minimum);
    EXPECT_EQ(low.value.to<std::int32_t>(), std::numeric_limits<std::int32_t>::min());
    auto lower = to_int4.apply(any{std::in_place_type<double>, -2147483648.5});
    EXPECT_EQ(lower.order, order::rounded_up);
    EXPECT_EQ(lower.value.to<std::int32_t>(), std::numeric_limits<std::int32_t>::min());
    EXPECT_EQ(to_int4.apply(any{}).order, order::exact);
    EXPECT_EQ(value_conversion::prepare(meta::field_type{}).apply(limit).order, order::unsupported);
}
TEST(field_operations_test, prepared_assignment_applies_without_legacy_value_conversion) {
    takatori::type::int4 source_type;
    takatori::type::int8 target_type;
    auto target = mock::create_record<k::int8>(99);
    auto offset = target.record_meta()->value_offset(0);
    auto plan = executor::conv::record_assignment::prepare_value(source_type, target_type,
        target.record_meta()->at(0), {offset, static_cast<std::size_t>(-1)}, false,
        executor::conv::conversion_kind::assignment);
    request_context context;
    request_context error_context;
    auto error_value = any{std::in_place_type<executor::expr::error>, executor::expr::error_kind::arithmetic_error};
    EXPECT_EQ(plan.apply(error_value, target.ref(), &error_context, nullptr), status::err_expression_evaluation_failure);
    EXPECT_EQ(target.ref().get_value<std::int64_t>(offset), 99);
    ASSERT_EQ(plan.apply(any{std::in_place_type<std::int32_t>, 42}, target.ref(), &context, nullptr), status::ok);
    EXPECT_EQ(target.ref().get_value<std::int64_t>(offset), 42);
    EXPECT_EQ(plan.apply(any{}, target.ref(), &context, nullptr), status::err_integrity_constraint_violation);
    EXPECT_EQ(target.ref().get_value<std::int64_t>(offset), 42);
}
TEST(field_operations_test, context_is_required_only_for_sql_assignment_conversion) {
    takatori::type::int4 small;
    takatori::type::int8 large;
    auto target = mock::create_record<k::int8>(99);
    auto offset = target.record_meta()->value_offset(0);
    auto plan = executor::conv::record_assignment::prepare_value(small, large,
        target.record_meta()->at(0), {offset, static_cast<std::size_t>(-1)}, false,
        executor::conv::conversion_kind::assignment);
    EXPECT_EQ(plan.apply(any{std::in_place_type<std::int32_t>, 42}, target.ref(), nullptr, nullptr), status::err_invalid_state);
    EXPECT_EQ(target.ref().get_value<std::int64_t>(offset), 99);
    auto copy = executor::conv::record_assignment::prepare_value(large, large,
        target.record_meta()->at(0), {offset, static_cast<std::size_t>(-1)}, false,
        executor::conv::conversion_kind::assignment);
    ASSERT_EQ(copy.apply(any{std::in_place_type<std::int64_t>, 42}, target.ref(), nullptr, nullptr), status::ok);
    EXPECT_EQ(target.ref().get_value<std::int64_t>(offset), 42);
}

TEST(field_operations_test, prepared_key_conversion_preserves_source) {
    using order = value_conversion::ordering;
    auto conversion = value_conversion::prepare(meta::field_type{meta::field_enum_tag<k::int8>});
    auto integer = any{std::in_place_type<std::int32_t>, 42};
    auto floating = any{std::in_place_type<double>, -3.75};
    auto integral = conversion.apply(integer);
    auto rounded = conversion.apply(floating);
    EXPECT_EQ(integral.order, order::exact);
    EXPECT_EQ(integral.value.to<std::int64_t>(), 42);
    EXPECT_EQ(rounded.order, order::rounded_up);
    EXPECT_EQ(rounded.value.to<std::int64_t>(), -3);
    EXPECT_EQ(integer.to<std::int32_t>(), 42);
    EXPECT_EQ(floating.to<double>(), -3.75);
    EXPECT_TRUE(conversion.apply(any{}).value.empty());
    auto mismatch = any{std::in_place_type<accessor::text>, accessor::text{"text"}};
    EXPECT_EQ(conversion.apply(mismatch).order, order::unsupported);
    auto identity = value_conversion::prepare(meta::field_type{std::make_shared<meta::character_field_option>()});
    EXPECT_EQ(identity.apply(mismatch).order, order::exact);
    EXPECT_EQ(identity.apply(integer).order, order::unsupported);
}

TEST(field_operations_test, approximate_conversion_retains_rounding_and_underflow) {
    using order = value_conversion::ordering;
    auto conversion = value_conversion::prepare(meta::field_type{meta::field_enum_tag<k::float4>});
    EXPECT_EQ(conversion.apply(any{std::in_place_type<double>, 0.1}).order, order::rounded_up);
    EXPECT_EQ(conversion.apply(any{std::in_place_type<double>, 0.01}).order, order::rounded_down);
    EXPECT_EQ(conversion.apply(any{std::in_place_type<double>, static_cast<double>(0.1F)}).order, order::exact);
    auto underflow = conversion.apply(any{std::in_place_type<double>, std::numeric_limits<double>::denorm_min()});
    EXPECT_EQ(underflow.order, order::rounded_down);
    EXPECT_EQ(underflow.value.to<float>(), 0);
}

TEST(field_operations_test, real_overflow_is_unsupported_without_mutation) {
    using order = value_conversion::ordering;
    auto conversion = value_conversion::prepare(meta::field_type{meta::field_enum_tag<k::float4>});
    auto limit = static_cast<double>(std::numeric_limits<float>::max());
    // The exact positive and negative REAL limits remain representable.
    for (double boundary : {limit, -limit}) {
        auto result = conversion.apply(any{std::in_place_type<double>, boundary});
        ASSERT_EQ(order::exact, result.order);
        EXPECT_EQ(static_cast<double>(result.value.to<float>()), boundary);
    }
    // The nearest out-of-range DOUBLE and very large finite values must not be clamped.
    for (double value : {std::nextafter(limit, std::numeric_limits<double>::infinity()),
                         std::nextafter(-limit, -std::numeric_limits<double>::infinity()),
                         std::numeric_limits<double>::max(), -std::numeric_limits<double>::max()}) {
        SCOPED_TRACE(value);
        ASSERT_TRUE(std::isfinite(value));
        auto input = any{std::in_place_type<double>, value};
        auto result = conversion.apply(input);
        EXPECT_EQ(order::unsupported, result.order);
        EXPECT_EQ(input.type_index(), any::index<double>);
        EXPECT_EQ(input.to<double>(), value);
        EXPECT_EQ(result.value.type_index(), any::index<double>);
        EXPECT_EQ(result.value.to<double>(), value);
    }
}

}
