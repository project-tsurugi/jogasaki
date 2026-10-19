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
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/int.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/request_context.h>
#include <jogasaki/value_operations/record_assignment.h>

namespace jogasaki::value_operations {
namespace {
using kind = meta::field_type_kind;
// A prepared plan cannot retain references to temporary declared types.
template<class Source, class Target, class = void>
struct can_prepare_assignment : std::false_type {};
template<class Source, class Target>
struct can_prepare_assignment<Source, Target, std::void_t<decltype(prepared_record_assignment::prepare(
    std::declval<Source>(), std::declval<Target>(), record_position{}, record_position{}, true))>> : std::true_type {};
template<class Source, class Target, class = void>
struct can_prepare_unification : std::false_type {};
template<class Source, class Target>
struct can_prepare_unification<Source, Target, std::void_t<decltype(prepared_record_unification::prepare(
    std::declval<Source>(), std::declval<Target>(), record_position{}, std::declval<record_destination>()))>>
    : std::true_type {};
static_assert(can_prepare_assignment<takatori::type::int4 const&, takatori::type::int8 const&>::value);
static_assert(!can_prepare_assignment<takatori::type::int4&&, takatori::type::int8 const&>::value);
static_assert(!can_prepare_assignment<takatori::type::int4 const&, takatori::type::int8&&>::value);
static_assert(can_prepare_unification<takatori::type::int4 const&, takatori::type::int8 const&>::value);
static_assert(!can_prepare_unification<takatori::type::int4&&, takatori::type::int8 const&>::value);
static_assert(!can_prepare_unification<takatori::type::int4 const&, takatori::type::int8&&>::value);

record_position position(mock::basic_record const& record, std::size_t index = 0) {
    return {record.record_meta()->value_offset(index), record.record_meta()->nullity_offset(index)};
}
record_destination destination(mock::basic_record const& record) {
    return record_destination::from_record(*record.record_meta(), 0);
}
} // namespace

TEST(record_assignment_test, assignment_same_type_fast_path) {
    takatori::type::int8 type{};
    auto source = mock::create_nullable_record<kind::int8>(42);
    auto target = mock::create_nullable_record<kind::int8>(99);
    auto operation = prepared_record_assignment::prepare(type, type, position(source), position(target), false);
    request_context context{};
    EXPECT_EQ(status::ok, operation.apply_borrowed(source.ref(), target.ref(), context, nullptr));
    EXPECT_EQ(42, target.get_value<std::int64_t>(0));
}

TEST(record_assignment_test, assignment_widens_integer) {
    takatori::type::int4 from{};
    takatori::type::int8 to{};
    auto source = mock::create_nullable_record<kind::int4>(42);
    auto target = mock::create_nullable_record<kind::int8>(99);
    auto operation = prepared_record_assignment::prepare(from, to, position(source), position(target), false);
    memory::page_pool pool{};
    memory::lifo_paged_memory_resource storage{&pool};
    request_context context{};
    EXPECT_EQ(status::ok, operation.apply_borrowed(source.ref(), target.ref(), context, &storage));
    EXPECT_EQ(42, target.get_value<std::int64_t>(0));
}

TEST(record_assignment_test, rejects_null_before_modifying_destination) {
    takatori::type::int4 from{};
    takatori::type::int8 to{};
    auto source = mock::create_nullable_record<kind::int4>(std::nullopt);
    auto target = mock::create_nullable_record<kind::int8>(99);
    auto operation = prepared_record_assignment::prepare(from, to, position(source), position(target), false);
    request_context context{};
    EXPECT_EQ(status::err_integrity_constraint_violation,
        operation.apply_borrowed(source.ref(), target.ref(), context, nullptr));
    ASSERT_TRUE(context.error_info());
    EXPECT_EQ(error_code::not_null_constraint_violation_exception, context.error_info()->code());
    EXPECT_EQ("Null assigned for non-nullable field.", context.error_info()->message());
    EXPECT_EQ(99, target.get_value<std::int64_t>(0));
    EXPECT_FALSE(target.is_null(0));
}

TEST(record_assignment_test, unification_propagates_null) {
    takatori::type::int4 from{};
    takatori::type::int8 to{};
    auto source = mock::create_nullable_record<kind::int4>(std::nullopt);
    auto target = mock::create_nullable_record<kind::int8>(99);
    auto operation = prepared_record_unification::prepare(from, to, position(source), destination(target));
    memory::page_pool pool{};
    memory::lifo_paged_memory_resource storage{&pool};
    EXPECT_EQ(status::ok, operation.apply_borrowed(source.ref(), target.ref(), &storage));
    EXPECT_TRUE(target.is_null(0));
}

TEST(record_assignment_test, no_conversion_borrows_long_character_data) {
    takatori::type::character type{takatori::type::varying};
    std::string bytes(128, 'x');
    auto source = mock::create_nullable_record<kind::character>(accessor::text{std::string_view{bytes}});
    auto target = mock::create_nullable_record<kind::character>();
    auto operation = prepared_record_unification::prepare(type, type, position(source), destination(target));
    EXPECT_EQ(status::ok, operation.apply_borrowed(source.ref(), target.ref(), nullptr));
    auto text = target.get_value<accessor::text>(0);
    EXPECT_EQ(bytes, static_cast<std::string_view>(text));
    EXPECT_EQ(bytes.data(), static_cast<std::string_view>(text).data());
}

TEST(record_assignment_test, assignment_preserves_conversion_error_status) {
    takatori::type::int4 from{};
    takatori::type::date to{};
    auto source = mock::create_nullable_record<kind::int4>(42);
    auto target = mock::create_nullable_record<kind::date>(takatori::datetime::date{2020, 1, 1});
    auto operation = prepared_record_assignment::prepare(from, to, position(source), position(target), true);
    request_context context{};
    EXPECT_EQ(status::err_unsupported, operation.apply_borrowed(source.ref(), target.ref(), context, nullptr));
    ASSERT_TRUE(context.error_info());
    EXPECT_EQ(error_code::unsupported_runtime_feature_exception, context.error_info()->code());
    EXPECT_EQ((takatori::datetime::date{2020, 1, 1}), target.get_value<takatori::datetime::date>(0));
}

TEST(record_assignment_test, unification_leaves_error_remapping_to_caller) {
    takatori::type::int4 from{};
    takatori::type::date to{};
    auto source = mock::create_nullable_record<kind::int4>(42);
    auto target = mock::create_nullable_record<kind::date>(takatori::datetime::date{2020, 1, 1});
    auto operation = prepared_record_unification::prepare(from, to, position(source), destination(target));
    EXPECT_EQ(status::err_expression_evaluation_failure,
        operation.apply_borrowed(source.ref(), target.ref(), nullptr));
    EXPECT_EQ((takatori::datetime::date{2020, 1, 1}), target.get_value<takatori::datetime::date>(0));
}

TEST(record_assignment_test, prepared_plan_reuses_bound_positions_for_multiple_rows) {
    takatori::type::int8 type{};
    auto source = mock::create_nullable_record<kind::int8, kind::int8>(10, 42);
    auto target = mock::create_nullable_record<kind::int8, kind::int8>(77, 99);
    auto operation = prepared_record_assignment::prepare(type, type, position(source, 1), position(target, 0), true);
    request_context context{};
    ASSERT_EQ(status::ok, operation.apply_borrowed(source.ref(), target.ref(), context, nullptr));
    EXPECT_EQ(42, target.get_value<std::int64_t>(0));
    auto next = mock::create_nullable_record<kind::int8, kind::int8>(33, 55);
    ASSERT_EQ(status::ok, operation.apply_borrowed(next.ref(), target.ref(), context, nullptr));
    EXPECT_EQ(55, target.get_value<std::int64_t>(0));
    EXPECT_EQ(99, target.get_value<std::int64_t>(1));
}

} // namespace jogasaki::value_operations
