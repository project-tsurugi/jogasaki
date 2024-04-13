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
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/data.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <mizugaki/placeholder_entry.h>
#include <mizugaki/translator/shakujo_translator.h>
#include <mizugaki/translator/shakujo_translator_code.h>
#include <mizugaki/translator/shakujo_translator_options.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::process::impl::expression {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace yugawara::binding;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using namespace testing;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

using take = relation::step::take_flat;
using offer = relation::step::offer;
using buffer = relation::buffer;

using rgraph = ::takatori::relation::graph_type;

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

using takatori::decimal::triple;
using accessor::text;

class cast_to_string_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        executor::process::impl::expression::details::ensure_decimal_context();
    }

    yugawara::analyzer::variable_mapping& variables() noexcept {
        return *variables_;
    }

    yugawara::analyzer::expression_mapping& expressions() noexcept {
        return *expressions_;
    }

    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_ = std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expressions_ = std::make_shared<yugawara::analyzer::expression_mapping>();

    factory f_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    variable_table_info info_{};
    variable_table vars_{};

    compiled_info c_info_{};
    expression::evaluator evaluator_{};
    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

inline immediate constant(int v, type::data&& type = type::int8()) {
    return immediate { value::int8(v), std::move(type) };
}

inline immediate constant_bool(bool v, type::data&& type = type::boolean()) {
    return immediate { value::boolean(v), std::move(type) };
}

any any_text(std::string_view s) {
    return any{std::in_place_type<text>, s};
}

void check_lost_precision(bool expected, evaluator_context& ctx) {
    EXPECT_EQ(expected, ctx.lost_precision());
    ctx.lost_precision(false);
}

#define lost_precision(arg) {   \
    SCOPED_TRACE("check_lost_precision");   \
    check_lost_precision(arg, ctx); \
}

TEST_F(cast_to_string_test, from_int) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("1"), details::from_int4::to_character(1, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-1"), details::from_int4::to_character(-1, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-100"), details::from_int4::to_character(-100, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-100  "), details::from_int4::to_character(-100, ctx, 6, true)); lost_precision(false);
}

TEST_F(cast_to_string_test, truncate) {
    evaluator_context ctx{&resource_};
    {
        // no truncation
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "ABC"}), details::truncate_or_pad_if_needed(ctx, "ABC", 3, false, false, lost_precision));
        EXPECT_FALSE(lost_precision);
    }
    {
        // truncated non-padding char
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "AB"}), details::truncate_or_pad_if_needed(ctx, "ABC", 2, false, false, lost_precision));
        EXPECT_TRUE(lost_precision);
    }
    {
        // truncated padding char, but is not lenient
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "AB"}), details::truncate_or_pad_if_needed(ctx, "AB  ", 2, false, false, lost_precision));
        EXPECT_TRUE(lost_precision);
    }
    {
        // truncated padding char, and is lenient
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "AB"}),
            details::truncate_or_pad_if_needed(ctx, "AB  ", 2, false, true, lost_precision));
        EXPECT_FALSE(lost_precision);
    }
}

TEST_F(cast_to_string_test, padding) {
    evaluator_context ctx{&resource_};
    {
        // no padding
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "ABC"}),
            details::truncate_or_pad_if_needed(ctx, "ABC", 5, false, false, lost_precision));
        EXPECT_FALSE(lost_precision);
    }
    {
        // add padding
        bool lost_precision = false;
        EXPECT_EQ((any{std::in_place_type<text>, "ABC  "}),
            details::truncate_or_pad_if_needed(ctx, "ABC", 5, true, false, lost_precision));
        EXPECT_FALSE(lost_precision);
    }
}

TEST_F(cast_to_string_test, from_int1_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("127"), details::from_int4::to_character(127, ctx, std::nullopt, false)); lost_precision(false);  // 2^7-1
    EXPECT_EQ(any_text("-128"), details::from_int4::to_character(-128, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_int2_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("32767"), details::from_int4::to_character(32767, ctx, std::nullopt, false)); lost_precision(false); // 2^15-1
    EXPECT_EQ(any_text("-32768"), details::from_int4::to_character(-32768, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_int4_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("2147483647"), details::from_int4::to_character(2147483647, ctx, std::nullopt, false)); lost_precision(false);  // 2^31-1
    EXPECT_EQ(any_text("-2147483648"), details::from_int4::to_character(-2147483648, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_int8_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("9223372036854775807"), details::from_int8::to_character(9'223'372'036'854'775'807L, ctx, std::nullopt, false)); lost_precision(false);  // 2^63-1
    EXPECT_EQ(any_text("-9223372036854775808"), details::from_int8::to_character(-9'223'372'036'854'775'807L-1, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_boolean) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("false"), details::from_boolean::to_character(0, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("true"), details::from_boolean::to_character(1, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_decimal) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("1"), details::from_decimal::to_character(triple{1}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-1"), details::from_decimal::to_character(triple{-1}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("0"), details::from_decimal::to_character(triple{}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("1.23"), details::from_decimal::to_character(triple{1, 0, 123, -2}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-12345.67890"), details::from_decimal::to_character(triple{-1, 0, 1234567890, -5}, ctx, std::nullopt, false)); lost_precision(false);

    // scientific representation if exp > 0 or adjexp < -6
    EXPECT_EQ(any_text("-1234567890"), details::from_decimal::to_character(triple{-1, 0, 1234567890, 0}, ctx, std::nullopt, false)); lost_precision(false); // exp = 0, adjexp = 9
    EXPECT_EQ(any_text("-1.23456789E+9"), details::from_decimal::to_character(triple{-1, 0, 123456789, 1}, ctx, std::nullopt, false)); lost_precision(false); // exp = 1, adjexp = 9
    EXPECT_EQ(any_text("-0.00000123456789"), details::from_decimal::to_character(triple{-1, 0, 123456789, -14}, ctx, std::nullopt, false)); lost_precision(false); // adjexp = -6
    EXPECT_EQ(any_text("-1.23456789E-7"), details::from_decimal::to_character(triple{-1, 0, 123456789, -15}, ctx, std::nullopt, false)); lost_precision(false); // adjexp = -7
}

TEST_F(cast_to_string_test, from_decimal_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("340282366920938463463374607431768211455"), details::from_decimal::to_character(triple{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-340282366920938463463374607431768211455"), details::from_decimal::to_character(triple{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_decimal_large) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("1E+100"), details::from_decimal::to_character(triple{1, 0, 1, 100}, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-1E+100"), details::from_decimal::to_character(triple{-1, 0, 1, 100}, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_float) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("1"), details::from_float4::to_character(1.0f, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("0.1"), details::from_float4::to_character(0.1f, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("1.23457"), details::from_float4::to_character(1.234567f, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("3.40282E+38"), details::from_float4::to_character(std::numeric_limits<float>::max(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("1.17549E-38"), details::from_float4::to_character(std::numeric_limits<float>::min(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("Infinity"), details::from_float4::to_character(std::numeric_limits<float>::infinity(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-Infinity"), details::from_float4::to_character(-std::numeric_limits<float>::infinity(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float4::to_character(std::numeric_limits<float>::quiet_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float4::to_character(-std::numeric_limits<float>::quiet_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float4::to_character(std::numeric_limits<float>::signaling_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float4::to_character(-std::numeric_limits<float>::signaling_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("0"), details::from_float4::to_character(0.0F, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-0"), details::from_float4::to_character(-0.0F, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_double) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("1"), details::from_float8::to_character(1.0, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("0.1"), details::from_float8::to_character(0.1, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("1.23457"), details::from_float8::to_character(1.234567, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("1.79769E+308"), details::from_float8::to_character(std::numeric_limits<double>::max(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("2.22507E-308"), details::from_float8::to_character(std::numeric_limits<double>::min(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("Infinity"), details::from_float8::to_character(std::numeric_limits<double>::infinity(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-Infinity"), details::from_float8::to_character(-std::numeric_limits<double>::infinity(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float8::to_character(std::numeric_limits<double>::quiet_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float8::to_character(-std::numeric_limits<double>::quiet_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float8::to_character(std::numeric_limits<double>::signaling_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("NaN"), details::from_float8::to_character(-std::numeric_limits<double>::signaling_NaN(), ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("0"), details::from_float8::to_character(0.0, ctx, std::nullopt, false)); lost_precision(false);
    EXPECT_EQ(any_text("-0"), details::from_float8::to_character(-0.0, ctx, std::nullopt, false)); lost_precision(false);
}

TEST_F(cast_to_string_test, from_character) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_text("A  "), details::from_character::to_character("A", ctx, 3, true, false)); lost_precision(false);
    EXPECT_EQ(any_text("A "), details::from_character::to_character("A  ", ctx, 2, false, false)); lost_precision(true);
    EXPECT_EQ(any_text("A "), details::from_character::to_character("A  ", ctx, 2, false, true)); lost_precision(false);
    EXPECT_EQ(any_text("A "), details::from_character::to_character("A B", ctx, 2, true, true)); lost_precision(true);
}

}  // namespace jogasaki::executor::process::impl::expression
