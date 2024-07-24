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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
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
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/common.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/make_triple.h>

namespace jogasaki::executor::process::impl::expression {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace yugawara::binding;

using namespace testing;

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

class cast_from_string_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        details::ensure_decimal_context();
    }

    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

using namespace details::from_character;

void check_lost_precision(bool expected, evaluator_context& ctx) {
    EXPECT_EQ(expected, ctx.lost_precision());
    ctx.lost_precision(false);
}

#define lost_precision(arg) {   \
    SCOPED_TRACE("check_lost_precision");   \
    check_lost_precision(arg, ctx); \
}

TEST_F(cast_from_string_test, to_int) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int1("1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int2("1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int4("1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), to_int8("1", ctx)); lost_precision(false);

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), to_int8("1.5", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -1}), to_int8("-1.5", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 2}), to_int8("2.5", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -2}), to_int8("-2.5", ctx)); lost_precision(true);

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), to_int8("+1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -1}), to_int8("-1", ctx)); lost_precision(false);

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 0}), to_int8("+0", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 0}), to_int8("-0", ctx)); lost_precision(false);

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 20}), to_int8(" 20  ", ctx)); lost_precision(false);
}

TEST_F(cast_from_string_test, to_int1_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 127}), to_int1("+127", ctx)); lost_precision(false);  // 2^7-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -128}), to_int1("-128", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 127}), to_int1("128", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -128}), to_int1("-129", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int2_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 32767}), to_int2("+32767", ctx)); lost_precision(false); // 2^15-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -32768}), to_int2("-32768", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 32767}), to_int2("32768", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -32768}), to_int2("-32769", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int4_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 2147483647}), to_int4("+2147483647", ctx)); lost_precision(false);  // 2^31-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -2147483648}), to_int4("-2147483648", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 2147483647}), to_int4("2147483648", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -2147483648}), to_int4("-2147483649", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int8_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 9'223'372'036'854'775'807L}), to_int8("+9223372036854775807", ctx)); lost_precision(false);  // 2^63-1
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -9'223'372'036'854'775'807L-1}), to_int8("-9223372036854775808", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 9'223'372'036'854'775'807L}), to_int8("9223372036854775808", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -9'223'372'036'854'775'807L-1}), to_int8("-9223372036854775809", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int8("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int8("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int8("NaN", ctx));
}

TEST_F(cast_from_string_test, string_trim) {
    EXPECT_EQ(std::string_view{}, details::trim_spaces(""));
    EXPECT_EQ("ABC", details::trim_spaces(" ABC "));
    EXPECT_EQ("A  B", details::trim_spaces(" A  B "));
    EXPECT_EQ("ABC", details::trim_spaces("  ABC"));
    EXPECT_EQ("ABC", details::trim_spaces("ABC  "));
    EXPECT_EQ("ABC  ABC", details::trim_spaces("ABC  ABC "));
    EXPECT_EQ("ABC  ABC", details::trim_spaces(" ABC  ABC"));
}

TEST_F(cast_from_string_test, is_prefix) {
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("T", "true"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("TR", "true"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("TRU", "true"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("TRUE", "true"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("F", "false"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("FA", "false"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("FAL", "false"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("FALS", "false"));
    EXPECT_TRUE(details::is_prefix_of_case_insensitive("FALSE", "false"));

    EXPECT_FALSE(details::is_prefix_of_case_insensitive("TRUEX", "true"));
    EXPECT_FALSE(details::is_prefix_of_case_insensitive("", "true"));
}

TEST_F(cast_from_string_test, equals) {
    EXPECT_TRUE(details::equals_case_insensitive("", ""));
    EXPECT_TRUE(details::equals_case_insensitive("a", "a"));
    EXPECT_TRUE(details::equals_case_insensitive("abc", "aBc"));
    EXPECT_TRUE(details::equals_case_insensitive("abc", "abC"));

    EXPECT_FALSE(details::equals_case_insensitive("", "1"));
    EXPECT_FALSE(details::equals_case_insensitive("1", ""));
}

TEST_F(cast_from_string_test, bad_format) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("++1", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("", ctx));
}

TEST_F(cast_from_string_test, to_boolean) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), to_boolean("true", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), to_boolean("T", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), to_boolean("false", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), to_boolean("F", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_boolean("", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_boolean("wrong text", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), to_boolean(" true  ", ctx)); lost_precision(false);
}

TEST_F(cast_from_string_test, to_decimal) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, 1}), to_decimal("1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, -1}), to_decimal("-1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("+0", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("-0", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1, -1}}), to_decimal(".1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 1, -1}}), to_decimal("-.1", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 123456789, -4}}), to_decimal("-12345.67890", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("NaN", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("sNaN", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("bad", ctx));
}

TEST_F(cast_from_string_test, to_decimal_exceeding_digits) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("1E38")}), to_decimal("100000000000000000000000000000000000001", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-1E38")}), to_decimal("-100000000000000000000000000000000000001", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("10000000000000000000000000000000000001E1")}), to_decimal("100000000000000000000000000000000000011", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-10000000000000000000000000000000000001E1")}), to_decimal("-100000000000000000000000000000000000011", ctx)); lost_precision(true);
}

TEST_F(cast_from_string_test, to_decimal_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("34028236692093846346337460743176821145E1")}), to_decimal("340282366920938463463374607431768211455", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-34028236692093846346337460743176821145E1")}), to_decimal("-340282366920938463463374607431768211455", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("34028236692093846346337460743176821145E1")}), to_decimal("340282366920938463463374607431768211456", ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-34028236692093846346337460743176821145E1")}), to_decimal("-340282366920938463463374607431768211456", ctx)); lost_precision(true);
}

TEST_F(cast_from_string_test, to_decimal_exceeding_digits_hit_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("9.9999999999999999999999999999999999999E+24576")}), to_decimal("9.9999999999999999999999999999999999999E+24576", ctx)); lost_precision(false); // 38 digits
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("9.9999999999999999999999999999999999999E+24576")}), to_decimal("9.99999999999999999999999999999999999999E+24576", ctx)); lost_precision(true); // 39 digits
}

TEST_F(cast_from_string_test, to_decimal_large) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1, 100}}), to_decimal("1E100", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 1, 100}}), to_decimal("-1E100", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("1E+24577", ctx)); // emax + 1
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("0.0E+1000000000000", ctx)); lost_precision(false) ;// zero is the exception for too large exp
}

TEST_F(cast_from_string_test, to_decimal_with_ps) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, 1}), to_decimal("1", ctx, 1, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, -1}), to_decimal("-1", ctx, 1, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("+0", ctx, 1, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("-0", ctx, 1, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, 3, 2)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 123456789, -4}}), to_decimal("-12345.67890", ctx, 10, 5)); lost_precision(false);

    // truncate with scale 1
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.23", ctx, 2, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.25", ctx, 2, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.26", ctx, 2, 1)); lost_precision(true);

    // extend scale to 5
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, 10, 5)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, std::nullopt, 5)); lost_precision(false);

    // precision overflow by extending scale
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 999999, -5}}), to_decimal("12.34", ctx, 6, 5)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1234, -2}}), to_decimal("12.34", ctx, std::nullopt, 5)); lost_precision(false);
    EXPECT_EQ((to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, std::nullopt)), to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, 2)); lost_precision(false);
    EXPECT_EQ((to_decimal("99999999999999999999999999999999999.999", ctx, std::nullopt, std::nullopt)), to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, 3)); lost_precision(true);
}

TEST_F(cast_from_string_test, to_float) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<float>, 1.0}), to_float4("1.0", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 3.40282e+38}), to_float4("3.40282e+38", ctx)); lost_precision(false);  // FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, -3.40282e+38}), to_float4("-3.40282e+38", ctx)); lost_precision(false);  // -FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, 1.17550e-38}), to_float4("1.17550e-38", ctx)); lost_precision(false);  // FLT_MIN(1.17549e-38) + alpha
    EXPECT_EQ((any{std::in_place_type<float>, -1.17550e-38}), to_float4("-1.17550e-38", ctx)); lost_precision(false);  // -(FLT_MIN(1.17549e-38) + alpha)
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("3.40283e+38", ctx)); lost_precision(false);  // FLT_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-3.40283e+38", ctx)); lost_precision(false);  // -(FLT_MAX + alpha)
    EXPECT_EQ((any{std::in_place_type<float>, 0.0F}), to_float4("0", ctx)); lost_precision(false);
    {
        auto a = to_float4("0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), a);
        auto f = a.to<float>();
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("0.0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), a);
        auto f = a.to<float>();
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("-0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<float>, -0.0f}), a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::signbit(f));
    }
    {
        auto a = to_float4("-0.0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<float>, -0.0f}), a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::signbit(f));
    }
    {
        auto a = to_float4("1.17549e-38", ctx); lost_precision(false); // FLT_MIN (underflows)
        EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), a);
        auto f = a.to<float>();
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("-1.17549e-38", ctx); lost_precision(false); // -FLT_MIN (underflows)
        EXPECT_EQ((any{std::in_place_type<float>, -0.0f}), a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::signbit(f));
    }
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("inf", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-inf", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("Infinity", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-Infinity", ctx)); lost_precision(false);
    {
        auto a = to_float4("NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("+NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // even if minus sign is specified, it is ignored
        auto a = to_float4("-NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // nan with diagnostic code is not supported
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("NaN0", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("NaN0000", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("infi", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("Infinity_", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("++inf", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("sNaN", ctx));
    }
}

TEST_F(cast_from_string_test, to_double) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<double>, 1.0}), to_float8("1.0", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 1.79769e+308}), to_float8("1.79769e+308", ctx)); lost_precision(false); // DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, -1.79769e+308}), to_float8("-1.79769e+308", ctx)); lost_precision(false); // -DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, 2.22508e-308}), to_float8("2.22508e-308", ctx)); lost_precision(false); // DBL_MIN(2.22507e-308) + alpha
    EXPECT_EQ((any{std::in_place_type<double>, -2.22508e-308}), to_float8("-2.22508e-308", ctx)); lost_precision(false); // -(DBL_MIN(2.22507e-308) + alpha)
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("1.79770e+308", ctx)); lost_precision(false); // DBL_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-1.79770e+308", ctx)); lost_precision(false); // -(DBL_MAX + alpha)
    {
        auto a = to_float8("0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<double>, 0.0}), a);
        auto d = a.to<double>();
        EXPECT_FALSE(std::signbit(d));
    }
    {
        auto a = to_float8("0.0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<double>, 0.0}), a);
        auto d = a.to<double>();
        EXPECT_FALSE(std::signbit(d));
    }
    {
        auto a = to_float8("-0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<double>, -0.0}), a);
        auto d = a.to<double>();
        EXPECT_TRUE(std::signbit(d));
    }
    {
        auto a = to_float8("-0.0", ctx); lost_precision(false);
        EXPECT_EQ((any{std::in_place_type<double>, -0.0}), a);
        auto d = a.to<double>();
        EXPECT_TRUE(std::signbit(d));
    }
    {
        auto a = to_float8("2.22507e-308", ctx); lost_precision(false); // DBL_MIN - alpha (underflows)
        EXPECT_EQ((any{std::in_place_type<double>, 0.0}), a);
        auto d = a.to<double>();
        EXPECT_FALSE(std::signbit(d));
    }
    {
        auto a = to_float8("-2.22507e-308", ctx); lost_precision(false); // -(DBL_MIN - alpha) (underflows)
        EXPECT_EQ((any{std::in_place_type<double>, -0.0}), a);
        auto d = a.to<double>();
        EXPECT_TRUE(std::signbit(d));
    }
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("inf", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-inf", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("Infinity", ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-Infinity", ctx)); lost_precision(false);
    {
        auto a = to_float8("NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float8("+NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // even if minus sign is specified, it is ignored
        auto a = to_float8("-NaN", ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // nan with diagnostic code is not supported
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("NaN0", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("NaN0000", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("infi", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("Infinity_", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("++inf", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("sNaN", ctx));
    }
}

TEST_F(cast_from_string_test, to_decimal_long_string) {
    // verify very long string hits format error
    constexpr std::size_t repeats = 100000;
    evaluator_context ctx{&resource_};
    std::stringstream ss{};
    for(std::size_t i=0; i < repeats; ++i) {
        ss << "1234567890";
    }
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal(ss.str(), ctx));
}

TEST_F(cast_from_string_test, to_decimal_long_string3) {
    // mpdecimal can accept very long string e.g. 100MB
    // TODO what is the realistic max for acceptable string length for decimals?
    constexpr std::size_t repeats = 10000000;
    evaluator_context ctx{&resource_};
    {
        std::stringstream ss{};
        ss << "0.";
        for(std::size_t i=0; i < repeats; ++i) {
            ss << "0000000000";
        }
        ss << "1E100000000";
        EXPECT_EQ((any{std::in_place_type<triple>, triple{1,0,1,-1}}), to_decimal(ss.str(), ctx));
    }
}

TEST_F(cast_from_string_test, to_octet) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<accessor::binary>, accessor::binary{"\x00"sv}}), to_octet("00", ctx, std::nullopt, false, false)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<accessor::binary>, accessor::binary{"\x00"sv}}), to_octet(" 00 ", ctx, std::nullopt, false, false)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<accessor::binary>, accessor::binary{""sv}}), to_octet("", ctx, std::nullopt, false, false)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<accessor::binary>, accessor::binary{"\xff"sv}}), to_octet("fF", ctx, std::nullopt, false, false)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_octet("0", ctx, std::nullopt, false, false));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_octet(" bad string", ctx, std::nullopt, false, false));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_octet(" 0  1 ", ctx, std::nullopt, false, false));
    EXPECT_EQ((any{std::in_place_type<accessor::binary>, accessor::binary{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F\x20"}}), to_octet("000102030405060708090a0b0c0d0e0f101112131415161718191A1B1C1D1E1F20"sv, ctx, std::nullopt, false, false)); lost_precision(false);
}

}

