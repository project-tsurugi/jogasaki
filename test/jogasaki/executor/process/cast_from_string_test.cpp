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

#include <takatori/decimal/triple.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/process.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/immediate.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/write.h>
#include <takatori/type/float.h>
#include <takatori/type/int.h>
#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>
#include <takatori/value/float.h>
#include <takatori/value/int.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/storage/configurable_provider.h>
#include <mizugaki/translator/shakujo_translator.h>

#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/details/common.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/to_field_type_kind.h>
#include <jogasaki/test_utils/make_triple.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/field_types.h>

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

class cast_from_string_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        details::ensure_decimal_context();
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

using namespace details::from_character;

TEST_F(cast_from_string_test, to_int) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int1("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int2("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), to_int4("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), to_int8("1", ctx));

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), to_int8("+1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -1}), to_int8("-1", ctx));
}

TEST_F(cast_from_string_test, to_int1_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 127}), to_int1("+127", ctx));  // 2^7-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -128}), to_int1("-128", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 127}), to_int1("128", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -128}), to_int1("-129", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int1("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int2_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 32767}), to_int2("+32767", ctx)); // 2^15-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -32768}), to_int2("-32768", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 32767}), to_int2("32768", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -32768}), to_int2("-32769", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int2("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int4_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 2147483647}), to_int4("+2147483647", ctx));  // 2^31-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -2147483648}), to_int4("-2147483648", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 2147483647}), to_int4("2147483648", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -2147483648}), to_int4("-2147483649", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("-Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_int4("NaN", ctx));
}

TEST_F(cast_from_string_test, to_int8_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 9'223'372'036'854'775'807L}), to_int8("+9223372036854775807", ctx));  // 2^63-1
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -9'223'372'036'854'775'807L-1}), to_int8("-9223372036854775808", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 9'223'372'036'854'775'807L}), to_int8("9223372036854775808", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -9'223'372'036'854'775'807L-1}), to_int8("-9223372036854775809", ctx));
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
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), to_boolean("true", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), to_boolean("T", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), to_boolean("false", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), to_boolean("F", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_boolean("", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_boolean("wrong text", ctx));
}

TEST_F(cast_from_string_test, to_decimal) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, 1}), to_decimal("1", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, -1}), to_decimal("-1", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("+0", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("-0", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 123456789, -4}}), to_decimal("-12345.67890", ctx));
}

TEST_F(cast_from_string_test, to_decimal_exceeding_digits) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("1E38")}), to_decimal("100000000000000000000000000000000000001", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-1E38")}), to_decimal("-100000000000000000000000000000000000001", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("10000000000000000000000000000000000001E1")}), to_decimal("100000000000000000000000000000000000011", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-10000000000000000000000000000000000001E1")}), to_decimal("-100000000000000000000000000000000000011", ctx));
}

TEST_F(cast_from_string_test, to_decimal_min_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("34028236692093846346337460743176821145E1")}), to_decimal("340282366920938463463374607431768211455", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-34028236692093846346337460743176821145E1")}), to_decimal("-340282366920938463463374607431768211455", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("34028236692093846346337460743176821145E1")}), to_decimal("340282366920938463463374607431768211456", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("-34028236692093846346337460743176821145E1")}), to_decimal("-340282366920938463463374607431768211456", ctx));
}

TEST_F(cast_from_string_test, to_decimal_exceeding_digits_hit_max) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("9.9999999999999999999999999999999999999E+24576")}), to_decimal("9.9999999999999999999999999999999999999E+24576", ctx)); // 38 digits
    EXPECT_EQ((any{std::in_place_type<triple>, make_triple("9.9999999999999999999999999999999999999E+24576")}), to_decimal("9.99999999999999999999999999999999999999E+24576", ctx)); // 39 digits
}

TEST_F(cast_from_string_test, to_decimal_large) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1, 100}}), to_decimal("1E100", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 1, 100}}), to_decimal("-1E100", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_decimal("1E+24577", ctx)); // emax + 1
}

TEST_F(cast_from_string_test, to_decimal_with_ps) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, 1}), to_decimal("1", ctx, 1, 0));
    EXPECT_EQ((any{std::in_place_type<triple>, -1}), to_decimal("-1", ctx, 1, 0));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("+0", ctx, 1, 0));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), to_decimal("-0", ctx, 1, 0));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, 3, 2));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 123456789, -4}}), to_decimal("-12345.67890", ctx, 10, 5));

    // truncate with scale 1
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.23", ctx, 2, 1));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.25", ctx, 2, 1));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12, -1}}), to_decimal("1.26", ctx, 2, 1));

    // extend scale to 5
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, 10, 5));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), to_decimal("1.23", ctx, std::nullopt, 5));

    // precision overflow by extending scale
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 999999, -5}}), to_decimal("12.34", ctx, 6, 5));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1234, -2}}), to_decimal("12.34", ctx, std::nullopt, 5));
    EXPECT_EQ((to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, std::nullopt)), to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, 2));
    EXPECT_EQ((to_decimal("99999999999999999999999999999999999.999", ctx, std::nullopt, std::nullopt)), to_decimal("123456789012345678901234567890123456.78", ctx, std::nullopt, 3));
}

TEST_F(cast_from_string_test, to_float) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<float>, 1.0}), to_float4("1.0", ctx));
    EXPECT_EQ((any{std::in_place_type<float>, 3.40282e+38}), to_float4("3.40282e+38", ctx));  // FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, -3.40282e+38}), to_float4("-3.40282e+38", ctx));  // -FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, 1.17550e-38}), to_float4("1.17550e-38", ctx));  // FLT_MIN(1.17549e-38) + alpha
    EXPECT_EQ((any{std::in_place_type<float>, -1.17550e-38}), to_float4("-1.17550e-38", ctx));  // -(FLT_MIN(1.17549e-38) + alpha)
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("3.40283e+38", ctx));  // FLT_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-3.40283e+38", ctx));  // -(FLT_MAX + alpha)
    {
        auto a = to_float4("1.17549e-38", ctx); // FLT_MIN (underflows)
        EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), a);
        auto f = a.to<float>();
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("-1.17549e-38", ctx); // -FLT_MIN (underflows)
        EXPECT_EQ((any{std::in_place_type<float>, -0.0f}), a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::signbit(f));
    }
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("inf", ctx));
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-inf", ctx));
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), to_float4("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), to_float4("-Infinity", ctx));
    {
        auto a = to_float4("NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("+NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float4("-NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // nan with diagnostic code is not supported
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("NaN0000", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("infi", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("Infinity_", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float4("++inf", ctx));
    }
}

TEST_F(cast_from_string_test, to_double) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<double>, 1.0}), to_float8("1.0", ctx));
    EXPECT_EQ((any{std::in_place_type<double>, 1.79769e+308}), to_float8("1.79769e+308", ctx)); // DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, -1.79769e+308}), to_float8("-1.79769e+308", ctx)); // -DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, 2.22508e-308}), to_float8("2.22508e-308", ctx)); // DBL_MIN(2.22507e-308) + alpha
    EXPECT_EQ((any{std::in_place_type<double>, -2.22508e-308}), to_float8("-2.22508e-308", ctx)); // -(DBL_MIN(2.22507e-308) + alpha)
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("1.79770e+308", ctx)); // DBL_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-1.79770e+308", ctx)); // -(DBL_MAX + alpha)
    {
        auto a = to_float8("2.22507e-308", ctx); // DBL_MIN - alpha (underflows)
        EXPECT_EQ((any{std::in_place_type<double>, 0.0}), a);
        auto d = a.to<double>();
        EXPECT_FALSE(std::signbit(d));
    }
    {
        auto a = to_float8("-2.22507e-308", ctx); // -(DBL_MIN - alpha) (underflows)
        EXPECT_EQ((any{std::in_place_type<double>, -0.0}), a);
        auto d = a.to<double>();
        EXPECT_TRUE(std::signbit(d));
    }
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("inf", ctx));
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-inf", ctx));
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), to_float8("Infinity", ctx));
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), to_float8("-Infinity", ctx));
    {
        auto a = to_float8("NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float8("+NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        auto a = to_float8("-NaN", ctx);
        ASSERT_TRUE(a);
        auto f = a.to<double>();
        EXPECT_TRUE(std::isnan(f));
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // nan with diagnostic code is not supported
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("NaN0000", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("infi", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("Infinity_", ctx));
        EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), to_float8("++inf", ctx));
    }
}
}
