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
#include <cfenv>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/details/common.h>
#include <jogasaki/executor/expr/details/constants.h>
#include <jogasaki/executor/expr/details/decimal_context.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/make_triple.h>
#include <jogasaki/test_utils/to_field_type_kind.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::executor::expr {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace jogasaki::executor::expr::details;

using namespace testing;

using takatori::decimal::triple;
using accessor::text;

using namespace details;

class cast_between_numerics_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        executor::expr::details::ensure_decimal_context();
    }

    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};

    template <class Source, class Target, class RangeTarget>
    void test_int_narrowing(std::function<any(Source, evaluator_context&)> fn);

    template <class Source, class Target, class RangeTarget>
    void test_int_widening(std::function<any(Source, evaluator_context&)> fn);

    template <class Source, class RangeTarget, class RangeTargetUnsigned>
    void test_int_to_decimal(std::function<any(Source, evaluator_context&, std::optional<std::size_t>, std::optional<std::size_t>)> fn);

    template <class Target, class RangeTarget = Target>
    void test_decimal_to_int(std::function<any(triple, evaluator_context&)> fn);
};

any any_triple(
    std::int64_t coefficient,
    std::int32_t exponent = 0
) {
    return any{std::in_place_type<triple>, triple{coefficient, exponent}};
}

any any_triple(
    std::int64_t sign,
    std::uint64_t coefficient_high,
    std::uint64_t coefficient_low,
    std::int32_t exponent
) {
    return any{std::in_place_type<triple>, triple{sign, coefficient_high, coefficient_low, exponent}};
}

any any_error(
    error_kind arg
) {
    return any{std::in_place_type<error>, arg};
}

// get max of T as E
template <class T, class E = T>
E int_max() {
    return static_cast<E>(std::numeric_limits<T>::max());
}

// get min of T as E
template <class T, class E = T>
E int_min() {
    return static_cast<E>(std::numeric_limits<T>::min());
}

// get max of T + 1 as string
template <class T, class E = std::make_unsigned_t<T>>
std::string int_max_plus_one_str() {
    return std::to_string(static_cast<E>(std::numeric_limits<T>::max())+1);
}

// get min of T - 1 as string
template <class T, class E = std::make_unsigned_t<T>>
std::string int_min_minus_one_str() {
    return "-"+std::to_string(static_cast<E>(-(std::numeric_limits<T>::min()+1))+2);
}

// get min of T as E with sign reversed
template <class T, class E>
E int_min_positive() {
    return static_cast<E>(-(std::numeric_limits<T>::min()+1))+1;
}

void check_lost_precision(bool expected, evaluator_context& ctx) {
    EXPECT_EQ(expected, ctx.lost_precision());
    ctx.lost_precision(false);
}

#define lost_precision(arg) {   \
    SCOPED_TRACE("check_lost_precision");   \
    check_lost_precision(arg, ctx); \
}

template <class Source, class RangeTarget, class RangeTargetUnsigned>
void cast_between_numerics_test::test_int_to_decimal(std::function<any(Source, evaluator_context&, std::optional<std::size_t>, std::optional<std::size_t>)> fn) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_triple(1), fn(1, ctx, std::nullopt, std::nullopt));
    EXPECT_EQ(any_triple(1, 0, 123000, -3), fn(123, ctx, 6, 3));
    EXPECT_EQ(any_triple(0, 0, 0, -3), fn(0, ctx, 5, 3));
    EXPECT_EQ(any_triple(1, 0, 10, 0), fn(10, ctx, 5, 0));

    EXPECT_EQ(any_triple(1, 0, 99, 0), fn(100, ctx, 2, 0));
    EXPECT_EQ(any_error(error_kind::unsupported), fn(10, ctx, 1, std::nullopt));
    EXPECT_EQ(any_triple(1, 0, 99999, -3), fn(123, ctx, 5, 3));
    EXPECT_EQ(any_triple(1, 0, 123000, -3), fn(123, ctx, std::nullopt, 3));
    EXPECT_EQ(any_triple(1, 0, int_max<RangeTarget, Source>(), 0), fn(int_max<RangeTarget, Source>(), ctx, std::nullopt, std::nullopt));
    EXPECT_EQ(any_triple(-1, 0, int_min_positive<RangeTarget, RangeTargetUnsigned>(), 0), fn(int_min<RangeTarget, Source>(), ctx, std::nullopt, std::nullopt));
}

TEST_F(cast_between_numerics_test, int4_to_decimal) {
    test_int_to_decimal<std::int32_t, std::int32_t, std::uint32_t>([](std::int32_t src, evaluator_context& ctx, std::optional<std::size_t> precision, std::optional<std::size_t> scale){
        return from_int4::to_decimal(src, ctx, precision, scale);
    });
}

TEST_F(cast_between_numerics_test, int8_to_decimal) {
    test_int_to_decimal<std::int64_t, std::int64_t, std::uint64_t>([](std::int64_t src, evaluator_context& ctx, std::optional<std::size_t> precision, std::optional<std::size_t> scale){
        return from_int8::to_decimal(src, ctx, precision, scale);
    });
}

TEST_F(cast_between_numerics_test, verify_make_triple) {
    // verify the test utility to generate triples correctly
    EXPECT_EQ((triple{1, 0, 1, 0}), make_triple("1"));
    EXPECT_EQ((triple{0, 0, 0, 0}), make_triple("0"));
    EXPECT_EQ((triple{0, 0, 0, 0}), make_triple("-0"));
    EXPECT_EQ((triple{1, 0, 10, 0}), make_triple("10"));
    EXPECT_EQ((triple{1, 0, 123, 0}), make_triple("123"));

    EXPECT_EQ((triple{1, 0, 149, -2}), make_triple("1.49"));
    EXPECT_EQ((triple{1, 0, 150, -2}), make_triple("1.50"));
    EXPECT_EQ((triple{1, 0, 250, -2}), make_triple("2.50"));
    EXPECT_EQ((triple{1, 0, 251, -2}), make_triple("2.51"));
    EXPECT_EQ((triple{1, 0, 349, -2}), make_triple("3.49"));
    EXPECT_EQ((triple{1, 0, 350, -2}), make_triple("3.50"));

    // make_triple throws if digits exceed 38
    ASSERT_THROW({make_triple("1234567890123456789012345678901234567890");}, std::invalid_argument);
    ASSERT_THROW({make_triple("A");}, std::invalid_argument);
    ASSERT_THROW({make_triple("0x1");}, std::invalid_argument);
    ASSERT_THROW({make_triple("");}, std::invalid_argument);
    ASSERT_THROW({make_triple("Infinity");}, std::invalid_argument);
    ASSERT_THROW({make_triple("nan");}, std::invalid_argument);
    ASSERT_THROW({make_triple("1E+2147483648");}, std::domain_error); // exp=INT_MAX+1
    ASSERT_THROW({make_triple("1E-2147483649");}, std::domain_error); // exp=INT_MIN-1
}

TEST_F(cast_between_numerics_test, verify_triples_comparison) {
    EXPECT_EQ((triple{1, 0, 15, -1}), make_triple("1.5"));
    EXPECT_EQ((triple{1, 0, 150, -2}), make_triple("1.50"));
    EXPECT_NE(make_triple("1.50"), make_triple("1.5"));
}

template <class Target, class RangeTarget>
void cast_between_numerics_test::test_decimal_to_int(std::function<any(triple, evaluator_context&)> fn) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<Target>, 1}), fn(make_triple("1"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(make_triple("0"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, 10}), fn(make_triple("10"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, 123}), fn(make_triple("123"), ctx)); lost_precision(false);

    // the numbers under decimal point will be truncated
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(make_triple("0.1"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 1}), fn(make_triple("1.5"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 2}), fn(make_triple("2.5"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(make_triple("-0.1"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, -1}), fn(make_triple("-1.5"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, -2}), fn(make_triple("-2.5"), ctx)); lost_precision(true);

    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget>()}), fn(make_triple(std::to_string(int_max<RangeTarget, std::int64_t>())), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget>()}), fn(make_triple(int_max_plus_one_str<RangeTarget>()), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget>()}), fn(make_triple(std::to_string(int_min<RangeTarget, std::int64_t>())), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget>()}), fn(make_triple(int_min_minus_one_str<RangeTarget>()), ctx)); lost_precision(true);

    // extreme triple
    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget>()}), fn(triple_max, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget>()}), fn(triple_min, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(make_triple("99999999999999999999999999999999999999E-38"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(make_triple("-99999999999999999999999999999999999999E-38"), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(triple{1, 0, 1, decimal_context_emin}, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(triple{-1, 0, 1, decimal_context_emin}, ctx)); lost_precision(true);
}

TEST_F(cast_between_numerics_test, decimal_to_int1) {
    test_decimal_to_int<std::int32_t, std::int8_t>([](triple src, evaluator_context& ctx){
        return from_decimal::to_int1(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, decimal_to_int2) {
    test_decimal_to_int<std::int32_t, std::int16_t>([](triple src, evaluator_context& ctx){
        return from_decimal::to_int2(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, decimal_to_int4) {
    test_decimal_to_int<std::int32_t, std::int32_t>([](triple src, evaluator_context& ctx){
        return from_decimal::to_int4(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, decimal_to_int8) {
    test_decimal_to_int<std::int64_t, std::int64_t>([](triple src, evaluator_context& ctx){
        return from_decimal::to_int8(src, ctx);
    });
}

template <class Source, class Target, class RangeTarget>
void cast_between_numerics_test::test_int_narrowing(std::function<any(Source, evaluator_context&)> fn) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(0, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, 1}), fn(1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, -1}), fn(-1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget, Target>()}), fn(int_max<RangeTarget, Source>(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget, Target>()}), fn(int_min<RangeTarget, Source>(), ctx)); lost_precision(false);

    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget, Target>()}), fn(int_max<RangeTarget, Source>()+1, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget, Target>()}), fn(int_min<RangeTarget, Source>()-1, ctx)); lost_precision(true);
}

TEST_F(cast_between_numerics_test, int8_to_int1) {
    test_int_narrowing<std::int64_t, std::int32_t, std::int8_t>([](std::int64_t src, evaluator_context& ctx) {
        return from_int8::to_int1(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int8_to_int2) {
    test_int_narrowing<std::int64_t, std::int32_t, std::int16_t>([](std::int64_t src, evaluator_context& ctx) {
        return from_int8::to_int2(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int8_to_int4) {
    test_int_narrowing<std::int64_t, std::int32_t, std::int32_t>([](std::int64_t src, evaluator_context& ctx) {
        return from_int8::to_int4(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int4_to_int2) {
    test_int_narrowing<std::int32_t, std::int32_t, std::int16_t>([](std::int32_t src, evaluator_context& ctx) {
        return from_int4::to_int2(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int4_to_int1) {
    test_int_narrowing<std::int32_t, std::int32_t, std::int8_t>([](std::int32_t src, evaluator_context& ctx) {
        return from_int4::to_int1(src, ctx);
    });
}

template <class Source, class Target, class RangeTarget>
void cast_between_numerics_test::test_int_widening(std::function<any(Source, evaluator_context&)> fn) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<Target>, 0}), fn(0, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, 1}), fn(1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, -1}), fn(-1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_max<RangeTarget, Target>()}), fn(int_max<RangeTarget, Source>(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Target>, int_min<RangeTarget, Target>()}), fn(int_min<RangeTarget, Source>(), ctx)); lost_precision(false);
}

TEST_F(cast_between_numerics_test, int1_to_int8) {
    test_int_widening<std::int32_t, std::int64_t, std::int8_t>([](std::int32_t src, evaluator_context& ctx) {
        return from_int4::to_int8(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int2_to_int8) {
    test_int_widening<std::int32_t, std::int64_t, std::int16_t>([](std::int32_t src, evaluator_context& ctx) {
        return from_int4::to_int8(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int4_to_int8) {
    test_int_widening<std::int32_t, std::int64_t, std::int32_t>([](std::int32_t src, evaluator_context& ctx) {
        return from_int4::to_int8(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, decimal_to_float4) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<float>, 1}), from_decimal::to_float4(make_triple("1"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 0}), from_decimal::to_float4(make_triple("0"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -1}), from_decimal::to_float4(make_triple("-1"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 123}), from_decimal::to_float4(make_triple("123"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 1.23}), from_decimal::to_float4(make_triple("1.23"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 100}), from_decimal::to_float4(make_triple("100"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 1000}), from_decimal::to_float4(make_triple("1E+3"), ctx)); lost_precision(false);

    // verify (approx.) boundaries
    EXPECT_EQ((any{std::in_place_type<float>, 3.40282e+38}), from_decimal::to_float4(make_triple("3.40282e+38"), ctx)); lost_precision(false); // FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), from_decimal::to_float4(make_triple("3.4029e+38"), ctx));  lost_precision(false); // FLT_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<float>, -3.40282e+38}), from_decimal::to_float4(make_triple("-3.40282e+38"), ctx));  lost_precision(false); // -FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), from_decimal::to_float4(make_triple("-3.4029e+38"), ctx));  lost_precision(false); // -FLT_MAX - alpha
    EXPECT_EQ((any{std::in_place_type<float>, 1.17550e-38}), from_decimal::to_float4(make_triple("1.17550e-38"), ctx));  lost_precision(false); // FLT_MIN + alpha (because FLT_MIN underflows)
    {
        // FLT_MIN - alpha will be +0.0
        auto a = from_decimal::to_float4(make_triple("1.1754e-38"), ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_EQ(0, f);
        EXPECT_FALSE(std::signbit(f));
    }
    {
        // - FLT_MIN + alpha will be -0.0
        auto a = from_decimal::to_float4(make_triple("-1.1754e-38"), ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto f = a.to<float>();
        EXPECT_EQ(0, f);
        EXPECT_TRUE(std::signbit(f));
    }
}

TEST_F(cast_between_numerics_test, decimal_to_float8) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<double>, 1}), from_decimal::to_float8(make_triple("1"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 0}), from_decimal::to_float8(make_triple("0"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -1}), from_decimal::to_float8(make_triple("-1"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 123}), from_decimal::to_float8(make_triple("123"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 1.23}), from_decimal::to_float8(make_triple("1.23"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 100}), from_decimal::to_float8(make_triple("100"), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 1000}), from_decimal::to_float8(make_triple("1E+3"), ctx)); lost_precision(false);

    // verify (approx.) boundaries
    EXPECT_EQ((any{std::in_place_type<double>, 1.79769e+308}), from_decimal::to_float8(make_triple("1.79769e+308"), ctx));  lost_precision(false); // DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), from_decimal::to_float8(make_triple("1.7977e+308"), ctx));  lost_precision(false); // DBL_MAX + alpha
    EXPECT_EQ((any{std::in_place_type<double>, -1.79769e+308}), from_decimal::to_float8(make_triple("-1.79769e+308"), ctx));  lost_precision(false); // - DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), from_decimal::to_float8(make_triple("-1.7977e+308"), ctx));  lost_precision(false); // - DBL_MAX - alpha
    EXPECT_EQ((any{std::in_place_type<double>, 2.22508e-308}), from_decimal::to_float8(make_triple("2.22508e-308"), ctx));  lost_precision(false); // DBL_MIN + alpha (because DBL_MIN underflows)
    EXPECT_EQ((any{std::in_place_type<double>, 0.0}), from_decimal::to_float8(make_triple("2.22506e-308"), ctx));  lost_precision(false); // DBL_MIN - alpha
    EXPECT_EQ((any{std::in_place_type<double>, -0.0}), from_decimal::to_float8(make_triple("-2.22506e-308"), ctx));  lost_precision(false); // negative (DBL_MIN - alpha)
    {
        // DBL_MIN - alpha will be +0.0
        auto a = from_decimal::to_float8(make_triple("2.22506e-308"), ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto d = a.to<double>();
        EXPECT_EQ(0, d);
        EXPECT_FALSE(std::signbit(d));
    }
    {
        // - DBL_MIN + alpha will be +0.0
        auto a = from_decimal::to_float8(make_triple("-2.22506e-308"), ctx); lost_precision(false);
        ASSERT_TRUE(a);
        auto d = a.to<double>();
        EXPECT_EQ(0, d);
        EXPECT_TRUE(std::signbit(d));
    }
}

TEST_F(cast_between_numerics_test, float4_to_decimal) {
    // note: testing lost precision here is a little vague. See the comment for float8_to_decimal
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any_triple(1, 0, 1000, -3)), from_float4::to_decimal(1.0f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(0, 0, 0, -3)), from_float4::to_decimal(0.0f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(0, 0, 0, -3)), from_float4::to_decimal(-0.0f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(-1, 0, 1000, -3)), from_float4::to_decimal(-1.0f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 10000, -3)), from_float4::to_decimal(10.0f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 1230, -3)), from_float4::to_decimal(1.23f, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 123, -2)), from_float4::to_decimal(1.23f, ctx, 5, 2)); lost_precision(false);

    // verify on float min/max
    {
        auto a = from_float4::to_decimal(3.40282e+38f, ctx, {}, {}); lost_precision(false);
        EXPECT_TRUE(a);
        decimal::Decimal d{a.to<triple>()};
        EXPECT_EQ(1, d.sign());
        EXPECT_LE(38, d.adjexp());
    }
    {
        auto a = from_float4::to_decimal(-3.40282e+38f, ctx, {}, {}); lost_precision(false);
        decimal::Decimal d{a.to<triple>()};
        EXPECT_EQ(-1, d.sign());
        EXPECT_LE(38, d.adjexp());
    }
    EXPECT_EQ((any_triple(1, 0, 117549, -43)), from_float4::to_decimal(1.17549e-38f, ctx, {}, {})); lost_precision(false);
    EXPECT_EQ((any_triple(0, 0, 0, 0)), from_float4::to_decimal(1.17549e-38f, ctx, {}, 0)); lost_precision(true);

    // verify on decimal min/max
    EXPECT_FALSE(from_float4::to_decimal(1.0e+37F, ctx, {}, 0).error()); lost_precision(false); // 9999...(38 digits) - alpha
    EXPECT_FALSE(from_float4::to_decimal(1.1e+38f, ctx, 38, 0).error()); lost_precision(true); // 9999...(38 digits) + alpha

    // special values
    EXPECT_EQ((any{std::in_place_type<triple>, triple_max}), from_float4::to_decimal(std::numeric_limits<float>::infinity(), ctx, {}, {})); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple_min}), from_float4::to_decimal(-std::numeric_limits<float>::infinity(), ctx, {}, {})); lost_precision(true);
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), from_float4::to_decimal(std::numeric_limits<float>::quiet_NaN(), ctx, {}, {}));
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), from_float4::to_decimal(std::numeric_limits<float>::signaling_NaN(), ctx, {}, {}));
}

TEST_F(cast_between_numerics_test, float8_to_decimal) {
    // note: testing lost precision here is a little vague since approx. to exact almost always has binary to decimal conversion error.
    // For example, 0.3 in float8 is not precise and it's actually something like 0.300000011920928955078125.
    // So if we can convert 0.3 in float8 to triple{1, 0, 3, -1}, it's not exact conversion.
    // To avoid the confusion, we block the implicit conversion from approx. to exact.
    // Lost precision here means only the case going over max/min boundery and saturated to max/min.
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any_triple(1, 0, 1000, -3)), from_float8::to_decimal(1, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(0, 0, 0, -3)), from_float8::to_decimal(0.0, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(0, 0, 0, -3)), from_float8::to_decimal(-0.0, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(-1, 0, 1000, -3)), from_float8::to_decimal(-1, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 10000, -3)), from_float8::to_decimal(10, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 1230, -3)), from_float8::to_decimal(1.23, ctx, 5, 3)); lost_precision(false);
    EXPECT_EQ((any_triple(1, 0, 123, -2)), from_float8::to_decimal(1.23, ctx, 5, 2)); lost_precision(false);

    // verify on double min/max
    {
        auto a = from_float8::to_decimal(1.79769e+308, ctx, {}, {}); lost_precision(false); // DBL_MAX
        EXPECT_TRUE(a);
        decimal::Decimal d{a.to<triple>()};
        EXPECT_EQ(1, d.sign());
        EXPECT_LE(308, d.adjexp());
    }
    {
        auto a = from_float8::to_decimal(-1.79769e+308, ctx, {}, {}); lost_precision(false); // -DBL_MAX
        decimal::Decimal d{a.to<triple>()};
        EXPECT_EQ(-1, d.sign());
        EXPECT_LE(308, d.adjexp());
    }
    EXPECT_EQ((any_triple(1, 0, 222507, -313)), from_float8::to_decimal(2.22507e-308, ctx, {}, {})); lost_precision(false);  // DBL_MIN
    EXPECT_EQ((any_triple(0, 0, 0, 0)), from_float8::to_decimal(2.22507e-308, ctx, {}, 0)); lost_precision(true);  // DBL_MIN

    // verify on decimal min/max
    EXPECT_FALSE(from_float8::to_decimal(1.0e+37, ctx, {}, 0).error()); lost_precision(false); // 9999...(38 digits) - alpha
    EXPECT_FALSE(from_float8::to_decimal(1.1e+38, ctx, 38, 0).error()); lost_precision(true); // 9999...(38 digits) + alpha

    // special values
    EXPECT_EQ((any{std::in_place_type<triple>, triple_max}), from_float8::to_decimal(std::numeric_limits<double>::infinity(), ctx, {}, {})); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple_min}), from_float8::to_decimal(-std::numeric_limits<double>::infinity(), ctx, {}, {})); lost_precision(true);
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), from_float8::to_decimal(std::numeric_limits<double>::quiet_NaN(), ctx, {}, {}));
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), from_float8::to_decimal(std::numeric_limits<double>::signaling_NaN(), ctx, {}, {}));
}

// next integral value representable in the float type (this is not simply +1/-1 of the original value since floats have gaps between representable values)
template <class Float>
Float next_int_representable(Float arg, bool toward_minus_infinity = false) {
    auto truncated = std::trunc(arg);
    auto next = std::nextafter(arg, toward_minus_infinity ? -std::numeric_limits<Float>::infinity() : std::numeric_limits<Float>::infinity());
    return toward_minus_infinity ? std::floor(next) : std::ceil(next);
}

template <kind IntKind, kind FloatKind>
void test_verify_constants() {
    // be careful in testing on constexpr template constants - updating constants file and recompile sometime fails to
    // reflect the update. I had to recompile the testcase file to ensure updates reflected.
    std::cerr << std::setprecision(30);
    {
        auto c = max_integral_float_convertible_to_int_source<IntKind, FloatKind>;
        auto x = static_cast<runtime_type<FloatKind>>(c);
        EXPECT_LT(0, x);
        std::feclearexcept(FE_ALL_EXCEPT);
        auto y = static_cast<runtime_type<IntKind>>(x);
        EXPECT_FALSE(std::fetestexcept(FE_INVALID)) << std::fetestexcept(FE_ALL_EXCEPT);
        EXPECT_EQ(c, y);
        std::cerr << "constant c=" << c << " casted to " << FloatKind << " x=" << x << " casted back to " << IntKind << " y=" << y << std::endl;
    }
    if constexpr(max_integral_float_convertible_to_int_source<IntKind, FloatKind> != std::numeric_limits<runtime_type<IntKind>>::max()) {
        auto x = next_int_representable(max_integral_float_convertible_to_int<IntKind, FloatKind>);
        EXPECT_LT(0, x);
        std::feclearexcept(FE_ALL_EXCEPT);
        auto y = static_cast<runtime_type<IntKind>>(x);
#ifndef NDEBUG
        // what we want to verify here is something bad happens as we go over the max limit, but the following
        // check doesn't reliably work on Release/RelWithDebInfo builds, so just do it on Debug build.
        EXPECT_TRUE(std::fetestexcept(FE_INVALID)) << std::fetestexcept(FE_ALL_EXCEPT);
        EXPECT_GT(0, y);
#endif
        // Even on release builds, you can manually verify the round-trip is failing by the message below
        std::cerr << "next int representable " << FloatKind << " x=" << x << " casted back to " << IntKind << " y=" << y << std::endl;
    }
    {
        auto c = min_integral_float_convertible_to_int_source<IntKind, FloatKind>;
        auto x = static_cast<runtime_type<FloatKind>>(c);
        EXPECT_GT(0, x);
        std::feclearexcept(FE_ALL_EXCEPT);
        auto y = static_cast<runtime_type<IntKind>>(x);
        EXPECT_FALSE(std::fetestexcept(FE_INVALID)) << std::fetestexcept(FE_ALL_EXCEPT);
        EXPECT_EQ(c, y);
        std::cerr << "constant c=" << c << " casted to " << FloatKind << " x=" << x << " casted back to " << IntKind << " y=" << y << std::endl;
    }
    // as int min is used for the constant, we cannot test its - 1.
}

TEST_F(cast_between_numerics_test, verify_float4_int4_constants) {
    // verify int4 max - 64 is the maximum that is safe to convert between int4/float4
    test_verify_constants<kind::int4, kind::float4>();
}

TEST_F(cast_between_numerics_test, verify_float4_int8_constants) {
    // verify int8 max - 256G is the maximum that is safe to convert between int8/float4
    test_verify_constants<kind::int8, kind::float4>();
}

TEST_F(cast_between_numerics_test, verify_float8_int4_constants) {
    test_verify_constants<kind::int4, kind::float8>();
}

TEST_F(cast_between_numerics_test, verify_float8_int8_constants) {
    // verify int8 max - 256G is the maximum that is safe to convert between int8/float8
    test_verify_constants<kind::int8, kind::float8>();
}

template <class Float, class Int, class RangeInt>
void test_float_to_int_common(evaluator_context& ctx, std::function<any(Float, evaluator_context&)> fn) {
    EXPECT_EQ((any{std::in_place_type<Int>, 0}), fn(0.0f, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, 0}), fn(-0.0f, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, 1}), fn(1.0f, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, -1}), fn(-1.0f, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, 10}), fn(10.0f, ctx)); lost_precision(false);

    // right to decimal point are truncated (round down)
    EXPECT_EQ((any{std::in_place_type<Int>, 1}), fn(1.5f, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, 2}), fn(2.5f, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, -1}), fn(-1.5f, ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, -2}), fn(-2.5f, ctx)); lost_precision(true);

    // verify on floats min/max
    EXPECT_EQ((any{std::in_place_type<Int>, std::numeric_limits<RangeInt>::max()}), fn(std::numeric_limits<Float>::max(), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, 0}), fn(std::numeric_limits<Float>::min(), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, std::numeric_limits<RangeInt>::min()}), fn(std::numeric_limits<Float>::lowest(), ctx)); lost_precision(true);

    // special values
    EXPECT_EQ((any{std::in_place_type<Int>, std::numeric_limits<RangeInt>::max()}), fn(std::numeric_limits<Float>::infinity(), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, std::numeric_limits<RangeInt>::min()}), fn(-std::numeric_limits<Float>::infinity(), ctx)); lost_precision(true);
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), fn(std::numeric_limits<Float>::quiet_NaN(), ctx));
    EXPECT_EQ((any_error(error_kind::arithmetic_error)), fn(-std::numeric_limits<Float>::quiet_NaN(), ctx));
}

template <kind IntKind, kind FloatKind>
void test_float_to_int_minmax(evaluator_context& ctx, std::function<any(runtime_type<FloatKind>, evaluator_context&)> fn) {
    // verify on int min/max
    using Int = runtime_type<IntKind>;
    using Range = typename meta::field_type_traits<IntKind>::value_range;
    using Float = runtime_type<FloatKind>;
    EXPECT_EQ((any{std::in_place_type<Int>, static_cast<Int>(max_integral_float_convertible_to_int<IntKind, FloatKind>)}), fn(max_integral_float_convertible_to_int<IntKind, FloatKind>, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, static_cast<Int>(std::numeric_limits<Range>::max())}), fn(next_int_representable(max_integral_float_convertible_to_int<IntKind, FloatKind>), ctx)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<Int>, static_cast<Int>(min_integral_float_convertible_to_int<IntKind, FloatKind>)}), fn(min_integral_float_convertible_to_int<IntKind, FloatKind>, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Int>, static_cast<Int>(std::numeric_limits<Range>::min())}), fn(next_int_representable(min_integral_float_convertible_to_int<IntKind, FloatKind>, true), ctx)); lost_precision(true);
}

TEST_F(cast_between_numerics_test, next_int_representable) {
    EXPECT_EQ(2.0f, next_int_representable(1.5));
    EXPECT_EQ(1.0f, next_int_representable(0.0));
    EXPECT_EQ(1.0f, next_int_representable(-0.0));
    EXPECT_EQ(128.0f, next_int_representable(127.0));
    EXPECT_EQ(129.0f, next_int_representable(128.0));
    EXPECT_EQ(-1.0f, next_int_representable(-1.5));
    EXPECT_EQ(-2.0f, next_int_representable(-2.5));

    EXPECT_EQ(1.0f, next_int_representable(1.5, true));
    EXPECT_EQ(-1.0f, next_int_representable(0.0, true));
    EXPECT_EQ(-1.0f, next_int_representable(-0.0, true));
    EXPECT_EQ(127.0, next_int_representable(128.0, true));
    EXPECT_EQ(-2.0f, next_int_representable(-1.5, true));
    EXPECT_EQ(-3.0f, next_int_representable(-2.5, true));

    EXPECT_EQ(2147483648.0f, next_int_representable(2147483647));
    EXPECT_EQ(-2147483904.0f, next_int_representable(-2147483648.0f, true));
}

TEST_F(cast_between_numerics_test, float4_to_int1) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<float, std::int32_t, std::int8_t>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int1(src, ctx);
    });
    test_float_to_int_minmax<kind::int1, kind::float4>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int1(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float4_to_int2) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<float, std::int32_t, std::int16_t>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int2(src, ctx);
    });
    test_float_to_int_minmax<kind::int2, kind::float4>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int2(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, float4_to_int4) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<float, std::int32_t, std::int32_t>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int4(src, ctx);
    });
    test_float_to_int_minmax<kind::int4, kind::float4>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int4(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float4_to_int8) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<float, std::int64_t, std::int64_t>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int8(src, ctx);
    });
    test_float_to_int_minmax<kind::int8, kind::float4>(ctx, [](float src, evaluator_context& ctx){
        return from_float4::to_int8(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float8_to_int1) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<double, std::int32_t, std::int8_t>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int1(src, ctx);
    });
    test_float_to_int_minmax<kind::int1, kind::float8>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int1(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float8_to_int2) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<double, std::int32_t, std::int16_t>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int2(src, ctx);
    });
    test_float_to_int_minmax<kind::int2, kind::float8>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int2(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, float8_to_int4) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<double, std::int32_t, std::int32_t>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int4(src, ctx);
    });
    test_float_to_int_minmax<kind::int4, kind::float8>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int4(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float8_to_int8) {
    evaluator_context ctx{&resource_};
    test_float_to_int_common<double, std::int64_t, std::int64_t>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int8(src, ctx);
    });
    test_float_to_int_minmax<kind::int8, kind::float8>(ctx, [](double src, evaluator_context& ctx){
        return from_float8::to_int8(src, ctx);
    });
}

template <class Int, class Float>
void test_int_to_float(evaluator_context& ctx, std::function<any(Int, evaluator_context&)> fn) {
    EXPECT_EQ((any{std::in_place_type<Float>, 0}), fn(0, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Float>, 1}), fn(1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Float>, 10}), fn(10, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Float>, -1}), fn(-1, ctx)); lost_precision(false);

    // verify on ints min/max
    EXPECT_EQ((any{std::in_place_type<Float>, static_cast<Float>(std::numeric_limits<Int>::max())}), fn(std::numeric_limits<Int>::max(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<Float>, static_cast<Float>(std::numeric_limits<Int>::min())}), fn(std::numeric_limits<Int>::min(), ctx)); lost_precision(false);
}

TEST_F(cast_between_numerics_test, int4_to_float4) {
    evaluator_context ctx{&resource_};
    test_int_to_float<std::int32_t, float>(ctx, [](std::int32_t src, evaluator_context& ctx){
        return from_int4::to_float4(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int8_to_float4) {
    evaluator_context ctx{&resource_};
    test_int_to_float<std::int64_t, float>(ctx, [](std::int64_t src, evaluator_context& ctx){
        return from_int8::to_float4(src, ctx);
    });
}
TEST_F(cast_between_numerics_test, int4_to_float8) {
    evaluator_context ctx{&resource_};
    test_int_to_float<std::int32_t, double>(ctx, [](std::int32_t src, evaluator_context& ctx){
        return from_int4::to_float8(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, int8_to_float8) {
    evaluator_context ctx{&resource_};
    test_int_to_float<std::int64_t, double>(ctx, [](std::int64_t src, evaluator_context& ctx){
        return from_int8::to_float8(src, ctx);
    });
}

TEST_F(cast_between_numerics_test, float4_to_float8) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<double>, 0.0}), from_float4::to_float8(+0.0f, ctx)); lost_precision(false);
    {
        // negative zero preserves
        double negative_zero = from_float4::to_float8(-0.0f, ctx).to<double>(); lost_precision(false);
        EXPECT_EQ(0.0, negative_zero);
        EXPECT_TRUE(std::signbit(negative_zero));
    }
    EXPECT_EQ((any{std::in_place_type<double>, 1}), from_float4::to_float8(1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, 10}), from_float4::to_float8(10, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -1}), from_float4::to_float8(-1, ctx)); lost_precision(false);

    // verify on special values
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<double>::infinity()}), from_float4::to_float8(std::numeric_limits<float>::infinity(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<double>::infinity()}), from_float4::to_float8(-std::numeric_limits<float>::infinity(), ctx)); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float4::to_float8(std::numeric_limits<float>::quiet_NaN(), ctx).to<double>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float4::to_float8(-std::numeric_limits<float>::quiet_NaN(), ctx).to<double>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float4::to_float8(std::numeric_limits<float>::signaling_NaN(), ctx).to<double>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float4::to_float8(-std::numeric_limits<float>::signaling_NaN(), ctx).to<double>())); lost_precision(false);

    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<float>::max()}), from_float4::to_float8(std::numeric_limits<float>::max(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, std::numeric_limits<float>::min()}), from_float4::to_float8(std::numeric_limits<float>::min(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<float>::max()}), from_float4::to_float8(-std::numeric_limits<float>::max(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<double>, -std::numeric_limits<float>::min()}), from_float4::to_float8(-std::numeric_limits<float>::min(), ctx)); lost_precision(false);

    // no overflow / underflow possible
}

TEST_F(cast_between_numerics_test, float8_to_float4) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<float>, 0.0}), from_float8::to_float4(+0.0, ctx)); lost_precision(false);
    {
        // negative zero preserves
        float negative_zero = from_float8::to_float4(-0.0, ctx).to<float>(); lost_precision(false);
        EXPECT_EQ(0.0, negative_zero);
        EXPECT_TRUE(std::signbit(negative_zero));
    }
    EXPECT_EQ((any{std::in_place_type<float>, 1}), from_float8::to_float4(1, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 10}), from_float8::to_float4(10, ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -1}), from_float8::to_float4(-1, ctx)); lost_precision(false);

    // verify on special values
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), from_float8::to_float4(std::numeric_limits<double>::infinity(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), from_float8::to_float4(-std::numeric_limits<double>::infinity(), ctx)); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float8::to_float4(std::numeric_limits<float>::quiet_NaN(), ctx).to<float>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float8::to_float4(-std::numeric_limits<float>::quiet_NaN(), ctx).to<float>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float8::to_float4(std::numeric_limits<float>::signaling_NaN(), ctx).to<float>())); lost_precision(false);
    EXPECT_TRUE(std::isnan(from_float8::to_float4(-std::numeric_limits<float>::signaling_NaN(), ctx).to<float>())); lost_precision(false);

    // verify on min/max values
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::max()}), from_float8::to_float4(std::numeric_limits<float>::max(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::min()}), from_float8::to_float4(std::numeric_limits<float>::min(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::max()}), from_float8::to_float4(-std::numeric_limits<float>::max(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::min()}), from_float8::to_float4(-std::numeric_limits<float>::min(), ctx)); lost_precision(false);

    // larger than float max
    EXPECT_EQ((any{std::in_place_type<float>, std::numeric_limits<float>::infinity()}), from_float8::to_float4(std::nextafter(static_cast<double>(std::numeric_limits<float>::max()), std::numeric_limits<double>::infinity()), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, -std::numeric_limits<float>::infinity()}), from_float8::to_float4(std::nextafter(static_cast<double>(-std::numeric_limits<float>::max()), -std::numeric_limits<double>::infinity()), ctx)); lost_precision(false);

    // between float min and - float min
    EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), from_float8::to_float4(std::nextafter(static_cast<double>(std::numeric_limits<float>::min()), -std::numeric_limits<double>::infinity()), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), from_float8::to_float4(std::nextafter(static_cast<double>(-std::numeric_limits<float>::min()), std::numeric_limits<double>::infinity()), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), from_float8::to_float4(std::numeric_limits<double>::min(), ctx)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<float>, 0.0f}), from_float8::to_float4(-std::numeric_limits<double>::min(), ctx)); lost_precision(false);
}

TEST_F(cast_between_numerics_test, decimal_to_decimal) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_triple(1, 0, 1, 0), from_decimal::to_decimal(make_triple("1"), ctx, {}, {})); lost_precision(false);
    EXPECT_EQ(any_triple(1, 0, 123, 0), from_decimal::to_decimal(make_triple("123"), ctx, {}, {})); lost_precision(false);
    EXPECT_EQ(any_triple(1, 0, 12345, -2), from_decimal::to_decimal(make_triple("123.45"), ctx, {}, {})); lost_precision(false);

    EXPECT_EQ(any_triple(1, 0, 12345, -2), from_decimal::to_decimal(make_triple("123.45"), ctx, 5, 2)); lost_precision(false);
    EXPECT_EQ(any_triple(1, 0, 123450, -3), from_decimal::to_decimal(make_triple("123.45"), ctx, 6, 3)); lost_precision(false);
    EXPECT_EQ(any_triple(1, 0, 123450, -3), from_decimal::to_decimal(make_triple("123.45"), ctx, {}, 3)); lost_precision(false);
    EXPECT_EQ(any_triple(1, 0, 9999, -2), from_decimal::to_decimal(make_triple("123.45"), ctx, 4, 2)); lost_precision(true);
    EXPECT_EQ(any_error(error_kind::unsupported), from_decimal::to_decimal(make_triple("123.45"), ctx, 4, {}));
    EXPECT_EQ(any_triple(1, 0, 123, 0), from_decimal::to_decimal(make_triple("123.45"), ctx, {}, 0)); lost_precision(true);
}
}

