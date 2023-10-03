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
#include <jogasaki/executor/process/impl/expression/evaluator.h>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/binding/factory.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/plan/forward.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/expression/error.h>

#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/common.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>

#include <jogasaki/test_utils/to_field_type_kind.h>

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

class cast_expression_test : public test_root {
public:

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

    template <class T>
    using from_operator_enum = typename std::conditional_t<
        std::is_same_v<T, binary_operator>, binary,
        std::conditional_t<
            std::is_same_v<T, comparison_operator>, compare,
            void
        >
    >;

    template<class In1, class In2, class Out, class Optype, class ...Args>
    std::unique_ptr<from_operator_enum<Optype>> create_two_arity_exp(Optype op, Args...args) {
        using T = from_operator_enum<Optype>;
        auto&& c1 = f_.stream_variable("c1");
        auto&& c2 = f_.stream_variable("c2");
        auto expr = std::make_unique<T>(
            op,
            varref(c1),
            varref(c2)
        );
        expressions().bind(*expr, Out{args...});
        expressions().bind(expr->left(), In1{args...});
        expressions().bind(expr->right(), In2{args...});

        meta_ = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                utils::type_for(In1{args...}),
                utils::type_for(In2{args...}),
            },
            boost::dynamic_bitset<std::uint64_t>{2}.flip()
        );

        std::unordered_map<variable, std::size_t> m{
            {c1, 0},
            {c2, 1},
        };
        info_ = variable_table_info{m, meta_};
        vars_ = variable_table{info_};

        c_info_ = compiled_info{expressions_, variables_};
        evaluator_ = expression::evaluator{*expr, c_info_};
        return expr;
    }

    template<class In1, class In2>
    void set_values(
        typename meta::field_type_traits<utils::to_field_type_kind<In1>()>::runtime_type c1,
        typename meta::field_type_traits<utils::to_field_type_kind<In2>()>::runtime_type c2,
        bool c1_null,
        bool c2_null
    ) {
        auto&& ref = vars_.store().ref();
        ref.set_value<decltype(c1)>(meta_->value_offset(0), c1);
        ref.set_null(meta_->nullity_offset(0), c1_null);
        ref.set_value<decltype(c2)>(meta_->value_offset(1), c2);
        ref.set_null(meta_->nullity_offset(1), c2_null);
    }

    template <class In1, class In2, class Out, class Optype, class ... Args>
    void test_two_arity_exp(
        Optype op,
        typename meta::field_type_traits<utils::to_field_type_kind<In1>()>::runtime_type c1,
        typename meta::field_type_traits<utils::to_field_type_kind<In2>()>::runtime_type c2,
        typename meta::field_type_traits<utils::to_field_type_kind<Out>()>::runtime_type exp,
        Args...args
    ) {
        using T = from_operator_enum<Optype>;

        // for compare operation, use bool instead of std::int8_t.
        using out_type = std::conditional_t<
            std::is_same_v<Optype, binary_operator>,
            typename meta::field_type_traits<utils::to_field_type_kind<Out>()>::runtime_type,
            bool
        >;
        auto expr = create_two_arity_exp<In1, In2, Out, Optype, Args...>(op, args...);
        {
            set_values<In1, In2>(c1, c2, false, false);
            utils::checkpoint_holder cph{&resource_};
            evaluator_context ctx{};
            auto result = evaluator_(ctx, vars_, &resource_).to<out_type>();
            ASSERT_EQ(exp, result);
        }
        {
            set_values<In1, In2>(c1, c2, true, false);
            utils::checkpoint_holder cph{&resource_};
            evaluator_context ctx{};
            auto result = evaluator_(ctx, vars_, &resource_);
            ASSERT_TRUE(result.empty());
            ASSERT_FALSE(result.error());
        }
        {
            set_values<In1, In2>(c1, c2, false, true);
            utils::checkpoint_holder cph{&resource_};
            evaluator_context ctx{};
            auto result = evaluator_(ctx, vars_, &resource_);
            ASSERT_TRUE(result.empty());
            ASSERT_FALSE(result.error());
        }
        expressions().clear();
    }
    template<class T>
    void test_compare();
};

inline immediate constant(int v, type::data&& type = type::int8()) {
    return immediate { value::int8(v), std::move(type) };
}

inline immediate constant_bool(bool v, type::data&& type = type::boolean()) {
    return immediate { value::boolean(v), std::move(type) };
}

TEST_F(cast_expression_test, string_to_int) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), details::to_int1("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), details::to_int2("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 1}), details::to_int4("1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), details::to_int8("1", ctx));

    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 1}), details::to_int8("+1", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -1}), details::to_int8("-1", ctx));
}

TEST_F(cast_expression_test, string_to_int1_min_max) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 127}), details::to_int1("+127", ctx));  // 2^7-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -128}), details::to_int1("-128", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int1("128", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int1("-129", ctx));
}

TEST_F(cast_expression_test, string_to_int2_min_max) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 32767}), details::to_int2("+32767", ctx)); // 2^15-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -32768}), details::to_int2("-32768", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int2("32768", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int2("-32769", ctx));
}

TEST_F(cast_expression_test, string_to_int4_min_max) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, 2147483647}), details::to_int4("+2147483647", ctx));  // 2^31-1
    EXPECT_EQ((any{std::in_place_type<std::int32_t>, -2147483648}), details::to_int4("-2147483648", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int4("2147483648", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int4("-2147483649", ctx));
}

TEST_F(cast_expression_test, string_to_int8_min_max) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, 9'223'372'036'854'775'807L}), details::to_int8("+9223372036854775807", ctx));  // 2^63-1
    EXPECT_EQ((any{std::in_place_type<std::int64_t>, -9'223'372'036'854'775'807L-1}), details::to_int8("-9223372036854775808", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int8("9223372036854775808", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_int8("-9223372036854775809", ctx));
}

TEST_F(cast_expression_test, string_trim) {
    EXPECT_EQ(std::string_view{}, details::trim_spaces(""));
    EXPECT_EQ("ABC", details::trim_spaces(" ABC "));
    EXPECT_EQ("A  B", details::trim_spaces(" A  B "));
    EXPECT_EQ("ABC", details::trim_spaces("  ABC"));
    EXPECT_EQ("ABC", details::trim_spaces("ABC  "));
    EXPECT_EQ("ABC  ABC", details::trim_spaces("ABC  ABC "));
    EXPECT_EQ("ABC  ABC", details::trim_spaces(" ABC  ABC"));
}

TEST_F(cast_expression_test, is_prefix) {
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

TEST_F(cast_expression_test, bad_format) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), details::to_int4("++1", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), details::to_int4("", ctx));
}

TEST_F(cast_expression_test, string_to_boolean) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), details::to_boolean("true", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 1}), details::to_boolean("T", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), details::to_boolean("false", ctx));
    EXPECT_EQ((any{std::in_place_type<std::int8_t>, 0}), details::to_boolean("F", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), details::to_boolean("", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::format_error}), details::to_boolean("wrong text", ctx));
}

TEST_F(cast_expression_test, string_to_decimal) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<triple>, 1}), details::to_decimal("1", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, -1}), details::to_decimal("-1", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), details::to_decimal("+0", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, 0}), details::to_decimal("-0", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -2}}), details::to_decimal("1.23", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 1234567890, -5}}), details::to_decimal("-12345.67890", ctx));
}

TEST_F(cast_expression_test, string_to_decimal_min_max) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}}), details::to_decimal("340282366920938463463374607431768211455", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}}), details::to_decimal("-340282366920938463463374607431768211455", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_decimal("340282366920938463463374607431768211456", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_decimal("-340282366920938463463374607431768211456", ctx));
}

TEST_F(cast_expression_test, string_to_decimal_large) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1, 100}}), details::to_decimal("1E100", ctx));
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 1, 100}}), details::to_decimal("-1E100", ctx));
}

TEST_F(cast_expression_test, string_to_float) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<float>, 1.0}), details::to_float4("1.0", ctx));
    EXPECT_EQ((any{std::in_place_type<float>, 3.40282e+38}), details::to_float4("3.40282e+38", ctx));  // FLT_MAX
    EXPECT_EQ((any{std::in_place_type<float>, 1.17550e-38}), details::to_float4("1.17550e-38", ctx));  // FLT_MIN is 1.17549e-38
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_float4("3.40283e+38", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_float4("1.17549e-38", ctx));
}

TEST_F(cast_expression_test, string_to_double) {
    evaluator_context ctx{};
    EXPECT_EQ((any{std::in_place_type<double>, 1.0}), details::to_float8("1.0", ctx));
    EXPECT_EQ((any{std::in_place_type<double>, 1.79769e+308}), details::to_float8("1.79769e+308", ctx)); // DBL_MAX
    EXPECT_EQ((any{std::in_place_type<double>, 2.22508e-308}), details::to_float8("2.22508e-308", ctx)); // DBL_MIN is 2.22507e-308
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_float8("1.79770e+308", ctx));
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::overflow}), details::to_float8("2.22507e-308", ctx));
}
}

