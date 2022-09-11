/*
 * Copyright 2018-2020 tsurugi project.
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

#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/expression/error.h>

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

class expression_evaluator_test : public test_root {
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
            auto result = evaluator_(vars_, &resource_).to<out_type>();
            ASSERT_EQ(exp, result);
        }
        {
            set_values<In1, In2>(c1, c2, true, false);
            utils::checkpoint_holder cph{&resource_};
            auto result = evaluator_(vars_, &resource_);
            ASSERT_TRUE(result.empty());
            ASSERT_FALSE(result.error());
        }
        {
            set_values<In1, In2>(c1, c2, false, true);
            utils::checkpoint_holder cph{&resource_};
            auto result = evaluator_(vars_, &resource_);
            ASSERT_TRUE(result.empty());
            ASSERT_FALSE(result.error());
        }
        expressions().clear();
    }
    template<class T>
    void test_compare();
};

TEST_F(expression_evaluator_test, add_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::float4, t::float4, t::float4>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::add, 10, 20, 30);
}

TEST_F(expression_evaluator_test, subtract_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::float4, t::float4, t::float4>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::subtract, 20, 5, 15);
}

TEST_F(expression_evaluator_test, multiply_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::float4, t::float4, t::float4>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::multiply, 2, 3, 6);
}

TEST_F(expression_evaluator_test, divide_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::float4, t::float4, t::float4>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::divide, 6, 3, 2);
}

TEST_F(expression_evaluator_test, remainder_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::remainder, 9, 4, 1);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::remainder, 9, 4, 1);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::remainder, 9, 4, 1);
}

TEST_F(expression_evaluator_test, concat) {
    test_two_arity_exp<t::character, t::character, t::character>(
        binary_operator::concat,
        accessor::text{&resource_, "A23456789012345678901234567890"},
        accessor::text{&resource_, "B23456789012345678901234567890"},
        accessor::text{&resource_, "A23456789012345678901234567890B23456789012345678901234567890"},
        type::varying, 200
    );
}

inline immediate constant(int v, type::data&& type = type::int8()) {
    return immediate { value::int8(v), std::move(type) };
}

inline immediate constant_bool(bool v, type::data&& type = type::boolean()) {
    return immediate { value::boolean(v), std::move(type) };
}

TEST_F(expression_evaluator_test, binary_expression) {
    auto&& c1 = f_.stream_variable("c1");
    auto&& c2 = f_.stream_variable("c2");

    binary expr {
        binary_operator::subtract,
        varref(c1),
        binary {
            binary_operator::add,
            varref(c2),
            constant(30)
        }
    };
    expressions().bind(expr, t::int8 {});
    expressions().bind(expr.left(), t::int8 {});
    expressions().bind(expr.right(), t::int8 {});

    auto& r = static_cast<binary&>(expr.right());
    expressions().bind(r.left(), t::int8 {});
    expressions().bind(r.right(), t::int8 {});

    compiled_info c_info{
        expressions_,
        variables_
    };
    evaluator_ = expression::evaluator{expr, c_info};

    meta_ = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
            meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{2}.flip()
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
        {c2, 1},
    };

    info_ = variable_table_info{m, meta_};
    vars_ = variable_table{info_};

    set_values<t::int8, t::int8>(10, 20, false, false);

    auto result = evaluator_(vars_).to<std::int64_t>();
    ASSERT_EQ(-40, result);
}

TEST_F(expression_evaluator_test, unary_expression) {
    unary expr {
        unary_operator::sign_inversion,
        unary {
            unary_operator::plus,
            constant(30)
        }
    };
    expressions().bind(expr, t::int8 {});
    expressions().bind(expr.operand(), t::int8 {});

    auto& o = static_cast<unary&>(expr.operand());
    expressions().bind(o.operand(), t::int8 {});

    compiled_info c_info{
        expressions_,
        variables_
    };
    expression::evaluator ev{expr, c_info};

    variable_table vars{};
    auto result = ev(vars).to<std::int64_t>();
    ASSERT_EQ(-30, result);
}

TEST_F(expression_evaluator_test, conditional_not) {
    unary expr {
        unary_operator::conditional_not,
        constant_bool(false)
    };
    expressions().bind(expr, t::boolean {});
    expressions().bind(expr.operand(), t::boolean {});

    compiled_info c_info{ expressions_, variables_ };
    expression::evaluator ev{expr, c_info};

    variable_table vars{};
    ASSERT_TRUE(ev(vars).to<bool>());
}

TEST_F(expression_evaluator_test, text_length) {
    factory f;
    auto&& c1 = f.stream_variable("c1");

    unary expr {
        unary_operator::length,
        varref(c1)
    };
    expressions().bind(expr, t::int4 {});
    expressions().bind(expr.operand(), t::character{type::varying, 200});

    maybe_shared_ptr<meta::record_meta> meta = std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(meta::field_enum_tag<meta::field_type_kind::character>),
        },
        boost::dynamic_bitset<std::uint64_t>{1}.flip()
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
    };

    variable_table_info info{m, meta};
    variable_table vars{info};

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    auto cp = resource.get_checkpoint();
    auto&& ref = vars.store().ref();
    ref.set_value<accessor::text>(meta->value_offset(0), accessor::text{&resource, "A23456789012345678901234567890"});
    ref.set_null(meta->nullity_offset(0), false);
    compiled_info c_info{ expressions_, variables_ };
    expression::evaluator ev{expr, c_info};
    auto result = ev(vars, &resource).to<std::int32_t>();
    ASSERT_EQ(30, ev(vars, &resource).to<std::int32_t>());
}

template<class T>
void expression_evaluator_test::test_compare() {
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less, 1, 2, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less, 1, 1, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, 1, 2, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, 1, 1, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, 2, 1, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, 2, 1, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, 1, 1, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater_equal, 2, 1, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater_equal, 1, 1, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, 1, 2, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::equal, 1, 1, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::equal, 1, 2, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::not_equal, 1, 1, false);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::not_equal, 1, 2, true);
}

TEST_F(expression_evaluator_test, compare_numeric) {
    {
        SCOPED_TRACE("int4");
        test_compare<t::int4>();
    }
    {
        SCOPED_TRACE("int8");
        test_compare<t::int8>();
    }
    {
        SCOPED_TRACE("float4");
        test_compare<t::float4>();
    }
    {
        SCOPED_TRACE("float8");
        test_compare<t::float8>();
    }
    {
        SCOPED_TRACE("decimal");
        test_compare<t::decimal>();
    }
}

TEST_F(expression_evaluator_test, conditional_and_or) {
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 1, 1, true);
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 1, 0, false);
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 0, 1, false);

    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 1, 1, true);
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 1, 0, true);
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 0, 1, true);
    test_two_arity_exp<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 0, 0, false);
}

TEST_F(expression_evaluator_test, arithmetic_error) {
    auto expr = create_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::divide);
    {
        set_values<t::float8, t::float8>(10.0, 0.0, false, false);
        utils::checkpoint_holder cph{&resource_};
        auto result = evaluator_(vars_, &resource_);
        ASSERT_FALSE(result);
        ASSERT_FALSE(result.empty());
        ASSERT_TRUE(result.error());
        auto err = result.to<error>();
        ASSERT_EQ(error_kind::arithmetic_error, err.kind());
    }
}
}

