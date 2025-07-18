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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <decimal.hh>
#include <float.h>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
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
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/to_field_type_kind.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::executor::expr {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
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
using variable = takatori::descriptor::variable;

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
    executor::process::impl::variable_table_info info_{};
    executor::process::impl::variable_table vars_{};

    compiled_info c_info_{};
    expr::evaluator evaluator_{};
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
        info_ = executor::process::impl::variable_table_info{m, meta_};
        vars_ = executor::process::impl::variable_table{info_};

        c_info_ = compiled_info{expressions_, variables_};
        evaluator_ = expr::evaluator{*expr, c_info_};
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
    void test_two_arity_exp_with_null(
        Optype op,
        typename meta::field_type_traits<utils::to_field_type_kind<In1>()>::runtime_type c1,
        bool c1_is_null,
        typename meta::field_type_traits<utils::to_field_type_kind<In2>()>::runtime_type c2,
        bool c2_is_null,
        typename meta::field_type_traits<utils::to_field_type_kind<Out>()>::runtime_type exp,
        bool exp_is_null,
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
            set_values<In1, In2>(c1, c2, c1_is_null, c2_is_null);
            utils::checkpoint_holder cph{&resource_};
            evaluator_context c{&resource_};
            auto a = evaluator_(c, vars_, &resource_);
            ASSERT_TRUE(! a.error());
            if(exp_is_null) {
                ASSERT_TRUE(a.empty());
            } else {
                ASSERT_TRUE(! a.empty());
                auto result = a.to<out_type>();
                ASSERT_EQ(exp, result);
            }
        }
        expressions().clear();
    }

    template <class In1, class In2, class Out, class Optype, class ... Args>
    void test_two_arity_exp(
        Optype op,
        typename meta::field_type_traits<utils::to_field_type_kind<In1>()>::runtime_type c1,
        typename meta::field_type_traits<utils::to_field_type_kind<In2>()>::runtime_type c2,
        typename meta::field_type_traits<utils::to_field_type_kind<Out>()>::runtime_type exp,
        Args...args
    ) {
        test_two_arity_exp_with_null<In1, In2, Out, Optype, Args...>(op, c1, false, c2, false, exp, false, args...);
        test_two_arity_exp_with_null<In1, In2, Out, Optype, Args...>(op, c1, true, c2, false, exp, true, args...);
        test_two_arity_exp_with_null<In1, In2, Out, Optype, Args...>(op, c1, false, c2, true, exp, true, args...);
    }

    template<class T>
    void test_compare();

    void compare_time_points(comparison_operator op, takatori::datetime::time_point l, takatori::datetime::time_point r, bool expected) {
        test_two_arity_exp<t::time_point, t::time_point, t::boolean>(op, l, r, expected);
    }

};

TEST_F(expression_evaluator_test, add_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::float4, t::float4, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::add, 10, 20, 30);
}

takatori::decimal::triple from_double(double x) {
    decimal::Decimal d{std::to_string(x)};
    return takatori::decimal::triple{d.as_uint128_triple()};
}

TEST_F(expression_evaluator_test, add_different_types) {
    test_two_arity_exp<t::int4, t::int8, t::int8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int4, t::float4, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int8, t::float4, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int4, t::float8, t::float8>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int4, t::decimal, t::decimal>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::int8, t::decimal, t::decimal>(binary_operator::add, 10, 20, 30);
    test_two_arity_exp<t::float4, t::decimal, t::float8>(binary_operator::add, 10.5, from_double(20.5), 31);
    test_two_arity_exp<t::float8, t::decimal, t::float8>(binary_operator::add, 10.5, from_double(20.5), 31);
}

TEST_F(expression_evaluator_test, subtract_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::float4, t::float4, t::float8>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::subtract, 20, 5, 15);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::subtract, 20, 5, 15);
}

TEST_F(expression_evaluator_test, multiply_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::float4, t::float4, t::float8>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::multiply, 2, 3, 6);
    test_two_arity_exp<t::decimal, t::decimal, t::decimal>(binary_operator::multiply, 2, 3, 6);
}

TEST_F(expression_evaluator_test, divide_numeric) {
    test_two_arity_exp<t::int8, t::int8, t::int8>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::int4, t::int4, t::int4>(binary_operator::divide, 6, 3, 2);
    test_two_arity_exp<t::float4, t::float4, t::float8>(binary_operator::divide, 6, 3, 2);
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
    evaluator_ = expr::evaluator{expr, c_info};

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

    info_ = executor::process::impl::variable_table_info{m, meta_};
    vars_ = executor::process::impl::variable_table{info_};

    set_values<t::int8, t::int8>(10, 20, false, false);

    evaluator_context c{nullptr};
    auto result = evaluator_(c, vars_).to<std::int64_t>();
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
    expr::evaluator ev{expr, c_info};

    executor::process::impl::variable_table vars{};
    evaluator_context c{nullptr};
    auto result = ev(c, vars).to<std::int64_t>();
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
    expr::evaluator ev{expr, c_info};

    executor::process::impl::variable_table vars{};
    evaluator_context c{nullptr};
    ASSERT_TRUE(ev(c, vars).to<bool>());
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
            meta::field_type(std::make_shared<meta::character_field_option>()),
        },
        boost::dynamic_bitset<std::uint64_t>{1}.flip()
    );

    std::unordered_map<variable, std::size_t> m{
        {c1, 0},
    };

    executor::process::impl::variable_table_info info{m, meta};
    executor::process::impl::variable_table vars{info};

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    auto cp = resource.get_checkpoint();
    auto&& ref = vars.store().ref();
    ref.set_value<accessor::text>(meta->value_offset(0), accessor::text{&resource, "A23456789012345678901234567890"});
    ref.set_null(meta->nullity_offset(0), false);
    compiled_info c_info{ expressions_, variables_ };
    expr::evaluator ev{expr, c_info};
    evaluator_context c{&resource};
    ASSERT_EQ(30, ev(c, vars, &resource).to<std::int32_t>());
}

template<class T>
void expression_evaluator_test::test_compare() {
    typename meta::field_type_traits<utils::to_field_type_kind<T>()>::runtime_type one{};
    typename meta::field_type_traits<utils::to_field_type_kind<T>()>::runtime_type two{};

    if constexpr (std::is_same_v<T, t::date>) {
        one = takatori::datetime::date{1};
        two = takatori::datetime::date{2};
    } else
    if constexpr (std::is_same_v<T, t::time_of_day>) {
        one = takatori::datetime::time_of_day{1ns};
        two = takatori::datetime::time_of_day{2ns};
    } else
    if constexpr (std::is_same_v<T, t::time_point>) {
        one = takatori::datetime::time_point{1ns};
        two = takatori::datetime::time_point{2ns};
    } else {
        one = 1;
        two = 2;
    }
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less, one, two, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less, one, one, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, one, two, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, one, one, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::less_equal, two, one, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, two, one, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, one, one, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater_equal, two, one, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater_equal, one, one, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::greater, one, two, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::equal, one, one, true);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::equal, one, two, false);

    test_two_arity_exp<T, T, t::boolean>(comparison_operator::not_equal, one, one, false);
    test_two_arity_exp<T, T, t::boolean>(comparison_operator::not_equal, one, two, true);
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
    {
        SCOPED_TRACE("date");
        test_compare<t::date>();
    }
    {
        SCOPED_TRACE("time_of_day");
        test_compare<t::time_of_day>();
    }
    {
        SCOPED_TRACE("time_point");
        test_compare<t::time_point>();
    }
}

TEST_F(expression_evaluator_test, compare_time_point) {
    // time point use lexicographical comparison for two parts
    auto one = takatori::datetime::time_point{
        takatori::datetime::date{1},
        takatori::datetime::time_of_day{999ns},
    };
    auto two = takatori::datetime::time_point{
        takatori::datetime::date{2},
        takatori::datetime::time_of_day{9ns},
    };

    compare_time_points(comparison_operator::less, one, two, true);
    compare_time_points(comparison_operator::less, two, one, false);

    compare_time_points(comparison_operator::less_equal, one, two, true);
    compare_time_points(comparison_operator::less_equal, one, one, true);
    compare_time_points(comparison_operator::less_equal, two, one, false);

    compare_time_points(comparison_operator::greater, two, one, true);
    compare_time_points(comparison_operator::greater, one, two, false);

    compare_time_points(comparison_operator::greater_equal, two, one, true);
    compare_time_points(comparison_operator::greater_equal, one, one, true);
    compare_time_points(comparison_operator::greater_equal, one, two, false);
}

TEST_F(expression_evaluator_test, conditional_and) {
    // condiation_and and conditional_or are exceptional operation in that the result is not always null even if one of the operad is null

    // T and T = T
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 1, false, 1, false, true, false);

    // T and F = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 1, false, 0, false, false, false);

    // F and T = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 0, false, 1, false, false, false);

    // F and F = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 0, false, 0, false, false, false);

    // null and T = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, -1, true, 1, false, false, true);

    // T and null = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 1, false, -1, true, false, true);

    // null and F = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, -1, true, 0, false, false, false);

    // F and null = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, 0, false, -1, true, false, false);

    // null and null = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_and, -1, true, -1, true, false, true);
}

TEST_F(expression_evaluator_test, conditional_or) {
    // condiation_and and conditional_or are exceptional operation in that the result is not always null even if one of the operad is null

    // T or T = T
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 1, false, 1, false, true, false);

    // T or F = T
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 1, false, 0, false, true, false);

    // F or T = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 0, false, 1, false, true, false);

    // F or F = F
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 0, false, 0, false, false, false);

    // null or T = T
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, -1, true, 1, false, true, false);

    // T or null = T
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 1, false, -1, true, true, false);

    // null or F = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, -1, true, 0, false, false, true);

    // F or null = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, 0, false, -1, true, false, true);

    // null or null = null
    test_two_arity_exp_with_null<t::boolean, t::boolean, t::boolean>(binary_operator::conditional_or, -1, true, -1, true, false, true);
}

TEST_F(expression_evaluator_test, arithmetic_error) {
    auto expr = create_two_arity_exp<t::float8, t::float8, t::float8>(binary_operator::divide);
    {
        set_values<t::float8, t::float8>(10.0, 0.0, false, false);
        utils::checkpoint_holder cph{&resource_};
        evaluator_context c{&resource_};
        auto result = evaluator_(c, vars_, &resource_);
        ASSERT_FALSE(result);
        ASSERT_FALSE(result.empty());
        ASSERT_TRUE(result.error());
        auto err = result.to<error>();
        ASSERT_EQ(error_kind::arithmetic_error, err.kind());
    }
}

TEST_F(expression_evaluator_test, to_triple) {
    EXPECT_EQ(0, details::triple_from_int(0));
    auto i64max = std::numeric_limits<std::int64_t>::max();
    EXPECT_EQ(i64max, details::triple_from_int(i64max));
    auto i64min = std::numeric_limits<std::int64_t>::min();
    EXPECT_EQ(i64min, details::triple_from_int(i64min));
}

TEST_F(expression_evaluator_test, triple_to_double) {
    using takatori::decimal::triple;
    {
        // boundary values for decimal with max precision
        auto v0 = triple{-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, 0}; // -999....9 (38 digits)
        auto v1 = triple{-1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, 0}; // -999....8 (38 digits)
        auto v2 = triple{0, 0, 0, 0}; // 0
        auto v3 = triple{1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFEUL, 0}; // +999....8 (38 digits)
        auto v4 = triple{1, 0x4B3B4CA85A86C47AUL, 0x098A223FFFFFFFFFUL, 0}; // +999....9 (38 digits)

        // expected values are approximate
        EXPECT_DOUBLE_EQ(-9.9999999999999998E37, details::triple_to_double(v0));
        EXPECT_DOUBLE_EQ(-9.9999999999999998E37, details::triple_to_double(v1));
        EXPECT_DOUBLE_EQ(0, details::triple_to_double(v2));
        EXPECT_DOUBLE_EQ(+9.9999999999999998E37, details::triple_to_double(v3));
        EXPECT_DOUBLE_EQ(+9.9999999999999998E37, details::triple_to_double(v4));
    }
    {
        // boundary values for triples
        auto v0 = triple{-1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};
        auto v1 = triple{-1, 0x8000000000000000UL, 0x0000000000000000UL, 0};
        auto v2 = triple{-1, 0x7FFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};
        auto v3 = triple{1, 0x8000000000000000UL,  0x0000000000000000UL, 0};
        auto v4 = triple{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0};

        // expected values are approximate
        EXPECT_DOUBLE_EQ(-3.4028236692093846e+38, details::triple_to_double(v0));
        EXPECT_DOUBLE_EQ(-1.7014118346046923e+38, details::triple_to_double(v1));
        EXPECT_DOUBLE_EQ(-1.7014118346046923e+38, details::triple_to_double(v2));
        EXPECT_DOUBLE_EQ(+1.7014118346046923e+38, details::triple_to_double(v3));
        EXPECT_DOUBLE_EQ(+3.4028236692093846e+38, details::triple_to_double(v4));
    }
    {
        // underflow
        auto v0 = from_double(DBL_MIN);
        EXPECT_DOUBLE_EQ(0, details::triple_to_double(v0));
    }
}
}

