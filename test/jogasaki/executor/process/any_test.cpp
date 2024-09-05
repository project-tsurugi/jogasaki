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
#include <iostream>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/relation/buffer.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/sort_direction.h>
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
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::expr {

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

class any_test : public test_root {
public:
};

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

TEST_F(any_test, simple) {
    any a{};
    ASSERT_FALSE(a);
    ASSERT_TRUE(a.empty());
    ASSERT_FALSE(a.error());
    a = any{std::in_place_type<std::int32_t>, 1};
    ASSERT_TRUE(a);
    ASSERT_FALSE(a.empty());
    ASSERT_FALSE(a.error());
    ASSERT_EQ(1, a.to<std::int32_t>());
}

TEST_F(any_test, fail_on_type_mismatch) {
    any a{};
    ASSERT_THROW({ (void)a.to<std::int32_t>(); }, std::logic_error);

    a = any{std::in_place_type<std::int64_t>, 1};
    ASSERT_THROW({ (void)a.to<std::int32_t>(); }, std::logic_error);
}

TEST_F(any_test, bool) {
    // bool and std::int8_t can be used synonymously
    {
        auto a = any{std::in_place_type<std::int8_t>, 1};
        ASSERT_TRUE(a);
        ASSERT_FALSE(a.empty());
        ASSERT_FALSE(a.error());
        ASSERT_EQ(1, a.to<std::int8_t>());
        ASSERT_TRUE(a.to<bool>());
    }
    {
        auto a = any{std::in_place_type<std::int8_t>, 0};
        ASSERT_TRUE(a);
        ASSERT_FALSE(a.empty());
        ASSERT_FALSE(a.error());
        ASSERT_EQ(0, a.to<std::int8_t>());
        ASSERT_FALSE(a.to<bool>());
    }
    {
        auto a = any{std::in_place_type<bool>, true};
        ASSERT_TRUE(a);
        ASSERT_FALSE(a.empty());
        ASSERT_FALSE(a.error());
        ASSERT_EQ(1, a.to<std::int8_t>());
        ASSERT_TRUE(a.to<bool>());
    }
    {
        auto a = any{std::in_place_type<bool>, false};
        ASSERT_TRUE(a);
        ASSERT_FALSE(a.empty());
        ASSERT_FALSE(a.error());
        ASSERT_EQ(0, a.to<std::int8_t>());
        ASSERT_FALSE(a.to<bool>());
    }
}

TEST_F(any_test, comparison) {
    {
        any a{};
        any b{};
        ASSERT_EQ(a, b);
    }
    {
        any a{std::in_place_type<std::int32_t>, 1};
        any b{std::in_place_type<std::int32_t>, 1};
        ASSERT_EQ(a, b);
    }
    {
        any a{std::in_place_type<std::int32_t>, 1};
        any b{std::in_place_type<std::int64_t>, 1};
        ASSERT_NE(a, b);
    }
    {
        any a{};
        any b{std::in_place_type<std::int64_t>, 1};
        ASSERT_NE(a, b);
    }
}

TEST_F(any_test, print) {
    std::cerr << "empty " << any{} << std::endl;
    std::cerr << "1:int32_t " << any{std::in_place_type<std::int32_t>, 1} << std::endl;
    std::cerr << "1:int64_t " << any{std::in_place_type<std::int64_t>, 1} << std::endl;
    std::cerr << "1:float " << any{std::in_place_type<float>, 1} << std::endl;
    std::cerr << "1:double " << any{std::in_place_type<double>, 1} << std::endl;
}
}

