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

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/value.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

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

class value_test : public test_root {
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

TEST_F(value_test, simple) {
    data::value v{};
    ASSERT_FALSE(v);
    ASSERT_TRUE(v.empty());
    v = data::value{std::in_place_type<std::int32_t>, 1};
    ASSERT_TRUE(v);
    ASSERT_FALSE(v.empty());
    ASSERT_EQ(1, v.ref<std::int32_t>());
}

TEST_F(value_test, fail_on_type_mismatch) {
    data::value v{};
    ASSERT_THROW({ (void)v.ref<std::int32_t>(); }, std::logic_error);

    v = data::value{std::in_place_type<std::int64_t>, 1};
    ASSERT_THROW({ (void)v.ref<std::int32_t>(); }, std::logic_error);
}

TEST_F(value_test, bool) {
    // bool and std::int8_t can be used synonymously
    {
        auto v = data::value{std::in_place_type<std::int8_t>, 1};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(1, v.ref<std::int8_t>());
        ASSERT_TRUE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<std::int8_t>, 0};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(0, v.ref<std::int8_t>());
        ASSERT_FALSE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<bool>, true};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(1, v.ref<std::int8_t>());
        ASSERT_TRUE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<bool>, false};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(0, v.ref<std::int8_t>());
        ASSERT_FALSE(v.ref<bool>());
    }
}

TEST_F(value_test, string) {
    data::value v{};
    ASSERT_FALSE(v);
    ASSERT_TRUE(v.empty());
    v = data::value{std::in_place_type<std::string>, "ABC"};
    ASSERT_TRUE(v);
    ASSERT_FALSE(v.empty());
    ASSERT_EQ("ABC", v.ref<std::string>());

    auto a = v.view();
    auto t = a.to<accessor::text>();
    // static_cast to sv requires accessor::text lvalue as sv can reference SSO'ed data in accessor::text.
    auto sv = static_cast<std::string_view>(t);
    ASSERT_EQ("ABC", sv);
}
}

