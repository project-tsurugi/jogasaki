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
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/variable_order.h>

namespace jogasaki::meta {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

using namespace ::yugawara::binding;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

class variable_order_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(variable_order_test, stream_variables) {
    factory f;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");

    std::vector<variable> cols{ c1, c0, c2 };

    variable_order ord{ variable_ordering_enum_tag<variable_ordering_kind::flat_record>, cols};

    EXPECT_FALSE(ord.for_group());
    EXPECT_EQ(1, ord.index(c0));
    EXPECT_EQ(0, ord.index(c1));
    EXPECT_EQ(2, ord.index(c2));
    auto it = ord.begin();
    EXPECT_EQ(c1, *it++);
    EXPECT_EQ(c0, *it++);
    EXPECT_EQ(c2, *it++);
    EXPECT_EQ(ord.end(), it);
}

TEST_F(variable_order_test, create_from_exchange_columns) {

    factory f;
    ::takatori::plan::forward f1 {
        f.exchange_column(),
        f.exchange_column(),
        f.exchange_column(),
    };
    auto&& c0 = f1.columns()[0];
    auto&& c1 = f1.columns()[1];
    auto&& c2 = f1.columns()[2];

    variable_order ord{ variable_ordering_enum_tag<variable_ordering_kind::flat_record>, f1.columns()};

    EXPECT_FALSE(ord.for_group());
    EXPECT_EQ(0, ord.index(c0));
    EXPECT_EQ(1, ord.index(c1));
    EXPECT_EQ(2, ord.index(c2));

    auto it = ord.begin();
    EXPECT_EQ(c0, *it++);
    EXPECT_EQ(c1, *it++);
    EXPECT_EQ(c2, *it++);
    EXPECT_EQ(ord.end(), it);
}

TEST_F(variable_order_test, flat_record_from_keys_values) {
    factory f;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& c3 = f.stream_variable("c3");

    std::vector<variable> keys{ c0, c1 };
    std::vector<variable> values{ c2, c3 };

    variable_order ord{ variable_ordering_enum_tag<variable_ordering_kind::flat_record_from_keys_values>, keys, values};

    EXPECT_FALSE(ord.for_group());
    using pair = std::pair<std::size_t, bool>;
    EXPECT_EQ(0,  ord.index(c0));
    EXPECT_EQ(1,  ord.index(c1));
    EXPECT_EQ(2,  ord.index(c2));
    EXPECT_EQ(3,  ord.index(c3));

    auto it = ord.begin();
    EXPECT_EQ(c0, *it++);
    EXPECT_EQ(c1, *it++);
    EXPECT_EQ(c2, *it++);
    EXPECT_EQ(c3, *it++);
    EXPECT_EQ(ord.end(), it);
}

TEST_F(variable_order_test, group_from_keys) {
    factory f;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& c3 = f.stream_variable("c3");

    std::vector<variable> cols{ c0, c1, c2, c3 };
    std::vector<variable> keys{ c2, c1 };

    variable_order ord{ variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>, cols, keys};

    EXPECT_TRUE(ord.for_group());
    using pair = std::pair<std::size_t, bool>;
    EXPECT_EQ(pair(0, false), ord.key_value_index(c0));
    EXPECT_EQ(pair(1, true), ord.key_value_index(c1));
    EXPECT_EQ(pair(0, true), ord.key_value_index(c2));
    EXPECT_EQ(pair(1, false), ord.key_value_index(c3));

    auto it = ord.begin();
    EXPECT_EQ(c2, *it++);
    EXPECT_EQ(c1, *it++);
    EXPECT_EQ(ord.end(), it);
    auto it2 = ord.value_begin();
    EXPECT_EQ(c0, *it2++);
    EXPECT_EQ(c3, *it2++);
    EXPECT_EQ(ord.value_end(), it2);
}

TEST_F(variable_order_test, group_from_keys_not_all_keys_in_columns) {
    factory f;
    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& c3 = f.stream_variable("c3");

    std::vector<variable> cols{ c0, c1, c3 };
    std::vector<variable> keys{ c2, c1 };

    variable_order ord{ variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>, cols, keys};

    EXPECT_TRUE(ord.for_group());
    using pair = std::pair<std::size_t, bool>;
    EXPECT_EQ(pair(0, false), ord.key_value_index(c0));
    EXPECT_EQ(pair(0, true), ord.key_value_index(c1));
    EXPECT_EQ(pair(1, false), ord.key_value_index(c3));

    auto it = ord.begin();
    EXPECT_EQ(c1, *it++);
    EXPECT_EQ(ord.end(), it);
    auto it2 = ord.value_begin();
    EXPECT_EQ(c0, *it2++);
    EXPECT_EQ(c3, *it2++);
    EXPECT_EQ(ord.value_end(), it2);
}

}

