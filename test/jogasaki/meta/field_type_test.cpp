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
#include <cstddef>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;
using namespace meta;

class field_type_test : public ::testing::Test {};

TEST_F(field_type_test, default_construct) {
    field_type t{};
    EXPECT_FALSE(t);
}

TEST_F(field_type_test, simple_construct) {
    field_type t{field_enum_tag<field_type_kind::int4>};
    EXPECT_EQ(4, t.runtime_type_size());
    EXPECT_EQ(4, t.runtime_type_alignment());
    EXPECT_TRUE(t);
}

TEST_F(field_type_test, options) {
    field_type t{std::make_shared<decimal_field_option>(5,3)};
    EXPECT_TRUE(t);
    auto opt = t.option<field_type_kind::decimal>();
    EXPECT_NE(0, opt->precision_);
    EXPECT_NE(0, opt->scale_);
}

TEST_F(field_type_test, print) {
    std::cout << decimal_field_option{5,3} << std::endl;
    std::cout << decimal_field_option{std::nullopt,1} << std::endl;
    std::cout << decimal_field_option{} << std::endl;
}

TEST_F(field_type_test, equality_complex_types) {
    field_type t1{std::make_shared<decimal_field_option>(5,3)};
    EXPECT_EQ(t1, t1);
    field_type t2{std::make_shared<decimal_field_option>(5,2)};
    EXPECT_NE(t1, t2);
    field_type t3{std::make_shared<decimal_field_option>(4,3)};
    EXPECT_NE(t1, t3);
    field_type t4{std::make_shared<decimal_field_option>(5,3)};
    EXPECT_EQ(t1, t4);
    field_type t5{std::make_shared<decimal_field_option>(std::nullopt,3)};
    EXPECT_NE(t1, t5);
    field_type t6{std::make_shared<decimal_field_option>(std::nullopt,3)};
    EXPECT_EQ(t5, t6);
}

TEST_F(field_type_test, pointer_type) {
    field_type t{field_enum_tag<field_type_kind::pointer>};
    EXPECT_EQ(8, t.runtime_type_size());
    EXPECT_EQ(8, t.runtime_type_alignment());
    EXPECT_TRUE(t);
}

TEST_F(field_type_test, comparison) {
    field_type i4{field_enum_tag<field_type_kind::int4>};
    field_type i8{field_enum_tag<field_type_kind::int8>};
    field_type dec{std::make_shared<decimal_field_option>(5,3)};
    field_type dat{field_enum_tag<field_type_kind::date>};
    field_type tod{std::make_shared<time_of_day_field_option>(false)};
    field_type todtz{std::make_shared<time_of_day_field_option>(true)};
    field_type tp{std::make_shared<time_point_field_option>(false)};
    field_type tptz{std::make_shared<time_point_field_option>(true)};

    EXPECT_EQ(i4, i4);
    EXPECT_EQ(dec, dec);
    EXPECT_EQ(dat, dat);
    EXPECT_EQ(tod, tod);
    EXPECT_EQ(tp, tp);

    EXPECT_NE(i8, i4);
    EXPECT_NE(dec, i4);
    EXPECT_NE(dec, i4);
    EXPECT_NE(dec, dat);
    EXPECT_NE(tod, dat);
    EXPECT_NE(tp, dat);
    EXPECT_NE(tp, tod);
    EXPECT_NE(tod, todtz);
    EXPECT_NE(tp, tptz);

}
}

