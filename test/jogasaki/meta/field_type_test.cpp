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
#include <jogasaki/meta/field_type.h>

#include <gtest/gtest.h>
#include <takatori/util/enum_tag.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;
using namespace meta;

class field_type_test : public ::testing::Test {};

TEST_F(field_type_test, default_construct) {
    field_type t{};
    EXPECT_FALSE(t);
}

TEST_F(field_type_test, simple_construct) {
    field_type t{takatori::util::enum_tag<field_type_kind::int4>};
    EXPECT_EQ(4, t.runtime_type_size());
    EXPECT_EQ(4, t.runtime_type_alignment());
    EXPECT_TRUE(t);
}

TEST_F(field_type_test, options) {
    field_type t{std::make_shared<array_field_option>(1)};
    EXPECT_TRUE(t);
    auto opt = t.option<field_type_kind::array>();
    EXPECT_NE(0, opt->size_);
}

TEST_F(field_type_test, equality_complex_types) {
    field_type t1{std::make_shared<array_field_option>(100UL)};
    EXPECT_EQ(t1, t1);
    field_type t2{std::make_shared<array_field_option>(200UL)};
    EXPECT_NE(t1, t2);
}

}

