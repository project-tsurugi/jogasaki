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

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class comparator_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(comparator_test, simple) {
    auto meta = std::make_shared<record_meta>(
            std::vector<field_type>{
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"00"s});

    ASSERT_EQ(0, meta->value_offset(0));
    ASSERT_EQ(8, meta->value_offset(1));
    executor::comparator comp{meta.get()};
    alignas(8) struct {
        std::int32_t x_;
        std::int64_t y_;
    } a, b, c;
    accessor::record_ref r0{&a, sizeof(a)};
    accessor::record_ref r1{&b, sizeof(b)};
    accessor::record_ref r2{&c, sizeof(c)};
    a.x_=1;
    a.y_=1000;
    b.x_=2;
    b.y_=2000;
    c.x_=2;
    c.y_=1000;

    EXPECT_EQ(0, comp(r0, r0));
    EXPECT_EQ(0, comp(r1, r1));
    EXPECT_EQ(0, comp(r2, r2));
    EXPECT_EQ(-1, comp(r0, r1));
    EXPECT_EQ(1, comp(r1, r2));
    EXPECT_EQ(-1, comp(r0, r2));
}

TEST_F(comparator_test, types) {
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int4>),
        },
        boost::dynamic_bitset<std::uint64_t>{"00"s});

    ASSERT_EQ(0, meta->value_offset(0));
    ASSERT_EQ(8, meta->value_offset(1));
    executor::comparator comp{meta.get()};
    struct {
        std::int64_t x_;
        std::int32_t y_;
    } a, b, c;
    accessor::record_ref r0{&a, sizeof(a)};
    accessor::record_ref r1{&b, sizeof(b)};
    accessor::record_ref r2{&c, sizeof(c)};
    a.x_=1;
    a.y_=100;
    b.x_=2;
    b.y_=200;
    c.x_=2;
    c.y_=100;

    EXPECT_EQ(0, comp(r0, r0));
    EXPECT_EQ(0, comp(r1, r1));
    EXPECT_EQ(0, comp(r2, r2));
    EXPECT_EQ(-1, comp(r0, r1));
    EXPECT_EQ(1, comp(r1, r2));
    EXPECT_EQ(-1, comp(r0, r2));
}

}

