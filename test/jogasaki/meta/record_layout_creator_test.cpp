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
#include <jogasaki/meta/impl/record_layout_creator.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

namespace jogasaki::meta::impl {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class record_layout_creator_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(record_layout_creator_test, single_field) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::int1>),
            },
            boost::dynamic_bitset<std::uint64_t>{"1"s}};

    EXPECT_EQ(4,  c.record_alignment());
    EXPECT_EQ(8,  c.record_size());
    EXPECT_EQ(4,  c.value_offset_table()[0]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
}

TEST_F(record_layout_creator_test, non_nullables) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::int4>),
                    field_type(field_enum_tag<kind::int8>),
                    field_type(field_enum_tag<kind::int4>),
                    field_type(field_enum_tag<kind::character>),
            },
            boost::dynamic_bitset<std::uint64_t>{4}};
    EXPECT_EQ(8,  c.record_alignment());
    EXPECT_EQ(40,  c.record_size());
    EXPECT_EQ(0,  c.value_offset_table()[0]);
    EXPECT_EQ(8,  c.value_offset_table()[1]);
    EXPECT_EQ(16,  c.value_offset_table()[2]);
    EXPECT_EQ(24,  c.value_offset_table()[3]);
}

TEST_F(record_layout_creator_test, multiple_nullable_fields) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::int1>),
                    field_type(field_enum_tag<kind::int2>),
                    field_type(field_enum_tag<kind::int4>),
                    field_type(field_enum_tag<kind::int8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"0101"s}};// right most one is bs[0]
    EXPECT_EQ(8,  c.record_alignment());
    EXPECT_EQ(24,  c.record_size());
    EXPECT_EQ(4,  c.value_offset_table()[0]);
    EXPECT_EQ(8,  c.value_offset_table()[1]);
    EXPECT_EQ(12,  c.value_offset_table()[2]);
    EXPECT_EQ(16,  c.value_offset_table()[3]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
    EXPECT_EQ(1,  c.nullity_offset_table()[2]);
}

TEST_F(record_layout_creator_test, 16_nullable_fields) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
            },
            boost::dynamic_bitset<std::uint64_t>{16}.flip()};
    EXPECT_EQ(1,  c.record_alignment());
    EXPECT_EQ(18,  c.record_size());
    EXPECT_EQ(2,  c.value_offset_table()[0]);
    EXPECT_EQ(17,  c.value_offset_table()[15]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
    EXPECT_EQ(15,  c.nullity_offset_table()[15]);
}

TEST_F(record_layout_creator_test, 17_nullable_fields) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::boolean>),
            },
            boost::dynamic_bitset<std::uint64_t>{17}.flip()};
    EXPECT_EQ(1,  c.record_alignment());
    EXPECT_EQ(20,  c.record_size());
    EXPECT_EQ(3,  c.value_offset_table()[0]);
    EXPECT_EQ(19,  c.value_offset_table()[16]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
    EXPECT_EQ(16,  c.nullity_offset_table()[16]);
}

TEST_F(record_layout_creator_test, type_variaties) {
    record_layout_creator c{
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::boolean>),
                    field_type(field_enum_tag<kind::int4>),
                    field_type(field_enum_tag<kind::int1>),
                    field_type(field_enum_tag<kind::int2>),
                    field_type(field_enum_tag<kind::int8>),
                    field_type(field_enum_tag<kind::character>),
                    field_type(field_enum_tag<kind::float4>),
                    field_type(field_enum_tag<kind::float8>),
                    field_type(std::make_shared<decimal_field_option>()),
            },
            boost::dynamic_bitset<std::uint64_t>{"101010101"s}};
    EXPECT_EQ(8,  c.record_alignment());
    EXPECT_EQ(88,  c.record_size());
    EXPECT_EQ(1,  c.value_offset_table()[0]);
    EXPECT_EQ(4,  c.value_offset_table()[1]);
    EXPECT_EQ(8,  c.value_offset_table()[2]);
    EXPECT_EQ(12,  c.value_offset_table()[3]);
    EXPECT_EQ(16,  c.value_offset_table()[4]);
    EXPECT_EQ(24,  c.value_offset_table()[5]);
    EXPECT_EQ(40,  c.value_offset_table()[6]);
    EXPECT_EQ(48,  c.value_offset_table()[7]);
    EXPECT_EQ(56,  c.value_offset_table()[8]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
    EXPECT_EQ(1,  c.nullity_offset_table()[2]);
    EXPECT_EQ(2,  c.nullity_offset_table()[4]);
    EXPECT_EQ(3,  c.nullity_offset_table()[6]);
    EXPECT_EQ(4,  c.nullity_offset_table()[8]);
}

TEST_F(record_layout_creator_test, temporal_types) {
    record_layout_creator c{
        std::vector<field_type>{
            field_type(field_enum_tag<kind::date>),
            field_type(std::make_shared<meta::time_of_day_field_option>()),
            field_type(std::make_shared<meta::time_point_field_option>()),
            field_type(field_enum_tag<kind::date>),
        },
        boost::dynamic_bitset<std::uint64_t>{"0101"s}};
    EXPECT_EQ(8,  c.record_alignment());
    EXPECT_EQ(48,  c.record_size());
    EXPECT_EQ(8,  c.value_offset_table()[0]);
    EXPECT_EQ(16,  c.value_offset_table()[1]);
    EXPECT_EQ(24,  c.value_offset_table()[2]);
    EXPECT_EQ(40,  c.value_offset_table()[3]);
    EXPECT_EQ(0,  c.nullity_offset_table()[0]);
    EXPECT_EQ(1,  c.nullity_offset_table()[2]);
}
}

