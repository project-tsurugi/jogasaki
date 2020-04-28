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
#include <meta/record_meta.h>

namespace dc::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class record_meta_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(record_meta_test, single_field) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
            },
            boost::dynamic_bitset<std::uint64_t>{"1"s}};

    EXPECT_EQ(1, meta.field_count());
    EXPECT_TRUE(meta.nullable(0));
    EXPECT_EQ(field_type(enum_tag<kind::int1>), meta[0]);
    EXPECT_NE(field_type(enum_tag<kind::int4>), meta[0]);
    EXPECT_EQ(4,  meta.record_alignment());
    EXPECT_EQ(8,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(4*8,  meta.nullity_offset(0));
}

TEST_F(record_meta_test, non_nullables) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int8>),
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::character>),
            },
            boost::dynamic_bitset<std::uint64_t>{4}};
    EXPECT_EQ(4, meta.field_count());
    EXPECT_FALSE(meta.nullable(0));
    EXPECT_FALSE(meta.nullable(1));
    EXPECT_EQ(field_type(enum_tag<kind::int4>), meta[0]);
    EXPECT_EQ(8,  meta.record_alignment());
    EXPECT_EQ(40,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(8,  meta.value_offset(1));
    EXPECT_EQ(16,  meta.value_offset(2));
    EXPECT_EQ(24,  meta.value_offset(3));
}

TEST_F(record_meta_test, multiple_nullable_fields) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
                    field_type(enum_tag<kind::int2>),
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"0101"s}};// right most one is bs[0]
    EXPECT_EQ(4, meta.field_count());
    EXPECT_TRUE(meta.nullable(0));
    EXPECT_FALSE(meta.nullable(1));
    EXPECT_TRUE(meta.nullable(2));
    EXPECT_FALSE(meta.nullable(3));
    EXPECT_EQ(field_type(enum_tag<kind::int1>), meta[0]);
    EXPECT_EQ(field_type(enum_tag<kind::int2>), meta[1]);
    EXPECT_EQ(field_type(enum_tag<kind::int4>), meta[2]);
    EXPECT_EQ(field_type(enum_tag<kind::int8>), meta[3]);
    EXPECT_EQ(8,  meta.record_alignment());
    EXPECT_EQ(32,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(4,  meta.value_offset(1));
    EXPECT_EQ(8,  meta.value_offset(2));
    EXPECT_EQ(16,  meta.value_offset(3));
    EXPECT_EQ(24*8,  meta.nullity_offset(0));
    EXPECT_EQ(24*8+1,  meta.nullity_offset(2));
}

TEST_F(record_meta_test, 16_nullable_fields) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
            },
            boost::dynamic_bitset<std::uint64_t>{16}.flip()};
    EXPECT_EQ(16, meta.field_count());
    EXPECT_TRUE(meta.nullable(0));
    EXPECT_TRUE(meta.nullable(15));
    EXPECT_EQ(field_type(enum_tag<kind::boolean>), meta[0]);
    EXPECT_EQ(field_type(enum_tag<kind::boolean>), meta[15]);
    EXPECT_EQ(1,  meta.record_alignment());
    EXPECT_EQ(18,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(15,  meta.value_offset(15));
    EXPECT_EQ(16*8,  meta.nullity_offset(0));
    EXPECT_EQ(16*8+15,  meta.nullity_offset(15));
}

TEST_F(record_meta_test, 17_nullable_fields) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::boolean>),
            },
            boost::dynamic_bitset<std::uint64_t>{17}.flip()};
    EXPECT_EQ(17, meta.field_count());
    EXPECT_TRUE(meta.nullable(0));
    EXPECT_TRUE(meta.nullable(16));
    EXPECT_EQ(field_type(enum_tag<kind::boolean>), meta[0]);
    EXPECT_EQ(field_type(enum_tag<kind::boolean>), meta[16]);
    EXPECT_EQ(1,  meta.record_alignment());
    EXPECT_EQ(20,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(16,  meta.value_offset(16));
    EXPECT_EQ(17*8,  meta.nullity_offset(0));
    EXPECT_EQ(17*8+16,  meta.nullity_offset(16));
}

TEST_F(record_meta_test, type_variaties) {
    record_meta meta{
            std::vector<field_type>{
                    field_type(enum_tag<kind::boolean>),
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int1>),
                    field_type(enum_tag<kind::int2>),
                    field_type(enum_tag<kind::int8>),
                    field_type(enum_tag<kind::character>),
                    field_type(enum_tag<kind::float4>),
                    field_type(enum_tag<kind::float8>),
                    field_type(enum_tag<kind::decimal>),
            },
            boost::dynamic_bitset<std::uint64_t>{"101010101"s}};
    EXPECT_EQ(9, meta.field_count());
    EXPECT_TRUE(meta.nullable(0));
    EXPECT_FALSE(meta.nullable(1));
    EXPECT_TRUE(meta.nullable(2));
    EXPECT_FALSE(meta.nullable(3));
    EXPECT_TRUE(meta.nullable(4));
    EXPECT_FALSE(meta.nullable(5));
    EXPECT_TRUE(meta.nullable(6));
    EXPECT_FALSE(meta.nullable(7));
    EXPECT_TRUE(meta.nullable(8));
    EXPECT_EQ(field_type(enum_tag<kind::boolean>), meta[0]);
    EXPECT_EQ(field_type(enum_tag<kind::int4>), meta[1]);
    EXPECT_EQ(field_type(enum_tag<kind::int1>), meta[2]);
    EXPECT_EQ(field_type(enum_tag<kind::int2>), meta[3]);
    EXPECT_EQ(field_type(enum_tag<kind::int8>), meta[4]);
    EXPECT_EQ(field_type(enum_tag<kind::character>), meta[5]);
    EXPECT_EQ(field_type(enum_tag<kind::float4>), meta[6]);
    EXPECT_EQ(field_type(enum_tag<kind::float8>), meta[7]);
    EXPECT_EQ(field_type(enum_tag<kind::decimal>), meta[8]);
    EXPECT_EQ(8,  meta.record_alignment());
    EXPECT_EQ(80,  meta.record_size());
    EXPECT_EQ(0,  meta.value_offset(0));
    EXPECT_EQ(4,  meta.value_offset(1));
    EXPECT_EQ(8,  meta.value_offset(2));
    EXPECT_EQ(12,  meta.value_offset(3));
    EXPECT_EQ(16,  meta.value_offset(4));
    EXPECT_EQ(24,  meta.value_offset(5));
    EXPECT_EQ(40,  meta.value_offset(6));
    EXPECT_EQ(48,  meta.value_offset(7));
    EXPECT_EQ(56,  meta.value_offset(8));
    EXPECT_EQ(72*8,  meta.nullity_offset(0));
    EXPECT_EQ(72*8+1,  meta.nullity_offset(2));
    EXPECT_EQ(72*8+2,  meta.nullity_offset(4));
    EXPECT_EQ(72*8+3,  meta.nullity_offset(6));
    EXPECT_EQ(72*8+4,  meta.nullity_offset(8));
}

TEST_F(record_meta_test, equality1) {
    record_meta r_int1{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
            },
            boost::dynamic_bitset<std::uint64_t>{"1"s}};

    record_meta r_int1_2{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
                    field_type(enum_tag<kind::int1>),
            },
            boost::dynamic_bitset<std::uint64_t>{"11"s}};

    record_meta r_int1_int2{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
                    field_type(enum_tag<kind::int2>),
            },
            boost::dynamic_bitset<std::uint64_t>{"11"s}};

    record_meta r_int1_non_nullable{
            std::vector<field_type>{
                    field_type(enum_tag<kind::int1>),
            },
            boost::dynamic_bitset<std::uint64_t>{"0"s}};

    EXPECT_EQ(r_int1, r_int1);
    EXPECT_EQ(r_int1_2, r_int1_2);
    EXPECT_EQ(r_int1_int2, r_int1_int2);

    EXPECT_NE(r_int1, r_int1_2);
    EXPECT_NE(r_int1, r_int1_int2);
    EXPECT_NE(r_int1, r_int1_non_nullable);

}

TEST_F(record_meta_test, equality_with_options) {
    // FIXME using date_field_option temporal implementation
    record_meta r_date_0{
            std::vector<field_type>{
                    field_type(std::make_shared<date_field_option>(0)),
            },
            boost::dynamic_bitset<std::uint64_t>{"1"s}};
    record_meta r_date_1{
            std::vector<field_type>{
                    field_type(std::make_shared<date_field_option>(1)),
            },
            boost::dynamic_bitset<std::uint64_t>{"1"s}};

    EXPECT_EQ(r_date_0, r_date_0);
    EXPECT_EQ(r_date_1, r_date_1);
    EXPECT_NE(r_date_0, r_date_1);
    EXPECT_NE(r_date_1, r_date_0);
}

}

