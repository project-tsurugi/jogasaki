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

#include <jogasaki/executor/exchange/group/shuffle_info.h>

namespace jogasaki::executor::exchange::group {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace executor;
using namespace takatori::util;

class shuffle_info_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(shuffle_info_test, simple) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(enum_tag<kind::int1>),
            field_type(enum_tag<kind::int2>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>("0001"s));
    shuffle_info info{rec_meta, {1}};
    auto key_meta = info.key_meta();
    EXPECT_EQ(1, key_meta->field_count());
    EXPECT_EQ(3, info.value_meta()->field_count());
}

TEST_F(shuffle_info_test, mutiple_key_fields) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(enum_tag<kind::int1>),
            field_type(enum_tag<kind::int2>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>("0001"s));
    shuffle_info info{rec_meta, {3,0,1}};
    auto key_meta = info.key_meta();
    ASSERT_EQ(3, key_meta->field_count());
    EXPECT_EQ(1, info.value_meta()->field_count());
    EXPECT_EQ(field_type{enum_tag<kind::int8>}, key_meta->at(0));
    EXPECT_EQ(field_type{enum_tag<kind::int1>}, key_meta->at(1));
    EXPECT_EQ(field_type{enum_tag<kind::int2>}, key_meta->at(2));
    EXPECT_FALSE(key_meta->nullable(0));
    EXPECT_TRUE(key_meta->nullable(1));
    EXPECT_FALSE(key_meta->nullable(2));
}

TEST_F(shuffle_info_test, nullability_for_value) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(enum_tag<kind::int1>),
            field_type(enum_tag<kind::int2>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
    },boost::dynamic_bitset<std::uint64_t>("0001"s));
    shuffle_info info{rec_meta, {2}};
    auto value_meta = info.value_meta();
    ASSERT_EQ(1, info.key_meta()->field_count());
    EXPECT_EQ(3, info.value_meta()->field_count());
    EXPECT_EQ(field_type{enum_tag<kind::int1>}, value_meta->at(0));
    EXPECT_EQ(field_type{enum_tag<kind::int2>}, value_meta->at(1));
    EXPECT_EQ(field_type{enum_tag<kind::int8>}, value_meta->at(2));
    EXPECT_TRUE(value_meta->nullable(0));
    EXPECT_FALSE(value_meta->nullable(1));
    EXPECT_FALSE(value_meta->nullable(2));
}
}

