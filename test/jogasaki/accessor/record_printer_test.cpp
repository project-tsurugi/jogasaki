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
#include <jogasaki/accessor/record_printer.h>

#include <gtest/gtest.h>
#include <takatori/util/enum_tag.h>
#include <takatori/util/string_builder.h>
#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::accessor {

using namespace std::string_view_literals;
using namespace jogasaki::meta;
using namespace takatori::util;

class record_printer_test : public ::testing::Test {
};

TEST_F(record_printer_test, basic) {
    struct {
        std::int64_t x_;
        std::int64_t y_;
        std::int64_t z_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(24, r.size());
    buffer.x_ = 1;
    buffer.y_ = 2;
    buffer.z_ = 3;

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int8>)
        },
        boost::dynamic_bitset<std::uint64_t>{3}};
    EXPECT_EQ(3, meta.field_count());
    EXPECT_EQ(1, r.get_value<std::int64_t>(0));
    EXPECT_EQ(2, r.get_value<std::int64_t>(8));
    EXPECT_EQ(3, r.get_value<std::int64_t>(16));

    std::cout << r << meta;
    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int8)[1] (1:int8)[2] (2:int8)[3]", ss.str());
}

TEST_F(record_printer_test, integers) {
    struct {
        std::int32_t i1_;
        std::int32_t i2_;
        std::int32_t i3_;
        std::int64_t i4_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(24, r.size());
    buffer.i1_ = 1;
    buffer.i2_ = 2;
    buffer.i3_ = 3;
    buffer.i4_ = 4;

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(enum_tag<kind::int1>),
            field_type(enum_tag<kind::int2>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{4}};
    EXPECT_EQ(4, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int1)[1] (1:int2)[2] (2:int4)[3] (3:int8)[4]", ss.str());
}

TEST_F(record_printer_test, floats) {
    struct {
        float f1_;
        double f2_;
        float f3_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(24, r.size());
    buffer.f1_ = 1.0;
    buffer.f2_ = 2.0;
    buffer.f3_ = 3.0;

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(enum_tag<kind::float4>),
            field_type(enum_tag<kind::float8>),
            field_type(enum_tag<kind::float4>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}};
    EXPECT_EQ(3, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:float4)[1] (1:float8)[2] (2:float4)[3]", ss.str());
}

TEST_F(record_printer_test, text) {
    mock_memory_resource resource;
    std::string s2{"A234567890123456"};
    std::string s4{"A23456789012345"};
    text t2{&resource, s2.data(), s2.size()};
    text t4{&resource, s4.data(), s4.size()};
    struct {
        std::int32_t i1_;
        accessor::text t2_;
        std::int64_t i3_;
        accessor::text t4_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(48, r.size());
    buffer.i1_ = 1;
    buffer.t2_ = t2;
    buffer.i3_ = 3;
    buffer.t4_ = t4;

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::character>),
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::character>),
        },
        boost::dynamic_bitset<std::uint64_t>{4}};
    EXPECT_EQ(4, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int4)[1] (1:character)[A234567890123456] (2:int8)[3] (3:character)[A23456789012345]", ss.str());
}

}
