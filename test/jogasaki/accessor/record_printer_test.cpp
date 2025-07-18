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
#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <gtest/gtest.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::accessor {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace jogasaki::meta;
using namespace takatori::util;

class record_printer_test : public ::testing::Test {
};

TEST_F(record_printer_test, simple) {
    struct {
        std::int64_t x_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(8, r.size());
    buffer.x_ = 1;

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{1}};
    EXPECT_EQ(1, meta.field_count());
    EXPECT_EQ(1, r.get_value<std::int64_t>(0));

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int8)[1]", ss.str());

    std::stringstream ss2{};
    auto printer = ss2 << r;
    ASSERT_EQ("", ss2.str());
    printer << meta;
    ASSERT_EQ("(0:int8)[1]", ss2.str());
}

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
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>)
        },
        boost::dynamic_bitset<std::uint64_t>{3}};
    EXPECT_EQ(3, meta.field_count());
    EXPECT_EQ(1, r.get_value<std::int64_t>(0));
    EXPECT_EQ(2, r.get_value<std::int64_t>(8));
    EXPECT_EQ(3, r.get_value<std::int64_t>(16));

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int8)[1] (1:int8)[2] (2:int8)[3]", ss.str());
    std::stringstream ss2{};
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
            field_type(field_enum_tag<kind::int1>),
            field_type(field_enum_tag<kind::int2>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::int8>),
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
            field_type(field_enum_tag<kind::float4>),
            field_type(field_enum_tag<kind::float8>),
            field_type(field_enum_tag<kind::float4>),
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
            field_type(field_enum_tag<kind::int4>),
            field_type(std::make_shared<meta::character_field_option>()),
            field_type(field_enum_tag<kind::int8>),
            field_type(std::make_shared<meta::character_field_option>()),
        },
        boost::dynamic_bitset<std::uint64_t>{4}};
    EXPECT_EQ(4, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int4)[1] (1:character varying(*))[A234567890123456] (2:int8)[3] (3:character varying(*))[A23456789012345]", ss.str());
}

TEST_F(record_printer_test, nullable) {
    struct S {
        std::int64_t x_;
        std::int64_t y_;
        std::int64_t z_;
        std::int64_t nullity_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    buffer.x_ = 1;
    buffer.y_ = 2;
    buffer.z_ = 3;
    buffer.nullity_ = 1;

    auto nullity_base = offsetof(S, nullity_)*bits_per_byte;
    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int8>)
        },
        boost::dynamic_bitset<std::uint64_t>{"101"s},
        std::vector<std::size_t>{
            offsetof(S, x_),
            offsetof(S, y_),
            offsetof(S, z_),
        },
        std::vector<std::size_t>{
            nullity_base,
            nullity_base+1,
            nullity_base+2,
        },
        alignof(S),
        sizeof(S)
    };
    EXPECT_EQ(3, meta.field_count());
    EXPECT_EQ(1, r.get_value<std::int64_t>(meta.value_offset(0)));
    EXPECT_EQ(2, r.get_value<std::int64_t>(meta.value_offset(1)));
    EXPECT_EQ(3, r.get_value<std::int64_t>(meta.value_offset(2)));

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:int8*)[-] (1:int8)[2] (2:int8*)[3]", ss.str());
}

TEST_F(record_printer_test, empty_object) {
    record_ref r{};
    ASSERT_EQ(0, r.size());

    using kind = field_type_kind;
    record_meta meta{};
    EXPECT_EQ(0, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("<null record>", ss.str());

    std::stringstream ss2{};
    auto printer = ss2 << r;
    ASSERT_EQ("", ss2.str());
    printer << meta;
    ASSERT_EQ("<null record>", ss2.str());
}

TEST_F(record_printer_test, temporal_types) {
    struct {
        rtype<ft::date> date_;
        rtype<ft::time_of_day> time_of_day_;
        rtype<ft::time_point> time_point_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(32, r.size());
    buffer.date_ = rtype<ft::date>{1};
    buffer.time_of_day_ = rtype<ft::time_of_day>{2ns};
    buffer.time_point_ = rtype<ft::time_point>{3ns};

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(field_enum_tag<kind::date>),
            field_type(std::make_shared<time_of_day_field_option>()),
            field_type(std::make_shared<time_point_field_option>()),
        },
        boost::dynamic_bitset<std::uint64_t>{3}};
    EXPECT_EQ(3, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:date)[date(1970-1-2)] (1:time_of_day(with_offset=0))[time_of_day(00:00:00.000000002)] (2:time_point(with_offset=0))[time_point(1970-1-1 00:00:00.000000003)]", ss.str());
}

TEST_F(record_printer_test, decimal) {
    struct {
        rtype<ft::decimal> decimal0_;
        rtype<ft::decimal> decimal1_;
        rtype<ft::decimal> decimal2_;
    } buffer;
    record_ref r{&buffer, sizeof(buffer)};
    ASSERT_EQ(32*3, r.size());
    buffer.decimal0_ = rtype<ft::decimal>{1};
    buffer.decimal1_ = rtype<ft::decimal>{100};
    buffer.decimal2_ = rtype<ft::decimal>{999999, -3};

    using kind = field_type_kind;
    record_meta meta{
        std::vector<field_type>{
            field_type(std::make_shared<decimal_field_option>(6,3)),
            field_type(std::make_shared<decimal_field_option>(6,3)),
            field_type(std::make_shared<decimal_field_option>(6,3)),
        },
        boost::dynamic_bitset<std::uint64_t>{3}};
    EXPECT_EQ(3, meta.field_count());

    std::stringstream ss{};
    ss << r << meta;
    ASSERT_EQ("(0:decimal(6, 3))[1] (1:decimal(6, 3))[100] (2:decimal(6, 3))[999.999]", ss.str());
}
}

