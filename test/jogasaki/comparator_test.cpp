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
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace jogasaki::mock;

class comparator_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(comparator_test, simple) {
    alignas(8) struct S {
        std::int32_t x_;
        std::int64_t y_;
    } a, b, c;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{"00"s},
        std::vector<std::size_t>{
            offsetof(S, x_),
            offsetof(S, y_),
        },
        std::vector<std::size_t>{
            0,
            0,
        },
        alignof(S),
        sizeof(S)
    );

    comparator comp{meta.get()};
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
    alignas(8) struct S {
        std::int64_t i8_;
        std::int32_t i1_;
        std::int32_t i4_;
        std::int32_t i2_;
        double f8_;
        float f4_;
    } a, b, c;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int1>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int2>),
            field_type(enum_tag<kind::float8>),
            field_type(enum_tag<kind::float4>),
        },
        boost::dynamic_bitset<std::uint64_t>{6},
        std::vector<std::size_t>{
            offsetof(S, i8_),
            offsetof(S, i1_),
            offsetof(S, i4_),
            offsetof(S, i2_),
            offsetof(S, f8_),
            offsetof(S, f4_),
        },
        std::vector<std::size_t>(6, 0),
        alignof(S),
        sizeof(S)
    );
    comparator comp{meta.get()};
    accessor::record_ref r0{&a, sizeof(a)};
    accessor::record_ref r1{&b, sizeof(b)};
    accessor::record_ref r2{&c, sizeof(c)};
    a.i8_=1;
    a.i4_=1;
    a.i2_=1;
    a.i1_=1;
    a.f8_=1;
    a.f4_=1;
    b = a;

    EXPECT_EQ(comp(r0, r0), 0);
    b.i1_=2;
    b.i8_=2;
    EXPECT_LT(comp(r0, r1), 0);
    b.i8_=-2;
    EXPECT_GT(comp(r0, r1), 0);
    b.i8_=1;
    b.i1_=1;
    EXPECT_EQ(comp(r0, r1), 0);

    b.i2_=2;
    b.i4_=2;
    EXPECT_LT(comp(r0, r1), 0);
    b.i4_=-2;
    EXPECT_GT(comp(r0, r1), 0);
    b.i4_=1;
    b.i2_=1;
    EXPECT_EQ(comp(r0, r1), 0);

    b.f8_=2;
    b.i2_=2;
    EXPECT_LT(comp(r0, r1), 0);
    b.i2_=-2;
    EXPECT_GT(comp(r0, r1), 0);
    b.i2_=1;
    b.f8_=1;
    EXPECT_EQ(comp(r0, r1), 0);

    b.f4_=2;
    b.f8_=2;
    EXPECT_LT(comp(r0, r1), 0);
    b.f8_=-2;
    EXPECT_GT(comp(r0, r1), 0);
    b.f8_=1;
    b.f4_=1;
    EXPECT_EQ(comp(r0, r1), 0);
}

TEST_F(comparator_test, text) {
    alignas(8) struct S {
        accessor::text x_;
        accessor::text y_;
    } a, b, c;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::character>),
            field_type(enum_tag<kind::character>),
        },
        boost::dynamic_bitset<std::uint64_t>{"00"s},
        std::vector<std::size_t>{
            offsetof(S, x_),
            offsetof(S, y_),
        },
        std::vector<std::size_t>{
            0,
            0,
        },
        alignof(S),
        sizeof(S)
    );

    comparator comp{meta.get()};
    accessor::record_ref r0{&a, sizeof(a)};
    accessor::record_ref r1{&b, sizeof(b)};
    accessor::record_ref r2{&c, sizeof(c)};
    a.x_=accessor::text{"A"};
    a.y_=accessor::text{"AAA"};
    b.x_=accessor::text{"A"};
    b.y_=accessor::text{"BBB"};
    c.x_=accessor::text{"C"};;
    c.y_=accessor::text{"AAA"};

    EXPECT_EQ(comp(r0, r0), 0);
    EXPECT_EQ(comp(r1, r1), 0);
    EXPECT_EQ(comp(r2, r2), 0);
    EXPECT_LT(comp(r0, r1), 0);
    EXPECT_LT(comp(r1, r2), 0);
    EXPECT_LT(comp(r0, r2), 0);
}

TEST_F(comparator_test, nullable) {
    alignas(8) struct S {
        std::int64_t x_;
        std::int64_t y_;
        char n_[1];
    } a, b, c;
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{2}.flip(),
        std::vector<std::size_t>{
            offsetof(S, x_),
            offsetof(S, y_),
        },
        std::vector<std::size_t>{
            offsetof(S, n_)*bits_per_byte,
            offsetof(S, n_)*bits_per_byte+1,
        },
        alignof(S),
        sizeof(S)
    );

    comparator comp{meta.get()};
    accessor::record_ref r0{&a, sizeof(a)};
    accessor::record_ref r1{&b, sizeof(b)};
    accessor::record_ref r2{&c, sizeof(c)};
    a.x_=1;
    a.y_=1000;
    a.n_[0]=3;
    b.x_=1;
    b.y_=1000;
    b.n_[0]=1;
    c.x_=1;
    c.y_=1000;
    c.n_[0]=0;

    EXPECT_EQ(comp(r0, r0), 0);
    EXPECT_EQ(comp(r1, r1), 0);
    EXPECT_EQ(comp(r2, r2), 0);
    EXPECT_LT(comp(r0, r1), 0);
    EXPECT_LT(comp(r1, r2), 0);
    EXPECT_LT(comp(r0, r2), 0);
}

TEST_F(comparator_test, different_meta_between_l_and_r) {
    auto l = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100), {false, true});
    auto l_meta = l.record_meta();
    alignas(8) struct S {
        float x_;
        std::int64_t y_;
        char n_[1];
    } a;
    auto r_meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::float4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{2}.flip(),
        std::vector<std::size_t>{
            offsetof(S, x_),
            offsetof(S, y_),
        },
        std::vector<std::size_t>{
            offsetof(S, n_)*bits_per_byte,
            offsetof(S, n_)*bits_per_byte+1,
        },
        alignof(S),
        sizeof(S)
    );
    a.x_ = 1.0;
    a.y_ = 200;
    a.n_[0] = 2; // a.y_ is null
    auto r = accessor::record_ref(&a, sizeof(S));

    comparator comp{l_meta.get(), r_meta.get()};
    EXPECT_EQ(comp(l.ref(), r), 0);
}

TEST_F(comparator_test, nullable_vs_non_nullable) {
    auto l = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100), {false, false});
    auto l_meta = l.record_meta();
    auto r = create_record<kind::float4, kind::int8>(1.0, 100);
    auto r_meta = r.record_meta();
    auto n = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100), {false, true});

    comparator comp{l_meta.get(), r_meta.get()};
    EXPECT_EQ(comp(l.ref(), r.ref()), 0);
    EXPECT_NE(comp(n.ref(), r.ref()), 0);
}

}

