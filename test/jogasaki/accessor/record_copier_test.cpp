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

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/meta/record_meta.h>

#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;
using namespace accessor;
using namespace meta;
using namespace takatori::util;

using namespace boost::container::pmr;
using namespace std::string_literals;

using kind = field_type_kind;

class record_copier_test : public ::testing::Test {
public:
};

TEST_F(record_copier_test, simple) {
    struct S {
        std::int32_t x_{};
        std::int64_t y_{};
        float f1_{};
        double f2_{};
        char n_[1]{};
    };
    S src{};
    static_assert(sizeof(S) == 40);
    record_ref r{&src, sizeof(src)};
    ASSERT_EQ(40, r.size());
    src.x_ = 1;
    src.y_ = 2;
    src.f1_ = 100.0;
    src.f2_ = 200.0;

    std::size_t nullity_bit_base = 32*8;
    r.set_null(nullity_bit_base + 0, false);
    r.set_null(nullity_bit_base + 1, false);
    r.set_null(nullity_bit_base + 2, true);
    r.set_null(nullity_bit_base + 3, false);

    auto meta = std::make_shared<meta::record_meta>(
            std::vector<field_type>{
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::int8>),
                    field_type(enum_tag<kind::float4>),
                    field_type(enum_tag<kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"1111"s});
    ASSERT_EQ(40, meta->record_size());
    record_copier copier{meta};
    S dst{};
    record_ref t{&dst, sizeof(dst)};
    copier(t, r);

    EXPECT_EQ(1, *t.get_if<std::int32_t>(nullity_bit_base+0, 0));
    EXPECT_EQ(2, *t.get_if<std::int64_t>(nullity_bit_base+1, 8));
    EXPECT_FALSE( t.get_if<float>(nullity_bit_base+2, 16));
    EXPECT_DOUBLE_EQ(200.0,  *t.get_if<double>(nullity_bit_base+3, 24));

    EXPECT_FALSE(t.is_null(nullity_bit_base+0));
    EXPECT_FALSE(t.is_null(nullity_bit_base+1));
    EXPECT_TRUE(t.is_null(nullity_bit_base+2));
    EXPECT_FALSE(t.is_null(nullity_bit_base+3));
}

TEST_F(record_copier_test, text) {
    struct S {
        std::int64_t x_{};
        text t1_{};
        text t2_{};
    };
    S src{};
    static_assert(sizeof(S) == 40);
    record_ref r{&src, sizeof(src)};
    ASSERT_EQ(40, r.size());
    mock_memory_resource resource{};
    src.x_ = 1;
    auto s1 = "ABC456789012345"s;
    auto s2 = "ABC4567890123456"s;
    src.t1_ = text{&resource, s1.data(), s1.size()};
    src.t2_ = text{&resource, s2.data(), s2.size()};
    ASSERT_EQ(16, resource.total_);

    auto meta = std::make_shared<meta::record_meta>(
            std::vector<field_type>{
                    field_type(enum_tag<kind::int4>),
                    field_type(enum_tag<kind::character>),
                    field_type(enum_tag<kind::character>),
            },
            boost::dynamic_bitset<std::uint64_t>{"000"s});
    ASSERT_EQ(40, meta->record_size());
    record_copier copier{meta, &resource};

    S dst{};
    record_ref t{&dst, sizeof(dst)};
    copier(t, r);
    ASSERT_EQ(32, resource.total_);

    EXPECT_EQ(1, t.get_value<std::int32_t>(0));
    EXPECT_EQ(src.t1_, t.get_value<text>(8));
    EXPECT_EQ(src.t2_, t.get_value<text>(24));
}



}

