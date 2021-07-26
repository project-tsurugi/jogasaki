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
#include <cstddef>
#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/meta/record_meta.h>

#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/mock/basic_record.h>

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

    std::vector<std::size_t> offsets{
        offsetof(S, x_),
        offsetof(S, y_),
        offsetof(S, f1_),
        offsetof(S, f2_),
    };
    S src{};
    record_ref r{&src, sizeof(src)};
    src.x_ = 1;
    src.y_ = 2;
    src.f1_ = 100.0;
    src.f2_ = 200.0;

    std::size_t nullity_bit_base = offsetof(S, n_)*bits_per_byte;
    r.set_null(nullity_bit_base + 0, false);
    r.set_null(nullity_bit_base + 1, false);
    r.set_null(nullity_bit_base + 2, true);
    r.set_null(nullity_bit_base + 3, false);
    std::vector<std::size_t> nullity_offsets{
        nullity_bit_base + 0,
        nullity_bit_base + 1,
        nullity_bit_base + 2,
        nullity_bit_base + 3,
    };

    auto meta = std::make_shared<meta::record_meta>(
            std::vector<field_type>{
                    field_type(field_enum_tag<kind::int4>),
                    field_type(field_enum_tag<kind::int8>),
                    field_type(field_enum_tag<kind::float4>),
                    field_type(field_enum_tag<kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{"1111"s},
            offsets,
            nullity_offsets,
            alignof(S),
            sizeof(S)
    );
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

TEST_F(record_copier_test, layout_by_basic_record) {
    using kind = meta::field_type_kind;
    mock::basic_record rec{mock::create_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::int1>(
        record_meta::nullability_type{"11110"s},
        1, 2, 100.0, 200.0, 0)};
    auto r{rec.ref()};
    auto meta = rec.record_meta();

    std::size_t nullity_bit_base = meta->nullity_offset(0);
    r.set_null(meta->nullity_offset(0), false);
    r.set_null(meta->nullity_offset(1), false);
    r.set_null(meta->nullity_offset(2), true);
    r.set_null(meta->nullity_offset(3), false);

    record_copier copier{meta};
    mock::basic_record dst{mock::create_record<kind::int4, kind::int8, kind::float4, kind::float8, kind::int1>(
        record_meta::nullability_type{"11110"s},
        0, 0, 0.0, 0.0, 0)};
    record_ref t{dst.ref()};
    copier(t, r);

    EXPECT_EQ(1, *t.get_if<std::int32_t>(meta->nullity_offset(0), meta->value_offset(0)));
    EXPECT_EQ(2, *t.get_if<std::int64_t>(meta->nullity_offset(1), meta->value_offset(1)));
    EXPECT_FALSE( t.get_if<float>(meta->nullity_offset(2), meta->value_offset(2)));
    EXPECT_DOUBLE_EQ(200.0,  *t.get_if<double>(meta->nullity_offset(3), meta->value_offset(3)));

    EXPECT_FALSE(t.is_null(meta->nullity_offset(0)));
    EXPECT_FALSE(t.is_null(meta->nullity_offset(1)));
    EXPECT_TRUE(t.is_null(meta->nullity_offset(2)));
    EXPECT_FALSE(t.is_null(meta->nullity_offset(3)));
}

TEST_F(record_copier_test, text) {
    struct S {
        std::int32_t x_{};
        text t1_{};
        text t2_{};
    };
    S src{};
    record_ref r{&src, sizeof(src)};
    mock_memory_resource resource{};
    src.x_ = 1;
    auto s1 = "ABC456789012345"s;
    auto s2 = "ABC4567890123456"s;
    src.t1_ = text{&resource, s1.data(), s1.size()};
    src.t2_ = text{&resource, s2.data(), s2.size()};
    ASSERT_EQ(16, resource.total_bytes_allocated_);

    auto meta = std::make_shared<meta::record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::character>),
            field_type(field_enum_tag<kind::character>),
        },
        boost::dynamic_bitset<std::uint64_t>{"000"s},
        std::vector<size_t>{
            offsetof(S, x_),
            offsetof(S, t1_),
            offsetof(S, t2_),
        },
        std::vector<size_t>{0, 0, 0},
        alignof(S),
        sizeof(S)
    );
    {
        record_copier copier{meta, &resource};
        S dst{};
        record_ref t{&dst, sizeof(dst)};
        copier(t, r);
        ASSERT_EQ(32, resource.total_bytes_allocated_);

        EXPECT_EQ(1, t.get_value<std::int32_t>(meta->value_offset(0)));
        EXPECT_EQ(src.t1_, t.get_value<text>(meta->value_offset(1)));
        EXPECT_EQ(src.t2_, t.get_value<text>(meta->value_offset(2)));
    }
    {
        record_copier shallow_copier{meta};

        S dst{};
        record_ref t{&dst, sizeof(dst)};
        shallow_copier(t, r);
        ASSERT_EQ(32, resource.total_bytes_allocated_);

        EXPECT_EQ(1, t.get_value<std::int32_t>(meta->value_offset(0)));
        EXPECT_EQ(src.t1_, t.get_value<text>(meta->value_offset(1)));
        EXPECT_EQ(src.t2_, t.get_value<text>(meta->value_offset(2)));
    }
}

}

