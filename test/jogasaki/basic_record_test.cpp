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
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/record.h>
#include <jogasaki/test_utils/types.h>

#include "test_root.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

class basic_record_test : public test_root {};

using kind = field_type_kind;

using namespace jogasaki::mock;

TEST_F(basic_record_test, simple) {
    basic_record r{create_record<kind::int4>(2)};

    test::record rec{1, 100.0};
    EXPECT_EQ(1, rec.key());
    EXPECT_EQ(100.0, rec.value());
}

TEST_F(basic_record_test, meta) {
    {
        basic_record r{create_record<kind::int4>()};
        auto meta = r.record_meta();
        EXPECT_EQ(1, meta->field_count());
        EXPECT_EQ(meta::field_type{meta::field_enum_tag<kind::int4>}, meta->at(0));
    }
    {
        basic_record r{create_record<kind::int4, kind::int8>()};
        auto meta = r.record_meta();
        EXPECT_EQ(2, meta->field_count());
        EXPECT_EQ(meta::field_type{meta::field_enum_tag<kind::int4>}, meta->at(0));
        EXPECT_EQ(meta::field_type{meta::field_enum_tag<kind::int8>}, meta->at(1));
    }
}

TEST_F(basic_record_test, default_metadata) {
    basic_record r{create_record<kind::float4, kind::int8>(1.0, 100)};
    basic_record r2{r.ref(), create_meta<kind::float4, kind::int8>()};
    auto meta = r2.record_meta();

    EXPECT_EQ(1.0, r2.ref().get_value<float>(meta->value_offset(0)));
    EXPECT_EQ(100, r2.ref().get_value<std::int64_t>(meta->value_offset(1)));
}

TEST_F(basic_record_test, share_metadata) {
    basic_record r{create_record<kind::int4>(1)};
    auto meta = r.record_meta();
    basic_record r2{create_record<kind::int4>(meta, 2)};
    auto meta2 = r2.record_meta();
    EXPECT_EQ(1, meta2->field_count());
    EXPECT_EQ(meta::field_type{meta::field_enum_tag<kind::int4>}, meta2->at(0));
    EXPECT_EQ(meta, meta2);
}

TEST_F(basic_record_test, nullable) {
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(1));
        EXPECT_TRUE(r.is_nullable(0));
        EXPECT_TRUE(r.is_nullable(1));
        EXPECT_FALSE( r.ref().is_null(meta->nullity_offset(0)));
        EXPECT_FALSE( r.ref().is_null(meta->nullity_offset(1)));
    }
    {
        auto r = create_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_FALSE(meta->nullable(0));
        EXPECT_FALSE(meta->nullable(1));
        EXPECT_FALSE(r.is_nullable(0));
        EXPECT_FALSE(r.is_nullable(1));
        EXPECT_FALSE( r.ref().is_null(meta->nullity_offset(0)));
        EXPECT_FALSE( r.ref().is_null(meta->nullity_offset(1)));
    }
    {
        auto r1 = create_nullable_record<kind::float4, kind::int8>(1.0, 100);
        auto r2 = create_record<kind::float4, kind::int8>(1.0, 100);
        EXPECT_EQ(r1, r2);
    }
}

TEST_F(basic_record_test, compare) {
    auto r1 = create_record<kind::float4, kind::int8>(1.0, 100);
    auto r2 = create_record<kind::float4, kind::int8>(1.0, 100);
    EXPECT_EQ(r1, r2);
    auto r3 = create_record<kind::float4, kind::int8>(1.0, 101);
    EXPECT_NE(r1, r3);

    EXPECT_LT(r1, r3);
    EXPECT_GT(r3, r1);
}

TEST_F(basic_record_test, nullity) {
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100), {false, true});
        EXPECT_TRUE(r.is_nullable(0));
        EXPECT_TRUE(r.is_nullable(1));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(0)));
        EXPECT_TRUE(r.ref().is_null(r.record_meta()->nullity_offset(1)));
        EXPECT_FALSE(r.is_null(0));
        EXPECT_TRUE(r.is_null(1));
        EXPECT_DOUBLE_EQ(1.0, r.get_value<float>(0));
        EXPECT_DOUBLE_EQ(1.0, *r.get_if<float>(0));
        EXPECT_FALSE(r.get_if<std::int64_t>(1));
    }
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100));
        EXPECT_TRUE(r.is_nullable(0));
        EXPECT_TRUE(r.is_nullable(1));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(0)));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(1)));
        EXPECT_FALSE(r.is_null(0));
        EXPECT_FALSE(r.is_null(1));
        EXPECT_DOUBLE_EQ(1.0, r.get_value<float>(0));
        EXPECT_DOUBLE_EQ(1.0, *r.get_if<float>(0));
        EXPECT_EQ(100, *r.get_if<std::int64_t>(1));
        EXPECT_EQ(100, r.get_value<std::int64_t>(1));
    }
}

TEST_F(basic_record_test, pointer_field) {
    // internal fields should be ignored on comparison
    auto r1 = create_record<kind::float4, kind::int8, kind::pointer>(1.0, 100, nullptr);
    auto r2 = create_record<kind::float4, kind::int8, kind::pointer>(1.0, 100, (void*)1);
    EXPECT_EQ(r1, r2);

    EXPECT_FALSE(r1 < r2);
    EXPECT_FALSE(r1 > r2);
}

TEST_F(basic_record_test, text) {
    {
        std::string data("12345678901234567890");
        basic_record rec{create_record<kind::character>(accessor::text(data.data(), data.size()))};
        basic_record copy{rec.ref(), rec.record_meta()};
        EXPECT_EQ(rec, copy);
        data.front() = 'A';
        EXPECT_EQ(rec, copy);
    }
    {
        std::string data("12345678901234567890");
        basic_record rec{create_record<kind::character>(accessor::text(data.data(), data.size()))};
        memory::lifo_paged_memory_resource resource(&global::page_pool());
        basic_record copy{rec.ref(), rec.record_meta(), &resource};
        EXPECT_EQ(rec, copy);
        data.front() = 'A';
        EXPECT_NE(rec, copy);
    }
}

TEST_F(basic_record_test, allocate_varlen) {
    std::string data("12345678901234567890");
    basic_record rec{};
    auto sv = rec.allocate_varlen_data(data);
    EXPECT_EQ(sv, data);
    EXPECT_NE(sv.data(), data.data());
}

TEST_F(basic_record_test, field_size) {
    EXPECT_LE(sizeof(rtype<ft::int1>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::int2>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::int4>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::int8>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::float4>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::float8>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::character>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::decimal>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::date>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::time_of_day>), mock::basic_record_field_size);
    EXPECT_LE(sizeof(rtype<ft::time_point>), mock::basic_record_field_size);
}

TEST_F(basic_record_test, compare_decimal) {
    auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 1230, -3)});
    auto r2 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 123, -2)});
    EXPECT_EQ(r1, r2);
    auto r3 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 1231, -3)});
    EXPECT_NE(r1, r3);

    EXPECT_LT(r1, r3);
    EXPECT_GT(r3, r1);
}

TEST_F(basic_record_test, compare_different_scale_decimal) {
    // verify basic_record compares decimal values correctly even if the scale is different
    auto fm0 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    auto fm1 = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm0}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0)});
    auto r2 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm1}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 0, 0)});
    EXPECT_NE(r1, r2);
}

TEST_F(basic_record_test, lob_types) {
    // regression testcase - when adding reference_tag to lob_reference
    // the size got larger than the mock::basic_record_field_size and many blob testcase failed due to the problem
    // in create_nullable_record

    EXPECT_EQ(
        (mock::create_nullable_record<kind::blob, kind::blob>(
            {lob::blob_reference{0, lob::lob_data_provider::datastore},
             lob::blob_reference{1, lob::lob_data_provider::datastore}}
        )),
        (mock::create_nullable_record<kind::blob, kind::blob>(
            {lob::blob_reference{0, lob::lob_data_provider::datastore},
             lob::blob_reference{1, lob::lob_data_provider::datastore}}
        ))
    );
}


}
