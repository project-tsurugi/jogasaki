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
#include <jogasaki/mock/basic_record.h>

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
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta->at(0));
    }
    {
        basic_record r{create_record<kind::int4, kind::int8>()};
        auto meta = r.record_meta();
        EXPECT_EQ(2, meta->field_count());
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta->at(0));
        EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int8>}, meta->at(1));
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
    EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, meta2->at(0));
    EXPECT_EQ(meta, meta2);
}

TEST_F(basic_record_test, nullable) {
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(0));
    }
    {
        auto r = create_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_FALSE(meta->nullable(0));
        EXPECT_FALSE(meta->nullable(0));
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
        auto r = create_nullable_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(0));
    }
    {
        auto r = create_record<kind::float4, kind::int8>(1.0, 100);
        auto meta = r.record_meta();
        EXPECT_FALSE(meta->nullable(0));
        EXPECT_FALSE(meta->nullable(0));
    }
    {
        auto r1 = create_nullable_record<kind::float4, kind::int8>(1.0, 100);
        auto r2 = create_record<kind::float4, kind::int8>(1.0, 100);
        EXPECT_EQ(r1, r2);
    }
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100), {false, true});
        auto meta = r.record_meta();
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(0)));
        EXPECT_TRUE(r.ref().is_null(r.record_meta()->nullity_offset(1)));
    }
    {
        auto r = create_nullable_record<kind::float4, kind::int8>(std::forward_as_tuple(1.0, 100));
        auto meta = r.record_meta();
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_TRUE(meta->nullable(0));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(0)));
        EXPECT_FALSE(r.ref().is_null(r.record_meta()->nullity_offset(1)));
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

}
