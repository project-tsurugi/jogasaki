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
#include <jogasaki/utils/copy_field_data.h>

#include <jogasaki/mock/basic_record.h>

#include <gtest/gtest.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::utils {

using namespace testing;
using namespace jogasaki::mock;
using namespace std::chrono_literals;

using kind = meta::field_type_kind;

class copy_field_data_test : public ::testing::Test {};

TEST_F(copy_field_data_test, simple) {
    mock::basic_record src{create_record<kind::float4, kind::int8>(1.0, 100)};
    mock::basic_record tgt{create_record<kind::int8, kind::float4>(200, 2.0)};
    auto src_meta = src.record_meta();
    auto tgt_meta = tgt.record_meta();
    auto cnt = src_meta->field_count();

    for(std::size_t i=0; i < cnt; ++i) {
        auto j = cnt - 1 - i;
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(j);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset);
    }
    ASSERT_EQ((create_record<kind::int8, kind::float4>(100, 1.0)), tgt);
}

TEST_F(copy_field_data_test, types) {
    using test_record = mock::basic_record;
    test_record src{create_record<kind::boolean, kind::int1, kind::int2, kind::int4, kind::int8, kind::float4, kind::float8>(1, 1, 1, 1, 1, 1.0, 1.0)};
    test_record tgt{create_record<kind::boolean, kind::int1, kind::int2, kind::int4, kind::int8, kind::float4, kind::float8>(2, 2, 2, 2, 2, 2.0, 2.0)};
    auto src_meta = src.record_meta();
    auto tgt_meta = tgt.record_meta();
    auto cnt = src_meta->field_count();
    for(std::size_t i=0; i < cnt; ++i) {
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(i);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset);
    }
    ASSERT_EQ(src, tgt);
}

TEST_F(copy_field_data_test, temporal_types) {
    auto src = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(1, rtype<ft::date>{10}, rtype<ft::time_of_day>{100ns}, rtype<ft::time_point>{1000ns});
    auto tgt = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(2, rtype<ft::date>{20}, rtype<ft::time_of_day>{200ns}, rtype<ft::time_point>{2000ns});
    auto src_meta = src.record_meta();
    auto tgt_meta = tgt.record_meta();
    auto cnt = src_meta->field_count();
    for(std::size_t i=0; i < cnt; ++i) {
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(i);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset);
    }
    ASSERT_EQ(src, tgt);
}

TEST_F(copy_field_data_test, text) {
    using test_record = mock::basic_record;
    using text = accessor::text;
    mock_memory_resource r1{};
    mock_memory_resource r2{};
    test_record src{create_record<kind::character, kind::character>(text{&r1, "A23456789012345678901234567890"}, text{&r1, "111"})};
    test_record tgt{create_record<kind::character, kind::character>(text{&r1, "B23456789012345678901234567890"}, text{&r1, "222"})};
    ASSERT_EQ(60, r1.total_bytes_allocated_);  // long strings
    auto src_meta = src.record_meta();
    auto tgt_meta = tgt.record_meta();
    auto cnt = src_meta->field_count();
    for(std::size_t i=0; i < cnt; ++i) {
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(i);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset); // copied text refers existing varlen buffer
    }
    ASSERT_EQ(60, r1.total_bytes_allocated_);
    ASSERT_EQ(0, r2.total_bytes_allocated_);
    ASSERT_EQ(src, tgt);
    for(std::size_t i=0; i < cnt; ++i) {
        auto& f = src_meta->at(i);
        auto src_offset = src_meta->value_offset(i);
        auto tgt_offset = tgt_meta->value_offset(i);
        copy_field(f, tgt.ref(), tgt_offset, src.ref(), src_offset, &r2); // copied text refers new varlen buffer
    }
    ASSERT_EQ(30, r2.total_bytes_allocated_);
    ASSERT_EQ(src, tgt);
}

}

