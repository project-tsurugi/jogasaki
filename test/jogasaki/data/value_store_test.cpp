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
#include <iostream>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/meta_type.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::data {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class value_store_test : public test_root {};

using kind = meta::field_type_kind;

TEST_F(value_store_test, simple) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource
    };

    ASSERT_TRUE(store.empty());
    ASSERT_EQ(store.begin<std::int32_t>(), store.end<std::int32_t>());
    store.append<std::int32_t>(10);
    store.append<std::int32_t>(20);
    store.append<std::int32_t>(30);

    ASSERT_EQ(3, store.count());
    ASSERT_FALSE(store.empty());
    EXPECT_EQ(meta::int4_type(), store.type());

    store.reset();
    ASSERT_EQ(0, store.count());
    ASSERT_TRUE(store.empty());
    ASSERT_EQ(store.begin<std::int32_t>(), store.end<std::int32_t>());
    store.append<std::int32_t>(1);
    store.append<std::int32_t>(2);
    store.append<std::int32_t>(3);

    auto it = store.begin<std::int32_t>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<std::int32_t>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_int4) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource
    };

    store.append<std::int32_t>(1);
    store.append<std::int32_t>(2);
    store.append<std::int32_t>(3);

    auto it = store.begin<std::int32_t>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<std::int32_t>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_int8) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::int8_type(),
        &resource,
        &varlen_resource
    };

    store.append<std::int64_t>(1);
    store.append<std::int64_t>(2);
    store.append<std::int64_t>(3);

    auto it = store.begin<std::int64_t>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<std::int64_t>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_float4) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::float4_type(),
        &resource,
        &varlen_resource
    };

    store.append<float>(1.0);
    store.append<float>(2.0);
    store.append<float>(3.0);

    auto it = store.begin<float>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<float>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_float8) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::float8_type(),
        &resource,
        &varlen_resource
    };

    store.append<double>(1.0);
    store.append<double>(2.0);
    store.append<double>(3.0);

    auto it = store.begin<double>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<double>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_character) {
    using accessor::text;
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    mock_memory_resource varlen_resource{};
    value_store store{
        meta::character_type(),
        &resource,
        &varlen_resource
    };

    store.append<text>(text{"111"});
    store.append<text>(text{"22222222222222222222"});
    EXPECT_EQ(20, varlen_resource.total_bytes_allocated_);
    store.append<text>(text{"333333"});
    EXPECT_EQ(20, varlen_resource.total_bytes_allocated_);
    store.append<text>(text{"44444444444444444444"});
    EXPECT_EQ(40, varlen_resource.total_bytes_allocated_);

    auto it = store.begin<text>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<text>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(text{"111"}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(text{"22222222222222222222"}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(text{"333333"}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(text{"44444444444444444444"}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_date) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::date_type(),
        &resource,
        &varlen_resource
    };

    using date = rtype<ft::date>;

    store.append<date>(date{1});
    store.append<date>(date{2});
    store.append<date>(date{3});

    auto it = store.begin<date>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<date>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(date{1}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(date{2}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(date{3}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_time_of_day) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::time_of_day_type(),
        &resource,
        &varlen_resource
    };

    using time_of_day = rtype<ft::time_of_day>;

    store.append<time_of_day>(time_of_day{1ns});
    store.append<time_of_day>(time_of_day{2ns});
    store.append<time_of_day>(time_of_day{3ns});

    auto it = store.begin<time_of_day>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<time_of_day>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(time_of_day{1ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(time_of_day{2ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(time_of_day{3ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, type_time_point) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::time_point_type(),
        &resource,
        &varlen_resource
    };

    using time_point = rtype<ft::time_point>;

    store.append(time_point{1ns});
    store.append(time_point{2ns});
    store.append(time_point{3ns});

    auto it = store.begin<time_point>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<time_point>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(time_point{1ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(time_point{2ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(time_point{3ns}, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, print_iterator) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource
    };
    store.append<std::int32_t>(1);
    store.append<std::int32_t>(2);
    store.append<std::int32_t>(3);

    auto it = store.begin<std::int32_t>();
    std::cout << it << std::endl;
}

TEST_F(value_store_test, range) {
    memory::page_pool pool{};
    mock_memory_resource resource{8, 0};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource
    };

    store.append<std::int32_t>(1);
    EXPECT_EQ(4, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(4, resource.total_bytes_allocated_);
    store.append<std::int32_t>(2);
    EXPECT_EQ(8, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(8, resource.total_bytes_allocated_);
    store.append<std::int32_t>(3);
    EXPECT_EQ(4, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(12, resource.total_bytes_allocated_);
    store.append<std::int32_t>(4);
    EXPECT_EQ(8, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(16, resource.total_bytes_allocated_);
    store.append<std::int32_t>(5);
    EXPECT_EQ(4, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(20, resource.total_bytes_allocated_);
    store.append<std::int32_t>(6);
    EXPECT_EQ(8, resource.allocated_bytes_on_current_page_);
    EXPECT_EQ(24, resource.total_bytes_allocated_);

    EXPECT_EQ(3, store.range_count());
    EXPECT_EQ(0, store.null_range_count());

    auto it = store.begin<std::int32_t>();
    EXPECT_TRUE(it.valid());
    auto end = store.end<std::int32_t>();
    EXPECT_FALSE(end.valid());
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(2, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(3, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(4, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(5, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(6, *it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, nullable) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    ASSERT_TRUE(store.empty());
    ASSERT_EQ(store.begin<std::int32_t>(), store.end<std::int32_t>());
    store.append_null();
    ASSERT_FALSE(store.empty());
    ASSERT_NE(store.begin<std::int32_t>(), store.end<std::int32_t>());
    store.append<std::int32_t>(10);
    store.append_null();
    store.append<std::int32_t>(20);
    store.append_null();
    store.append<std::int32_t>(30);

    ASSERT_EQ(6, store.count());
    EXPECT_EQ(meta::int4_type(), store.type());
    store.reset();
    ASSERT_EQ(0, store.count());
    ASSERT_TRUE(store.empty());
    ASSERT_EQ(store.begin<std::int32_t>(), store.end<std::int32_t>());
    store.append_null();
    store.append<std::int32_t>(1);
    store.append_null();
    store.append<std::int32_t>(2);
    store.append_null();
    store.append<std::int32_t>(3);

    auto it = store.begin<std::int32_t>();
    EXPECT_TRUE(it.valid());
    EXPECT_TRUE(it.is_null());
    it++;
    EXPECT_EQ(1, *it);
    it++;
    EXPECT_TRUE(it.is_null());
    it++;
    EXPECT_EQ(2, *it);
    it++;
    EXPECT_TRUE(it.is_null());
    it++;
    EXPECT_EQ(3, *it);
    it++;
    EXPECT_EQ(store.end<std::int32_t>(), it);
}

TEST_F(value_store_test, nullable_crossing_block_boundary) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    constexpr std::int32_t n = 30;
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 3 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());

    std::int32_t i = 0;
    for (auto it = store.begin<std::int32_t>(), e = store.end<std::int32_t>(); it != e; ++it, ++i) {
        if (i % 3 == 0) {
            EXPECT_TRUE(it.is_null());
        } else {
            EXPECT_TRUE(! it.is_null());
            EXPECT_EQ(i, *it);
        }
    }
    EXPECT_EQ(n, i);
}

TEST_F(value_store_test, nullable_across_null_ranges) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    // allow only one allocation per page so every null flag block is placed on a
    // fresh (non-contiguous) page, forcing null flags to span multiple ranges.
    // this reproduces the page boundary crossing that previously failed (issue #1528)
    mock_memory_resource nulls_resource{0, 1};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    constexpr std::int32_t n = 40;
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 2 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());

    std::int32_t i = 0;
    for (auto it = store.begin<std::int32_t>(), e = store.end<std::int32_t>(); it != e; ++it, ++i) {
        if (i % 2 == 0) {
            EXPECT_TRUE(it.is_null());
        } else {
            EXPECT_TRUE(! it.is_null());
            EXPECT_EQ(i, *it);
        }
    }
    EXPECT_EQ(n, i);
}

TEST_F(value_store_test, null_range) {
    memory::page_pool pool{};
    mock_memory_resource resource{};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    // allow only one allocation per page so every null flag block lands on a fresh
    // (non-contiguous) page, forcing each 8-flag block to start a new null range.
    // this is the null-flag counterpart of the `range` test above, which forces the value
    // storage to span multiple ranges.
    mock_memory_resource nulls_resource{0, 1};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    // even index -> null, odd index -> value. 10 elements span two null flag blocks
    // (flags 0-7 and 8-9), i.e. two null ranges, crossing the block/range boundary at index 8.
    store.append_null();               // index 0: first null block allocated
    EXPECT_EQ(1, nulls_resource.total_bytes_allocated_);
    store.append<std::int32_t>(1);     // index 1
    store.append_null();               // index 2
    store.append<std::int32_t>(3);     // index 3
    store.append_null();               // index 4
    store.append<std::int32_t>(5);     // index 5
    store.append_null();               // index 6
    store.append<std::int32_t>(7);     // index 7: still first null block
    EXPECT_EQ(1, nulls_resource.total_bytes_allocated_);
    store.append_null();               // index 8: second null block -> new null range
    EXPECT_EQ(2, nulls_resource.total_bytes_allocated_);
    store.append<std::int32_t>(9);     // index 9
    EXPECT_EQ(2, nulls_resource.total_bytes_allocated_);

    ASSERT_EQ(10, store.count());

    // 2 null flag blocks each on its own page -> 2 null ranges; the values all fit on the
    // single default value page -> 1 value range.
    EXPECT_EQ(1, store.range_count());
    EXPECT_EQ(2, store.null_range_count());

    auto it = store.begin<std::int32_t>();
    auto end = store.end<std::int32_t>();
    EXPECT_TRUE(it.valid());
    EXPECT_TRUE(! end.valid());

    EXPECT_TRUE(it.is_null());         // index 0
    EXPECT_NE(end, it);
    it++;
    EXPECT_TRUE(! it.is_null());       // index 1
    EXPECT_EQ(1, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_TRUE(it.is_null());         // index 2
    EXPECT_NE(end, it);
    it++;
    EXPECT_TRUE(! it.is_null());       // index 3
    EXPECT_EQ(3, *it);
    it++;
    EXPECT_TRUE(it.is_null());         // index 4
    it++;
    EXPECT_TRUE(! it.is_null());       // index 5
    EXPECT_EQ(5, *it);
    it++;
    EXPECT_TRUE(it.is_null());         // index 6
    it++;
    EXPECT_TRUE(! it.is_null());       // index 7: last flag of the first null range
    EXPECT_EQ(7, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_TRUE(it.is_null());         // index 8: first flag of the second null range
    EXPECT_NE(end, it);
    it++;
    EXPECT_TRUE(! it.is_null());       // index 9
    EXPECT_EQ(9, *it);
    EXPECT_NE(end, it);
    it++;
    EXPECT_EQ(end, it);
}

TEST_F(value_store_test, null_range_multiple_blocks_with_gaps) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    // allow up to three allocations (three 8-flag blocks) per page: each page becomes a null
    // range spanning multiple contiguous blocks, and every 4th block starts on a fresh
    // (non-contiguous) page, i.e. a new range with a gap before it. this exercises both
    // advancing null_block_ within a multi-block range and advancing null_range_ across the gap
    // in the same traversal, which the other null tests do not cover simultaneously.
    mock_memory_resource nulls_resource{0, 3};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    constexpr std::int32_t n = 60; // 8 null blocks grouped 3 per page -> ranges of {3,3,2} blocks
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 2 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());

    // 8 one-byte null blocks were allocated over 3 pages, so the flags span 3 non-contiguous
    // ranges and at least one range holds multiple contiguous blocks (8 blocks / 3 pages).
    EXPECT_EQ(8, nulls_resource.total_bytes_allocated_);
    EXPECT_EQ(3, nulls_resource.resources_.size());

    // 8 null blocks over 3 pages -> 3 null ranges; the values all fit on the single default
    // value page -> 1 value range.
    EXPECT_EQ(1, store.range_count());
    EXPECT_EQ(3, store.null_range_count());

    std::int32_t i = 0;
    for (auto it = store.begin<std::int32_t>(), e = store.end<std::int32_t>(); it != e; ++it, ++i) {
        if (i % 2 == 0) {
            EXPECT_TRUE(it.is_null());
        } else {
            EXPECT_TRUE(! it.is_null());
            EXPECT_EQ(i, *it);
        }
    }
    EXPECT_EQ(n, i);
}

TEST_F(value_store_test, empty_nullable_begin_equals_end) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    EXPECT_TRUE(store.empty());
    EXPECT_EQ(0, store.count());
    EXPECT_EQ(0, store.range_count());
    EXPECT_EQ(0, store.null_range_count());

    auto begin = store.begin<std::int32_t>();
    auto end = store.end<std::int32_t>();
    EXPECT_EQ(begin, end);
    EXPECT_TRUE(! begin.valid());
    EXPECT_TRUE(! end.valid());
}

TEST_F(value_store_test, single_value_nullable) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    store.append<std::int32_t>(42);

    EXPECT_TRUE(! store.empty());
    EXPECT_EQ(1, store.count());
    EXPECT_EQ(1, store.range_count());
    EXPECT_EQ(1, store.null_range_count());

    auto it = store.begin<std::int32_t>();
    auto end = store.end<std::int32_t>();
    EXPECT_NE(it, end);
    EXPECT_TRUE(it.valid());
    EXPECT_TRUE(! it.is_null());
    EXPECT_EQ(42, *it);
    it++;
    EXPECT_EQ(end, it);
    EXPECT_TRUE(! it.valid());
}

TEST_F(value_store_test, single_null_nullable) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    store.append_null();

    EXPECT_TRUE(! store.empty());
    EXPECT_EQ(1, store.count());
    EXPECT_EQ(1, store.range_count());
    EXPECT_EQ(1, store.null_range_count());

    auto it = store.begin<std::int32_t>();
    auto end = store.end<std::int32_t>();
    EXPECT_NE(it, end);
    EXPECT_TRUE(it.valid());
    EXPECT_TRUE(it.is_null());
    it++;
    EXPECT_EQ(end, it);
    EXPECT_TRUE(! it.valid());
}

TEST_F(value_store_test, value_and_null_ranges_with_different_boundaries) {
    // both the values and the null flags are split into multiple ranges, and the split positions
    // do not coincide: values are split every 3 records while null flags are split every 16
    // records (2 blocks x 8 flags). This is the production shape of issue #1528, where the value
    // pages and the null flag pages fill up at different rates, and it exercises the value cursor
    // and the null cursor of the iterator advancing across range boundaries independently.
    mock_memory_resource resource{0, 3};
    mock_memory_resource varlen_resource{};
    mock_memory_resource nulls_resource{0, 2};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    constexpr std::int32_t n = 50;
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 3 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());

    // 50 values, 3 per page -> ceil(50/3) = 17 value ranges
    EXPECT_EQ(17, store.range_count());
    // 50 flags -> ceil(50/8) = 7 blocks, 2 blocks per page -> ceil(7/2) = 4 null ranges
    EXPECT_EQ(7, nulls_resource.total_bytes_allocated_);
    EXPECT_EQ(4, store.null_range_count());

    std::int32_t i = 0;
    for (auto it = store.begin<std::int32_t>(), e = store.end<std::int32_t>(); it != e; ++it, ++i) {
        if (i % 3 == 0) {
            EXPECT_TRUE(it.is_null()) << "index:" << i;
        } else {
            EXPECT_TRUE(! it.is_null()) << "index:" << i;
            EXPECT_EQ(i, *it);
        }
    }
    EXPECT_EQ(n, i);
}

TEST_F(value_store_test, reset_with_multiple_null_ranges) {
    // verify reset() clears the null flag ranges and the in-block write cursor, so that the
    // flags appended after reset() start from a fresh range and a fresh block boundary
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    // one null flag block per page, so every 8 flags start a new null range
    mock_memory_resource nulls_resource{0, 1};
    value_store store{
        meta::int4_type(),
        &resource,
        &varlen_resource,
        &nulls_resource
    };

    // 20 records -> 3 null blocks -> 3 null ranges. 20 is not a multiple of 8, so the last block
    // is only partially used when reset() is called.
    constexpr std::int32_t n = 20;
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 2 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());
    ASSERT_EQ(3, store.null_range_count());

    store.reset();
    EXPECT_TRUE(store.empty());
    EXPECT_EQ(0, store.count());
    EXPECT_EQ(0, store.range_count());
    EXPECT_EQ(0, store.null_range_count());
    EXPECT_EQ(store.begin<std::int32_t>(), store.end<std::int32_t>());

    // append again with a different null pattern and cross the range boundaries once more
    for (std::int32_t i = 0; i < n; ++i) {
        if (i % 3 == 0) {
            store.append_null();
        } else {
            store.append<std::int32_t>(i);
        }
    }
    ASSERT_EQ(n, store.count());
    EXPECT_EQ(3, store.null_range_count());

    std::int32_t i = 0;
    for (auto it = store.begin<std::int32_t>(), e = store.end<std::int32_t>(); it != e; ++it, ++i) {
        if (i % 3 == 0) {
            EXPECT_TRUE(it.is_null()) << "index:" << i;
        } else {
            EXPECT_TRUE(! it.is_null()) << "index:" << i;
            EXPECT_EQ(i, *it);
        }
    }
    EXPECT_EQ(n, i);
}

}
