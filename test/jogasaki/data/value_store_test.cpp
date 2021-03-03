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
#include <jogasaki/data/value_store.h>

#include <gtest/gtest.h>

#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/accessor/text.h>

#include <jogasaki/test_root.h>

namespace jogasaki::data {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class value_store_test : public test_root {};

using kind = meta::field_type_kind;

TEST_F(value_store_test, simple) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    value_store store{
        meta::field_type{takatori::util::enum_tag<kind::int4>},
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
    EXPECT_EQ(meta::field_type{takatori::util::enum_tag<kind::int4>}, store.type());

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
        meta::field_type{takatori::util::enum_tag<kind::int4>},
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
        meta::field_type{takatori::util::enum_tag<kind::int8>},
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
        meta::field_type{takatori::util::enum_tag<kind::float4>},
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
        meta::field_type{takatori::util::enum_tag<kind::float8>},
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
        meta::field_type{takatori::util::enum_tag<kind::character>},
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

TEST_F(value_store_test, print_iterator) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    value_store store{
        meta::field_type{takatori::util::enum_tag<kind::int4>},
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
        meta::field_type{takatori::util::enum_tag<kind::int4>},
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

}

