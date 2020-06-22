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

#include <jogasaki/data/small_record_store.h>

#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock_memory_resource.h>

#include <jogasaki/test_root.h>

namespace jogasaki::data {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class small_record_store_test : public test_root {};

TEST_F(small_record_store_test, basic) {
    auto meta = test_record_meta1();
    small_record_store r{meta};
    auto c1_offset = meta->value_offset(0);
    auto c2_offset = meta->value_offset(1);
    r.ref().set_value(c1_offset, 2L);
    r.ref().set_value(c2_offset, 2.0);

    EXPECT_EQ(2, r.ref().get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(2.0, r.ref().get_value<double>(c2_offset));
}

TEST_F(small_record_store_test, memory_resource) {
    mock_memory_resource resource{};
    auto meta = test_record_meta1();
    small_record_store r{meta, 1, &resource};
    auto c1_offset = meta->value_offset(0);
    auto c2_offset = meta->value_offset(1);
    r.ref().set_value(c1_offset, 2L);
    r.ref().set_value(c2_offset, 2.0);

    EXPECT_EQ(2, r.ref().get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(2.0, r.ref().get_value<double>(c2_offset));
}

TEST_F(small_record_store_test, multiple_records) {
    mock_memory_resource resource{};
    auto meta = test_record_meta1();
    small_record_store r{meta, 3, &resource};
    auto c1_offset = meta->value_offset(0);
    auto c2_offset = meta->value_offset(1);
    r.ref(2).set_value(c1_offset, 2L);
    r.ref(2).set_value(c2_offset, 2.0);
    r.ref(0).set_value(c1_offset, 0L);
    r.ref(0).set_value(c2_offset, 0.0);
    r.ref(1).set_value(c1_offset, 1L);
    r.ref(1).set_value(c2_offset, 1.0);

    EXPECT_EQ(0, r.ref().get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(0.0, r.ref().get_value<double>(c2_offset));
    EXPECT_EQ(1, r.ref(1).get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(1.0, r.ref(1).get_value<double>(c2_offset));
    EXPECT_EQ(2, r.ref(2).get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(2.0, r.ref(2).get_value<double>(c2_offset));
}
}

