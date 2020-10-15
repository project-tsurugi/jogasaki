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
    mock::record record{};
    auto meta = record.record_meta();
    small_record_store r{meta};
    auto c0_offset = meta->value_offset(0);
    auto c1_offset = meta->value_offset(1);
    record.ref().set_value(c0_offset, 2L);
    record.ref().set_value(c1_offset, 2.0);
    r.set(record.ref());

    EXPECT_EQ(2, r.ref().get_value<std::int64_t>(c0_offset));
    EXPECT_EQ(2.0, r.ref().get_value<double>(c1_offset));
}

TEST_F(small_record_store_test, memory_resource) {
    mock_memory_resource resource{};
    mock::record record{};
    auto meta = record.record_meta();
    small_record_store r{meta, &resource};
    auto c0_offset = meta->value_offset(0);
    auto c1_offset = meta->value_offset(1);
    record.ref().set_value(c0_offset, 2L);
    record.ref().set_value(c1_offset, 2.0);
    r.set(record.ref());

    EXPECT_EQ(2, r.ref().get_value<std::int64_t>(c0_offset));
    EXPECT_EQ(2.0, r.ref().get_value<double>(c1_offset));
}

TEST_F(small_record_store_test, multiple_records) {
    mock_memory_resource resource{};
    mock::record record0{};
    mock::record record1{};
    mock::record record2{};
    auto meta = record0.record_meta();
    small_record_store r{meta, &resource, 3};
    auto c0_offset = meta->value_offset(0);
    auto c1_offset = meta->value_offset(1);
    record2.ref().set_value(c0_offset, 2L);
    record2.ref().set_value(c1_offset, 2.0);
    record0.ref().set_value(c0_offset, 0L);
    record0.ref().set_value(c1_offset, 0.0);
    record1.ref().set_value(c0_offset, 1L);
    record1.ref().set_value(c1_offset, 1.0);

    r.set(record2.ref(), 2);
    r.set(record0.ref(), 0);
    r.set(record1.ref(), 1);

    EXPECT_EQ(0, r.ref().get_value<std::int64_t>(c0_offset));
    EXPECT_EQ(0.0, r.ref().get_value<double>(c1_offset));
    EXPECT_EQ(1, r.ref(1).get_value<std::int64_t>(c0_offset));
    EXPECT_EQ(1.0, r.ref(1).get_value<double>(c1_offset));
    EXPECT_EQ(2, r.ref(2).get_value<std::int64_t>(c0_offset));
    EXPECT_EQ(2.0, r.ref(2).get_value<double>(c1_offset));
}

TEST_F(small_record_store_test, metadata_variation) {
    mock_memory_resource resource{};
    mock_memory_resource varlen_resource{};
    mock::record_f4f8ch record{};
    auto meta = record.record_meta();
    small_record_store r{meta, &resource};
    auto c0_offset = meta->value_offset(0);
    auto c1_offset = meta->value_offset(1);
    auto c2_offset = meta->value_offset(2);
    record.ref().set_value(c0_offset, 2.0);
    record.ref().set_value(c1_offset, 2);
    auto str = "12345678901234567890"sv;
    record.ref().set_value(c2_offset, accessor::text{&varlen_resource, str});
    EXPECT_EQ(20, varlen_resource.total_bytes_allocated_);

    r.set(record.ref());
    EXPECT_EQ(2.0, r.ref().get_value<double>(c0_offset));
    EXPECT_EQ(2, r.ref().get_value<std::int32_t>(c1_offset));
    EXPECT_EQ(str, static_cast<std::string_view>(r.ref().get_value<accessor::text>(c2_offset)));
    EXPECT_EQ(20, resource.total_bytes_allocated_);
}

TEST_F(small_record_store_test, comparison) {
    mock::record record{};
    auto meta = record.record_meta();
    small_record_store r1{meta};
    small_record_store r2{meta};
    auto c0_offset = meta->value_offset(0);
    auto c1_offset = meta->value_offset(1);
    record.ref().set_value(c0_offset, 2L);
    record.ref().set_value(c1_offset, 2.0);
    r1.set(record.ref());
    EXPECT_NE(r1, r2);
    r2.set(record.ref());
    EXPECT_EQ(r1, r2);

    small_record_store e{};
    EXPECT_NE(e, r1);
    EXPECT_EQ(e, e);
}

TEST_F(small_record_store_test, print) {
    mock::record record{};
    auto meta = record.record_meta();
    small_record_store r1{meta};
    std::cout << r1; // manually check the output

    std::stringstream ss{};
    small_record_store e{};
    ss << e;
    EXPECT_EQ("<empty>", ss.str());
}
}

