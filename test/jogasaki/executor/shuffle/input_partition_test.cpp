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

#include <jogasaki/executor/exchange/group/input_partition.h>

#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::exchange::group {

using namespace testing;
using namespace data;
using namespace executor;
using namespace meta;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class input_partition_test : public test_root {
public:
};

using kind = meta::field_type_kind;

TEST_F(input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<shuffle_info>(test_record_meta1(), std::vector<std::size_t>{0}), context.get()};
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

TEST_F(input_partition_test, use_monotonic_resource) {
    memory::page_pool pool{};
    auto context = std::make_shared<request_context>();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<shuffle_info>(test_record_meta1(), std::vector<std::size_t>{0}),
            context.get(),
            };

    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();

    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

TEST_F(input_partition_test, auto_flush_to_next_table_when_full) {
    auto context = std::make_shared<request_context>();
    auto meta = test_record_meta1();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<shuffle_info>(meta, std::vector<std::size_t>{0}),
            context.get(),
            2
            };
    test::record r1 {1, 1.0};
    test::record r2 {2, 2.0};
    test::record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r2.ref());
    partition.flush();

    auto record_size = meta->record_size();
    auto c1_offset = meta->value_offset(0);
    auto c2_offset = meta->value_offset(1);
    ASSERT_EQ(2, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t0 = *partition.begin();
    EXPECT_EQ(2, std::distance(t0.begin(), t0.end()));
    auto it = t0.begin();
    EXPECT_EQ(1, accessor::record_ref(*it, record_size).get_value<std::int64_t>(c1_offset));
    EXPECT_EQ(3, accessor::record_ref(*++it, record_size).get_value<std::int64_t>(c1_offset));
    auto& t1 = *++partition.begin();
    EXPECT_EQ(1, std::distance(t1.begin(), t1.end()));
    EXPECT_EQ(2, accessor::record_ref(*t1.begin(), record_size).get_value<std::int64_t>(c1_offset));
}

TEST_F(input_partition_test, text) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_shared<shuffle_info>(test_record_meta2(), std::vector<std::size_t>{0}),
            context.get(),
            };

    struct S {
        text t1_{};
        double f_{};
        text t2_{};
    };

    mock_memory_resource res{};
    S r1{text{&res, "111"sv}, 1.0, text{&res, "AAA"}};
    S r2{text{&res, "222"sv}, 2.0, text{&res, "BBB"}};
    S r3{text{&res, "333"sv}, 3.0, text{&res, "CCC"}};
    accessor::record_ref ref1{&r1, sizeof(S)};
    accessor::record_ref ref2{&r2, sizeof(S)};
    accessor::record_ref ref3{&r3, sizeof(S)};

    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    ASSERT_EQ(3, std::distance(t.begin(), t.end()));
    auto it = t.begin();
    accessor::record_ref res1{*it++, sizeof(S)};
    accessor::record_ref res2{*it++, sizeof(S)};
    accessor::record_ref res3{*it++, sizeof(S)};

    auto meta2 = test_record_meta2();
    comparator comp{meta2.get()};
    EXPECT_EQ(0, comp(ref1, res1));
    EXPECT_EQ(0, comp(ref2, res2));
    EXPECT_EQ(0, comp(ref3, res3));
}

}

