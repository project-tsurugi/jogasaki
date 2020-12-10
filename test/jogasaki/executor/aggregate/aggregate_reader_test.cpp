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

#include <jogasaki/executor/exchange/aggregate/reader.h>

#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/exchange/aggregate/input_partition.h>
#include <jogasaki/executor/builtin_functions.h>
#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::exchange::aggregate {

using namespace testing;
using namespace data;
using namespace executor;
using namespace meta;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aggregate_reader_test : public test_root {
public:
};

using kind = meta::field_type_kind;

auto const info = std::make_shared<aggregate_info>(
    test_root::test_record_meta1(),
    std::vector<size_t>{0},
    std::vector<aggregate_info::value_spec>{
        {
            executor::builtin::sum,
            {
                1
            },
            meta::field_type(enum_tag<kind::float8>)
        }
    }
);

auto get_key = [](group_reader& r) {
    return r.get_group().get_value<std::int64_t>(info->post_group_meta()->key().value_offset(0));
};

auto get_value = [](group_reader& r) {
    return r.get_member().get_value<double>(info->post_group_meta()->value().value_offset(0));
};

mock::basic_record create_rec(std::int64_t x, double y) {
    return mock::create_nullable_record<kind::int8, kind::float8>(x, y);
}

TEST_F(aggregate_reader_test, basic) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(info));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(info));

    mock::basic_record arr[] = {
        create_rec(1, 1.0),
        create_rec(1, 1.0),
        create_rec(3, 2.0),
        create_rec(3, 1.0),
        create_rec(1, 1.0),
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->write(arr[4].ref());
    p1->flush();
    p2->write(arr[0].ref());
    p2->write(arr[3].ref());
    p2->flush();

    reader r{info, partitions};
    std::multiset<double> res{};
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(aggregate_reader_test, multiple_partitions) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>( info ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>( info ));
    auto& p3 = partitions.emplace_back(std::make_unique<input_partition>( info ));

    test::record arr[] = {
            {0, 5.0},
            {1, 1.0},
            {1, 2.0},
            {1, 3.0},
            {3, 3.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p2->write(arr[1].ref());
    p3->write(arr[3].ref());
    p2->write(arr[0].ref());
    p2->write(arr[4].ref());
    p1->flush();
    p2->flush();
    p3->flush();

    reader r{info, partitions};

    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(0, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(5.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(6.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(aggregate_reader_test, empty_partition) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>( info ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>( info ));

    test::record arr[] = {
        {1, 1.0},
        {1, 2.0},
        {3, 3.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[0].ref());
    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->flush();
    p2->flush();

    reader r{info, partitions};
    std::multiset<double> res{};
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

}

