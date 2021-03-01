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
#include <jogasaki/executor/function/incremental/builtin_functions.h>
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

using executor::function::incremental::aggregate_function_info_impl;
using executor::function::incremental::aggregate_function_kind;

class aggregate_reader_test : public test_root {
public:
    using kind = meta::field_type_kind;

    using sum_info_impl = aggregate_function_info_impl<aggregate_function_kind::sum>;
    using avg_info_impl = aggregate_function_info_impl<aggregate_function_kind::avg>;
    std::shared_ptr<sum_info_impl> func_sum = std::make_shared<sum_info_impl>();
    std::shared_ptr<avg_info_impl> func_avg = std::make_shared<avg_info_impl>();

    std::shared_ptr<aggregate_info> sum_info = std::make_shared<aggregate_info>(
        test_root::test_record_meta1(),
        std::vector<size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                *func_sum,
                {
                    1
                },
                meta::field_type(enum_tag<kind::float8>)
            }
        }
    );

    std::shared_ptr<aggregate_info> avg_info = std::make_shared<aggregate_info>(
        test_root::test_record_meta1(),
        std::vector<size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                *func_avg,
                {
                    1
                },
                meta::field_type(enum_tag<kind::float8>)
            }
        }
    );

    std::shared_ptr<aggregate_info> avg_avg_info = std::make_shared<aggregate_info>(
        test_root::test_record_meta1(),
        std::vector<size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                *func_avg,
                {
                    1
                },
                meta::field_type(enum_tag<kind::float8>)
            },
            {
                *func_avg,
                {
                    1
                },
                meta::field_type(enum_tag<kind::float8>)
            }
        }
    );

    std::function<std::int64_t(group_reader&)> get_key = [this](group_reader& r) {
        return r.get_group().get_value<std::int64_t>(sum_info->post().group_meta()->key().value_offset(0));
    };

    std::function<double(group_reader&)> get_value = [this](group_reader& r) {
        return r.get_member().get_value<double>(sum_info->post().group_meta()->value().value_offset(0));
    };

    std::function<accessor::record_ref(group_reader&)> get_key_record = [](group_reader& r) {
        return r.get_group();
    };

    std::function<accessor::record_ref(group_reader&)> get_value_record = [](group_reader& r) {
        return r.get_member();
    };

};

mock::basic_record create_rec(std::int64_t x, double y) {
    return mock::create_nullable_record<kind::int8, kind::float8>(x, y);
}

TEST_F(aggregate_reader_test, basic) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(sum_info));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(sum_info));

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

    reader r{sum_info, partitions};
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
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>( sum_info ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>( sum_info ));
    auto& p3 = partitions.emplace_back(std::make_unique<input_partition>( sum_info ));

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

    reader r{sum_info, partitions};

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
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>( sum_info ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>( sum_info ));

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

    reader r{sum_info, partitions};
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

TEST_F(aggregate_reader_test, avg) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(avg_info));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(avg_info));

    mock::basic_record arr[] = {
        create_rec(1, 1.0),
        create_rec(1, 1.0),
        create_rec(3, 2.0),
        create_rec(3, 2.0),
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

    reader r{avg_info, partitions};
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(1.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_DOUBLE_EQ(2.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}
TEST_F(aggregate_reader_test, avg_avg) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(avg_avg_info));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(avg_avg_info));

    mock::basic_record arr[] = {
        create_rec(1, 1.0),
        create_rec(1, 1.0),
        create_rec(3, 2.0),
        create_rec(3, 2.0),
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

    reader r{avg_avg_info, partitions};
    auto value_meta = avg_avg_info->post().group_meta()->value_shared();
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    {
        auto exp = mock::create_nullable_record<kind::float8, kind::float8>(1.0, 1.0);
        EXPECT_EQ(exp, mock::basic_record(get_value_record(r), value_meta));
    }
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    {
        auto exp = mock::create_nullable_record<kind::float8, kind::float8>(2.0, 2.0);
        EXPECT_EQ(exp, mock::basic_record(get_value_record(r), value_meta));
    }
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}
}

