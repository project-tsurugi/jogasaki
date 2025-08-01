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

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/input_partition.h>
#include <jogasaki/executor/exchange/group/priority_queue_reader.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::executor::exchange::group {

using namespace testing;
using namespace data;
using namespace executor;
using namespace meta;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class priority_queue_reader_test : public test_root {
public:
};

using kind = meta::field_type_kind;

auto const info = std::make_shared<group_info>(test_root::test_record_meta1(), std::vector<size_t>{0});

auto get_key = [](io::group_reader& r) {
    return r.get_group().get_value<std::int64_t>(info->key_meta()->value_offset(0));
};

auto get_value = [](io::group_reader& r) {
    return r.get_member().get_value<double>(info->value_meta()->value_offset(0));
};

TEST_F(priority_queue_reader_test, basic) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
            partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
            ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
    ));

    test::record arr[] = {
            {1, 1.0},
            {1, 2.0},
            {3, 3.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->flush();
    p2->write(arr[0].ref());
    p2->flush();


    priority_queue_reader r{info, partitions};
    std::multiset<double> res{};
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    EXPECT_EQ((std::multiset<double>{1.0, 2.0}), res);
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(priority_queue_reader_test, multiple_partitions) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
            ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
            ));
    auto& p3 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
            ));

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

    priority_queue_reader r{info, partitions};

    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(0, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(5.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    std::multiset<double> res{};
    res.emplace(get_value(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    ASSERT_FALSE(r.next_member());
    EXPECT_EQ((std::multiset<double>{1.0, 2.0, 3.0}), res);
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(3.0, get_value(r));
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(priority_queue_reader_test, empty_partition) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
    ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info,
            context.get()
    ));

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

    priority_queue_reader r{info, partitions};
    std::multiset<double> res{};
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    ASSERT_TRUE(r.next_member());
    res.emplace(get_value(r));
    EXPECT_EQ((std::multiset<double>{1.0, 2.0}), res);
    ASSERT_FALSE(r.next_member());
    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(3, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(3.0, get_value(r));
}

TEST_F(priority_queue_reader_test, ordering) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto meta = test_root::test_record_meta1();

    auto info = std::make_shared<group_info>(
        meta,
        std::vector<std::size_t>{},
        std::vector<std::size_t>{0, 1},
        std::vector<ordering>{ordering::ascending, ordering::descending}
    );
        auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));

    test::record arr[] = {
        {0, 3.0},
        {0, 2.0},
        {0, 1.0},
        {1, 3.0},
        {1, 2.0},
        {1, 1.0},
        {2, 3.0},
        {2, 2.0},
        {2, 1.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->write(arr[7].ref());
    p1->write(arr[5].ref());
    p1->flush();
    p2->write(arr[0].ref());
    p2->write(arr[3].ref());
    p2->write(arr[6].ref());
    p2->write(arr[4].ref());
    p2->write(arr[8].ref());
    p2->flush();

    auto get_key = [&](io::group_reader& r) {
        return r.get_member().get_value<std::int64_t>(info->value_meta()->value_offset(0));
    };

    auto get_value = [&](io::group_reader& r) {
        return r.get_member().get_value<double>(info->value_meta()->value_offset(1));
    };

    priority_queue_reader r{info, partitions};
    std::multiset<double> res{};
    ASSERT_TRUE(r.next_group());
    EXPECT_TRUE(r.get_group()); // even if key field count is zero, record_ref is valid
    {
        ASSERT_TRUE(r.next_member());
        EXPECT_EQ(0, get_key(r));
        EXPECT_DOUBLE_EQ(3.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(2.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(1.0, get_value(r));
    }
    {
        ASSERT_TRUE(r.next_member());
        EXPECT_EQ(1, get_key(r));
        EXPECT_DOUBLE_EQ(3.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(2.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(1.0, get_value(r));
    }
    {
        ASSERT_TRUE(r.next_member());
        EXPECT_EQ(2, get_key(r));
        EXPECT_DOUBLE_EQ(3.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(2.0, get_value(r));
        ASSERT_TRUE(r.next_member());
        EXPECT_DOUBLE_EQ(1.0, get_value(r));
    }
    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(priority_queue_reader_test, empty_keys) {
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto meta = test_root::test_record_meta1();
    auto info = std::make_shared<group_info>(
        meta,
        std::vector<std::size_t>{},
        std::vector<std::size_t>{},
        std::vector<ordering>{}
    );
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));

    test::record arr[] = {
        {0, 3.0},
        {1, 2.0},
        {2, 1.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->flush();
    p2->write(arr[0].ref());
    p2->flush();

    auto get_key = [&](io::group_reader& r) {
        return r.get_member().get_value<std::int64_t>(info->value_meta()->value_offset(0));
    };

    auto get_value = [&](io::group_reader& r) {
        return r.get_member().get_value<double>(info->value_meta()->value_offset(1));
    };

    priority_queue_reader r{info, partitions};
    std::multiset<double> values{};
    std::multiset<std::int64_t> keys{};
    ASSERT_TRUE(r.next_group());
    EXPECT_TRUE(r.get_group()); // even if key field count is zero, record_ref is valid
    {
        ASSERT_TRUE(r.next_member());
        keys.emplace( get_key(r));
        values.emplace( get_value(r));
        ASSERT_TRUE(r.next_member());
        keys.emplace( get_key(r));
        values.emplace( get_value(r));
        ASSERT_TRUE(r.next_member());
        keys.emplace( get_key(r));
        values.emplace( get_value(r));
    }
    EXPECT_EQ((std::multiset<std::int64_t>{0, 1, 2}), keys);
    EXPECT_EQ((std::multiset<double>{1.0, 2.0, 3.0}), values);

    ASSERT_FALSE(r.next_member());
    ASSERT_FALSE(r.next_group());
}

TEST_F(priority_queue_reader_test, record_limit_per_group) {
    auto info = std::make_shared<group_info>(
        test_root::test_record_meta1(),
        std::vector<size_t>{0},
        std::vector<group_info::field_index_type>{} ,
        std::vector<jogasaki::executor::ordering>{},
        std::optional<std::size_t>{2}
    );
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.reserve(10); // avoid relocation when using references into vector
    auto context = std::make_shared<request_context>();
    auto& p1 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));
    auto& p2 = partitions.emplace_back(std::make_unique<input_partition>(
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info,
        context.get()
    ));

    test::record arr[] = {
        {1, 1.0},
        {1, 2.0},
        {4, 4.0},
        {1, 3.0},
        {2, 2.0},
        {2, 3.0},
        {2, 1.0},
    };
    auto sz = sizeof(arr[0]);

    p1->write(arr[2].ref());
    p1->write(arr[1].ref());
    p1->write(arr[4].ref());
    p1->write(arr[6].ref());
    p1->flush();
    p2->write(arr[0].ref());
    p2->write(arr[3].ref());
    p2->write(arr[5].ref());
    p2->flush();

    priority_queue_reader r{info, partitions};
    ASSERT_TRUE(r.next_group());

    EXPECT_EQ(1, get_key(r));
    ASSERT_TRUE(r.next_member());
    auto res0 = get_value(r);
    ASSERT_TRUE(r.next_member());
    auto res1 = get_value(r);
    ASSERT_FALSE(r.next_member());
    auto exp = std::multiset<double>{1.0, 2.0, 3.0};
    EXPECT_TRUE((exp.find(res0) != exp.end()));
    EXPECT_TRUE((exp.find(res1) != exp.end()));
    EXPECT_NE(res0, res1);

    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(2, get_key(r));
    ASSERT_TRUE(r.next_member());
    auto res2 = get_value(r);
    ASSERT_TRUE(r.next_member());
    auto res3 = get_value(r);
    ASSERT_FALSE(r.next_member());
    EXPECT_TRUE((exp.find(res2) != exp.end()));
    EXPECT_TRUE((exp.find(res3) != exp.end()));
    EXPECT_NE(res2, res3);

    ASSERT_TRUE(r.next_group());
    EXPECT_EQ(4, get_key(r));
    ASSERT_TRUE(r.next_member());
    EXPECT_EQ(4.0, get_value(r));
    ASSERT_FALSE(r.next_member());

    ASSERT_FALSE(r.next_group());

}

}

