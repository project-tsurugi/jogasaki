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

#include <executor/exchange/group/input_partition.h>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>
#include <executor/exchange/group/shuffle_info.h>
#include <accessor/record_ref.h>

#include <record.h>
#include <mock_memory_resource.h>
#include <memory/monotonic_paged_memory_resource.h>
#include "test_root.h"

namespace jogasaki::executor::exchange::group {

using namespace data;
using namespace executor;
using namespace meta;
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
    auto resource = std::make_unique<mock_memory_resource>();
    input_partition partition{
            std::move(resource),
            std::make_shared<shuffle_info>(test_record_meta1(), std::vector<std::size_t>{0})};
    record r1 {1, 1.0};
    record r2 {2, 2.0};
    record r3 {3, 3.0};
    accessor::record_ref ref1{&r1, sizeof(r1)};
    accessor::record_ref ref2{&r2, sizeof(r2)};
    accessor::record_ref ref3{&r3, sizeof(r3)};

    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.flush();
    EXPECT_EQ(3, std::distance(partition.begin(), partition.end()));
}

TEST_F(input_partition_test, use_monotonic_resource) {
    memory::page_pool pool{};
    auto resource = std::make_unique<memory::monotonic_paged_memory_resource>(&pool);
    input_partition partition{
            std::move(resource),
            std::make_shared<shuffle_info>(test_record_meta1(), std::vector<std::size_t>{0})};

    record r1 {1, 1.0};
    record r2 {2, 2.0};
    record r3 {3, 3.0};
    accessor::record_ref ref1{&r1, sizeof(r1)};
    accessor::record_ref ref2{&r2, sizeof(r2)};
    accessor::record_ref ref3{&r3, sizeof(r3)};

    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.flush();

    EXPECT_EQ(3, std::distance(partition.begin(), partition.end()));
}

}

