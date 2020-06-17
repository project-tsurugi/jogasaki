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

#include <jogasaki/executor/exchange/aggregate/input_partition.h>

#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/aggregate/shuffle_info.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::exchange::aggregate {

using namespace data;
using namespace executor;
using namespace meta;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aggregate_input_partition_test : public test_root {
public:

    using key_type = std::int64_t;
    using value_type = double;

    key_type get_key(accessor::record_ref key) {
        return key.get_value<key_type>(info_->key_meta()->value_offset(0));
    }
    value_type get_value(accessor::record_ref value) {
        return value.get_value<value_type>(info_->value_meta()->value_offset(0));
    }
    void set_value(accessor::record_ref value, value_type arg) {
        value.set_value<value_type>(info_->value_meta()->value_offset(0), arg);
    }
    std::shared_ptr<shuffle_info::aggregator_type> aggregator_ = std::make_shared<shuffle_info::aggregator_type>([this](meta::record_meta const*, accessor::record_ref target, accessor::record_ref source){
        auto new_value = get_value(target) + get_value(source);
        set_value(target, new_value);
    });
    std::shared_ptr<meta::record_meta> meta_ = test_record_meta1();
    std::shared_ptr<shuffle_info> info_ = std::make_shared<shuffle_info>(meta_, std::vector<std::size_t>{0}, aggregator_);
};

using kind = meta::field_type_kind;

TEST_F(aggregate_input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info_, context};
    record r1 {1, 1.0};
    record r2 {2, 2.0};
    record r3 {3, 3.0};
    accessor::record_ref ref1{&r1, sizeof(r1)};
    accessor::record_ref ref2{&r2, sizeof(r2)};
    accessor::record_ref ref3{&r3, sizeof(r3)};

    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.write(ref2);
    partition.flush();
    ASSERT_EQ(1, partition.tables_count());
    auto map = partition.maps(0);
    EXPECT_EQ(3, map.size());

    std::set<key_type> keys{};
    std::set<value_type> values{};
    ASSERT_TRUE(map.next());
    keys.emplace(get_key(map.key()));
    values.emplace(get_value(map.value()));
    ASSERT_TRUE(map.next());
    keys.emplace(get_key(map.key()));
    values.emplace(get_value(map.value()));
    ASSERT_TRUE(map.next());
    keys.emplace(get_key(map.key()));
    values.emplace(get_value(map.value()));
    ASSERT_FALSE(map.next());
    std::set<key_type> keys_exp{1,2,3};
    std::set<value_type> values_exp{1.0,4.0,3.0};
    ASSERT_EQ(keys_exp, keys);
    ASSERT_EQ(values_exp, values);
}

TEST_F(aggregate_input_partition_test, multiple_hash_tables) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        info_, context};
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
    partition.write(ref3);
    partition.write(ref1);
    partition.write(ref2);
    partition.flush();
    ASSERT_EQ(2, partition.tables_count());
    auto map0 = partition.maps(0);
    auto map1 = partition.maps(1);
    EXPECT_EQ(3, map0.size());
    EXPECT_EQ(3, map1.size());

    auto key_size = info_->key_meta()->record_size();
    auto value_size = info_->value_meta()->record_size();

    std::set<key_type> keys0{}, keys1{};
    std::set<value_type> values0{}, values1{};

    {
        ASSERT_TRUE(map0.next());
        keys0.emplace(get_key(map0.key()));
        values0.emplace(get_value(map0.value()));
        auto e1 = map1.find(map0.key());
        ASSERT_NE(map1.end(), e1);
        keys1.emplace(get_key(accessor::record_ref(e1->first, key_size)));
        values1.emplace(get_key(accessor::record_ref(e1->second, value_size)));
        map1.erase(e1);
    }
    {
        ASSERT_TRUE(map0.next());
        keys0.emplace(get_key(map0.key()));
        values0.emplace(get_value(map0.value()));
        auto e2 = map1.find(map0.key());
        ASSERT_NE(map1.end(), e2);
        keys1.emplace(get_key(accessor::record_ref(e2->first, key_size)));
        values1.emplace(get_key(accessor::record_ref(e2->second, value_size)));
        map1.erase(e2);
    }

    {
        ASSERT_TRUE(map0.next());
        keys0.emplace(get_key(map0.key()));
        values0.emplace(get_value(map0.value()));
        auto e3 = map1.find(map0.key());
        ASSERT_NE(map1.end(), e3);
        keys1.emplace(get_key(accessor::record_ref(e3->first, key_size)));
        values1.emplace(get_key(accessor::record_ref(e3->second, value_size)));
        map1.erase(e3);
    }

    ASSERT_FALSE(map0.next());
    ASSERT_FALSE(map1.next());
    EXPECT_EQ(3, map0.size());
    EXPECT_EQ(0, map1.size());
    std::set<key_type> keys_exp{1,2,3};
    std::set<value_type> values_exp{1.0,2.0,3.0};
    ASSERT_EQ(keys_exp, keys0);
    ASSERT_EQ(values_exp, values0);
    ASSERT_EQ(keys_exp, keys1);
    ASSERT_EQ(values_exp, values1);
}

}

