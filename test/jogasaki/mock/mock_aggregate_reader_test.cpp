/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/executor/exchange/mock/aggregate/reader.h>

#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/mock/aggregate/shuffle_info.h>
#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::exchange::mock::aggregate {

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

class mock_aggregate_reader_test : public test_root {
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

    std::unique_ptr<input_partition> create_input_partition(request_context* context, std::vector<std::pair<std::int64_t, double>> const& map) {
        auto partition = std::make_unique<input_partition>(
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            std::make_unique<mock_memory_resource>(),
            info_, context);

        for(auto& p: map) {
            test::record r1 {p.first, p.second};
            partition->write(r1.ref());
        }
        partition->flush();
        return partition;
    }

    std::multimap<std::int64_t, double> read_result(reader& r) {
        std::multimap<std::int64_t, double> ret{};
        while(r.next_group()) {
            auto k = get_key(r.get_group());
            while(r.next_member()) {
                auto v = get_value(r.get_member());
                ret.emplace(k, v);
            }
        }
        return ret;
    }
    maybe_shared_ptr<meta::record_meta> meta_ = test_record_meta1();
    std::shared_ptr<shuffle_info> info_ = std::make_shared<shuffle_info>(meta_, std::vector<std::size_t>{0}, aggregator_);
};

using kind = meta::field_type_kind;

TEST_F(mock_aggregate_reader_test, basic) {
    auto context = std::make_shared<request_context>();

    std::set<key_type> keys{};
    std::set<value_type> values{};

    std::vector<std::pair<std::int64_t, double>>
        v0 = {
        {10, 1.0},
    },
        v1 = {
        {3, 3.0},
        {1, 1.0},
        {2, 2.0},
        {4, 4.0},
    },
        v2 = {
        {2, 20.0},
        {1, 10.0},
    },
        empty = {
    },
        v3 = {
        {2, 200.0},
        {3, 300.0},
    };
    std::vector<std::unique_ptr<input_partition>> partitions{};
    partitions.emplace_back(create_input_partition(context.get(), v0));
    partitions.emplace_back(create_input_partition(context.get(), v1));
    partitions.emplace_back(create_input_partition(context.get(), v2));
    partitions.emplace_back(create_input_partition(context.get(), empty));
    partitions.emplace_back(create_input_partition(context.get(), v3));

    reader r{info_, partitions, *aggregator_};

    auto result = read_result(r);
    std::multimap<std::int64_t, double> exp{
        {1, 11.0},
        {2, 222.0},
        {3, 303.0},
        {4, 4.0},
        {10, 1.0},
    };
    EXPECT_EQ(exp, result);
}

}

