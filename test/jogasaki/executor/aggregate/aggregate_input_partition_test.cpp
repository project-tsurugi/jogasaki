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

#include <gtest/gtest.h>

#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/builtin_functions.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::exchange::aggregate {

using namespace testing;
using namespace data;
using namespace executor;
using namespace meta;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;
using namespace jogasaki::executor::exchange::shuffle;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aggregate_input_partition_test : public test_root {
public:
    using iterator = pointer_table::iterator;
    maybe_shared_ptr<meta::group_meta> group_meta_{};

    std::function<std::int64_t(iterator)> get_key = [&](iterator it) {
        auto key = accessor::record_ref(*it, group_meta_->key_shared()->record_size());
        return key.get_value<std::int64_t>(group_meta_->key_shared()->value_offset(0));
    };

    void* value_pointer(accessor::record_ref ref) const {
        return ref.get_value<void*>(group_meta_->key().value_offset(group_meta_->key().field_count()-1));
    }

    std::function<double(iterator)> get_val = [&](iterator it) {
        auto key = accessor::record_ref(*it, group_meta_->key_shared()->record_size());
        auto p = value_pointer(key);
        return accessor::record_ref(p, group_meta_->value_shared()->record_size()).get_value<double>(group_meta_->value_shared()->value_offset(0));
    };
};


using kind = meta::field_type_kind;

TEST_F(aggregate_input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    auto info = std::make_shared<aggregate_info>(test_record_meta1(), std::vector<std::size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                builtin::sum,
                {
                    1
                },
                meta::field_type(enum_tag<kind::float8>)
            }
        }
    );
    input_partition partition{ info };
    test::nullable_record r1 {1, 1.0};
    test::nullable_record r21 {2, 1.0};
    test::nullable_record r22 {2, 2.0};
    test::nullable_record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r21.ref());
    partition.write(r1.ref());
    partition.write(r22.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
    group_meta_ = info->mid_group_meta();
    auto it = t.begin();
    EXPECT_EQ(1, get_key(it));
    EXPECT_DOUBLE_EQ(1.0, get_val(it));
    ++it;
    EXPECT_EQ(2, get_key(it));
    EXPECT_DOUBLE_EQ(3.0, get_val(it));
    ++it;
    EXPECT_EQ(3, get_key(it));
    EXPECT_DOUBLE_EQ(3.0, get_val(it));
    ++it;
    EXPECT_EQ(t.end(), it);
}

}

