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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/exchange/aggregate/input_partition.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>
#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/executor/function/incremental/aggregate_function_kind.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/request_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

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

using executor::function::incremental::aggregate_function_info_impl;
using executor::function::incremental::aggregate_function_kind;

class incremental_aggregate_input_partition_test : public test_root {
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

    std::function<accessor::record_ref(iterator)> get_key_record = [&](iterator it) {
        auto key = accessor::record_ref(*it, group_meta_->key_shared()->record_size());
        return key;
    };
    std::function<accessor::record_ref(iterator)> get_val_record = [&](iterator it) {
        auto key = accessor::record_ref(*it, group_meta_->key_shared()->record_size());
        auto p = value_pointer(key);
        return accessor::record_ref(p, group_meta_->value_shared()->record_size());
    };
};


using kind = meta::field_type_kind;

TEST_F(incremental_aggregate_input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    auto func_sum = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::sum>>();
    auto info = std::make_shared<aggregate_info>(test_record_meta1(), std::vector<std::size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                *func_sum,
                {
                    1
                },
                meta::field_type(field_enum_tag<kind::float8>)
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
    group_meta_ = info->mid().group_meta();
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

TEST_F(incremental_aggregate_input_partition_test, avg) {
    auto context = std::make_shared<request_context>();
    auto func_sum = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::avg>>();
    auto info = std::make_shared<aggregate_info>(test_record_meta1(), std::vector<std::size_t>{0},
        std::vector<aggregate_info::value_spec>{
            {
                *func_sum,
                {
                    1
                },
                meta::field_type(field_enum_tag<kind::float8>)
            }
        }
    );
    input_partition partition{ info };
    test::nullable_record r1 {1, 1.0};
    test::nullable_record r21 {2, 2.0};
    test::nullable_record r22 {2, 4.0};
    test::nullable_record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r21.ref());
    partition.write(r1.ref());
    partition.write(r22.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
    group_meta_ = info->pre().group_meta();
    auto key_meta = group_meta_->key_shared();
    auto val_meta = group_meta_->value_shared();
    auto it = t.begin();
    EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::pointer>(1, nullptr)), mock::basic_record(get_key_record(it), key_meta));
    EXPECT_EQ((mock::create_nullable_record<kind::float8, kind::int8>(1.0, 1)), mock::basic_record(get_val_record(it), val_meta));
    ++it;
    EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::pointer>(2, nullptr)), mock::basic_record(get_key_record(it), key_meta));
    EXPECT_EQ((mock::create_nullable_record<kind::float8, kind::int8>(6.0, 2)), mock::basic_record(get_val_record(it), val_meta));
    ++it;
    EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::pointer>(3, nullptr)), mock::basic_record(get_key_record(it), key_meta));
    EXPECT_EQ((mock::create_nullable_record<kind::float8, kind::int8>(3.0, 1)), mock::basic_record(get_val_record(it), val_meta));
    ++it;
    EXPECT_EQ(t.end(), it);
}

}

