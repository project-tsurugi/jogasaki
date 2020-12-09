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

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aggregate_input_partition_test : public test_root {
public:
};

using kind = meta::field_type_kind;

TEST_F(aggregate_input_partition_test, basic) {
    auto context = std::make_shared<request_context>();
    input_partition partition{
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_unique<mock_memory_resource>(),
        std::make_shared<aggregate_info>(test_record_meta1(), std::vector<std::size_t>{0},
            std::vector<aggregate_info::value_spec>{
                {
                    builtin::sum,
                    {
                        1
                    },
                    meta::field_type(enum_tag<kind::float8>)
                }
            }
        )
    };
    test::nullable_record r1 {1, 1.0};
    test::nullable_record r21 {2, 1.0};
    test::nullable_record r22 {2, 2.0};
    test::nullable_record r3 {3, 3.0};

    partition.write(r3.ref());
    partition.write(r1.ref());
    partition.write(r21.ref());
    partition.write(r22.ref());
    partition.flush();
    ASSERT_EQ(1, std::distance(partition.begin(), partition.end())); //number of tables
    auto& t = *partition.begin();
    EXPECT_EQ(3, std::distance(t.begin(), t.end()));
}

}

