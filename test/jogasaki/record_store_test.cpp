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

#include <jogasaki/data/record_store.h>

#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock_memory_resource.h>
#include "test_root.h"


namespace jogasaki::testing {

using namespace data;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class record_store_test : public test_root {};

TEST_F(record_store_test, basic) {
    mock_memory_resource memory{};
    record_store r{&memory, &memory, test_record_meta1()};
    struct S {
        std::int64_t x_;
        double y_;
    };
    ASSERT_TRUE(r.empty());
    S buffer{};
    buffer.x_ = 2;
    buffer.y_ = 2.0;
    record_ref ref{&buffer, sizeof(S)};
    auto p1 = r.append(ref);
    ASSERT_FALSE(r.empty());
    buffer.x_ = 1;
    buffer.y_ = 1.0;
    auto p2 = r.append(ref);
    ASSERT_EQ(2, r.count());
    record_ref res1{p1, sizeof(S)};
    EXPECT_EQ(2, res1.get_value<std::int64_t>(0));
    record_ref res2{p2, sizeof(S)};
    EXPECT_EQ(1, res2.get_value<std::int64_t>(0));
}

}

