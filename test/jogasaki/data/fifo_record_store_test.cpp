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
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/fifo_record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/fifo_paged_memory_resource.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::data {

using namespace data;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class fifo_record_store_test : public test_root {};

TEST_F(fifo_record_store_test, basic) {
    fifo_paged_memory_resource memory{&global::page_pool()};
    test::record rec{2, 2.0};
    auto meta = rec.record_meta();
    fifo_record_store r{&memory, &memory, meta};
    ASSERT_TRUE(r.empty());
    auto p1 = r.push(rec.ref());
    ASSERT_FALSE(r.empty());
    rec.key(1);
    rec.value(1.0);
    auto p2 = r.push(rec.ref());
    ASSERT_EQ(2, r.count());
    auto sz = meta->record_size();
    record_ref res1{p1, sz};
    auto offset = meta->value_offset(0);
    EXPECT_EQ(2, res1.get_value<std::int64_t>(offset));
    record_ref res2{p2, sz};
    EXPECT_EQ(1, res2.get_value<std::int64_t>(offset));
}

}

