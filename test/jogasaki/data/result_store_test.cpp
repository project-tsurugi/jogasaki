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

#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::data {

using namespace data;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class result_store_test : public test_root {};

TEST_F(result_store_test, basic) {
    result_store result{};
    test::record rec{0, 0.0};
    ASSERT_TRUE(result.empty());
    ASSERT_EQ(0, result.partitions());
    ASSERT_FALSE(result.exists(0));
    result.initialize(3, rec.record_meta());
    ASSERT_EQ(3, result.partitions());
    ASSERT_TRUE(result.empty());
    ASSERT_TRUE(result.exists(0));
    result.partition(0).append(rec.ref());
    ASSERT_FALSE(result.empty());

    auto it = result.begin();
    ASSERT_NE(result.end(), it);
    ++it;
    ASSERT_EQ(result.end(), it);
}

TEST_F(result_store_test, iterator) {
    result_store result{};
    test::record rec0{0, 0.0};
    test::record rec1{1, 1.0};
    auto meta = rec0.record_meta();
    result.initialize(3, meta);
    ASSERT_TRUE(result.exists(1));
    result.partition(1).append(rec1.ref());
    ASSERT_FALSE(result.empty());
    ASSERT_TRUE(result.exists(0));
    result.partition(0).append(rec0.ref());
    auto it = result.begin();
    ASSERT_NE(result.end(), it);
    ASSERT_EQ(rec0, mock::basic_record(*it, meta));
    ++it;
    ASSERT_EQ(rec1, mock::basic_record(*it, meta));
    ++it;
    ASSERT_EQ(result.end(), it);
    ASSERT_EQ(2, std::distance(result.begin(), result.end()));
}

TEST_F(result_store_test, empty_internal_store) {
    result_store result{};
    test::record rec0{0, 0.0};
    test::record rec1{1, 1.0};
    test::record rec2{2, 2.0};
    auto meta = rec0.record_meta();
    result.initialize(3, meta);
    result.partition(2).append(rec1.ref());
    result.partition(2).append(rec2.ref());
    result.partition(0).append(rec0.ref());
    auto it = result.begin();
    ASSERT_NE(result.end(), it);
    ASSERT_EQ(rec0, mock::basic_record(*it, meta));
    ++it;
    ASSERT_EQ(rec1, mock::basic_record(*it, meta));
    ++it;
    ASSERT_EQ(rec2, mock::basic_record(*it, meta));
    ++it;
    ASSERT_EQ(result.end(), it);
    ASSERT_EQ(3, std::distance(result.begin(), result.end()));
}

TEST_F(result_store_test, empty_iterator) {
    result_store result{};
    ASSERT_TRUE(result.empty());
    ASSERT_EQ(result.end(), result.begin());
    test::record rec0{0, 0.0};
    auto meta = rec0.record_meta();
    result.initialize(3, meta);
    ASSERT_TRUE(result.empty());
    ASSERT_EQ(result.end(), result.begin());
    ASSERT_EQ(0, std::distance(result.begin(), result.end()));
}

}

