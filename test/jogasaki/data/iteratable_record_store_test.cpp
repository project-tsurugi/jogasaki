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

#include <jogasaki/data/iteratable_record_store.h>

#include <gtest/gtest.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/mock_memory_resource.h>

#include <jogasaki/test_root.h>

namespace jogasaki::testing {

using namespace data;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class iteratable_record_store_test : public test_root {};

struct S {
    std::int64_t x_;
    double y_;
    accessor::record_ref ref() {
        return accessor::record_ref{this, sizeof(S)};
    }
};

S create_record(int x, double y) {
    S buffer{};
    buffer.x_ = x;
    buffer.y_ = y;
    return buffer;
}

TEST_F(iteratable_record_store_test, empty) {
    mock_memory_resource memory{};
    iteratable_record_store r{&memory, &memory, test_record_meta1()};
    ASSERT_TRUE(r.empty());
    ASSERT_EQ(0, r.count());

    ASSERT_EQ(r.begin(), r.end());
}

TEST_F(iteratable_record_store_test, basic) {
    mock_memory_resource memory{};
    testing::record rec{2, 2.0};
    auto meta = rec.record_meta();
    iteratable_record_store r{&memory, &memory, meta};
    ASSERT_TRUE(r.empty());
    auto p1 = r.append(rec.ref());
    ASSERT_FALSE(r.empty());
    rec.key(1);
    rec.value(1.0);
    auto p2 = r.append(rec.ref());
    ASSERT_EQ(2, r.count());
    auto sz = rec.record_meta()->record_size();
    record_ref res1{p1, sz};
    auto offset_c0 = meta->value_offset(0);
    EXPECT_EQ(2, res1.get_value<std::int64_t>(offset_c0));
    record_ref res2{p2, sz};
    EXPECT_EQ(1, res2.get_value<std::int64_t>(offset_c0));

    // iterate
    auto it = r.begin();
    auto at0 = r.begin();
    auto at1 = ++r.begin();
    auto at2 = ++++r.begin();

    EXPECT_EQ(at0, it);
    ASSERT_EQ(r.begin(), it);
    ASSERT_NE(r.end(), it);
    EXPECT_EQ(p1, *it);
    EXPECT_NE(p2, *it);

    auto it2 = it++;
    EXPECT_EQ(at0, it2);
    EXPECT_EQ(at1, it);
    EXPECT_EQ(p2, *it);
    ASSERT_NE(r.begin(), it);
    ASSERT_NE(r.end(), it);

    auto it3 = it++;
    EXPECT_EQ(at1, it3);
    EXPECT_EQ(at2, it);
    ASSERT_EQ(r.end(), it);
    ASSERT_NE(r.begin(), it);
}

TEST_F(iteratable_record_store_test, multiple_pointer_intervals) {
    mock_memory_resource memory{0, 1};
    testing::record rec2{2, 2.0};
    auto meta = rec2.record_meta();
    iteratable_record_store r{&memory, &memory, meta};
    auto p2 = r.append(rec2.ref());

    testing::record rec1{1, 1.0};
    auto p1 = r.append(rec1.ref());

    testing::record rec3{3, 3.0};
    auto p3 = r.append(rec3.ref());
    ASSERT_EQ(3, r.count());

    // iterate
    auto it = r.begin();
    auto at0 = r.begin();
    auto at1 = ++r.begin();
    auto at2 = ++++r.begin();
    auto at3 = ++++++r.begin();

    EXPECT_EQ(at0, it);
    ASSERT_EQ(r.begin(), it);
    ASSERT_NE(r.end(), it);
    EXPECT_EQ(p2, *it);
    EXPECT_NE(p1, *it);

    auto it2 = it++;
    EXPECT_EQ(at0, it2);
    EXPECT_EQ(at1, it);
    EXPECT_EQ(p1, *it);
    ASSERT_NE(r.begin(), it);
    ASSERT_NE(r.end(), it);
    EXPECT_EQ(p1, *it);

    auto it3 = it++;
    EXPECT_EQ(at1, it3);
    EXPECT_EQ(at2, it);
    ASSERT_NE(r.end(), it);
    ASSERT_NE(r.begin(), it);
    EXPECT_EQ(p3, *it);

    auto it4 = it++;
    EXPECT_EQ(at2, it4);
    EXPECT_EQ(at3, it);
    ASSERT_EQ(r.end(), it);
    ASSERT_NE(r.begin(), it);
}

}

