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
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/meta_type.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils/record.h>

namespace jogasaki::testing {

using namespace data;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace jogasaki::executor;
using namespace boost::container::pmr;

class iterable_record_store_test : public test_root {};

TEST_F(iterable_record_store_test, empty) {
    mock_memory_resource memory{};
    iterable_record_store r{&memory, &memory, test_record_meta1()};
    ASSERT_TRUE(r.empty());
    ASSERT_EQ(0, r.count());

    ASSERT_EQ(r.begin(), r.end());
}

TEST_F(iterable_record_store_test, basic) {
    mock_memory_resource memory{};
    test::record rec{2, 2.0};
    auto meta = rec.record_meta();
    iterable_record_store r{&memory, &memory, meta};
    ASSERT_TRUE(r.empty());
    auto res1 = r.append(rec.ref());
    ASSERT_FALSE(r.empty());
    rec.key(1);
    rec.value(1.0);
    auto res2 = r.append(rec.ref());
    ASSERT_EQ(2, r.count());
    auto offset_c0 = meta->value_offset(0);
    EXPECT_EQ(2, res1.get_value<std::int64_t>(offset_c0));
    EXPECT_EQ(1, res2.get_value<std::int64_t>(offset_c0));

    // iterate
    auto it = r.begin();
    auto at0 = r.begin();
    auto at1 = ++r.begin();
    auto at2 = ++++r.begin();

    EXPECT_EQ(at0, it);
    ASSERT_EQ(r.begin(), it);
    ASSERT_NE(r.end(), it);

    compare_info cm{*meta};
    comparator comp{cm};
    EXPECT_EQ(0, comp(res1, *it));
    EXPECT_NE(0, comp(res2, *it));

    auto it2 = it++;
    EXPECT_EQ(at0, it2);
    EXPECT_EQ(at1, it);
    EXPECT_EQ(0, comp(res2, *it));
    ASSERT_NE(r.begin(), it);
    ASSERT_NE(r.end(), it);

    auto it3 = it++;
    EXPECT_EQ(at1, it3);
    EXPECT_EQ(at2, it);
    ASSERT_EQ(r.end(), it);
    ASSERT_NE(r.begin(), it);
}

TEST_F(iterable_record_store_test, multiple_pointer_intervals) {
    mock_memory_resource memory{0, 1};
    test::record rec2{2, 2.0};
    auto meta = rec2.record_meta();
    iterable_record_store r{&memory, &memory, meta};
    auto p2 = r.append(rec2.ref());

    test::record rec1{1, 1.0};
    auto p1 = r.append(rec1.ref());

    test::record rec3{3, 3.0};
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
    compare_info cm{*meta};
    comparator comp{cm};
    EXPECT_EQ(0, comp(p2, *it));
    EXPECT_NE(0, comp(p1, *it));

    auto it2 = it++;
    EXPECT_EQ(at0, it2);
    EXPECT_EQ(at1, it);
    EXPECT_EQ(0, comp(p1, *it));
    ASSERT_NE(r.begin(), it);
    ASSERT_NE(r.end(), it);
    EXPECT_EQ(0, comp(p1, *it));

    auto it3 = it++;
    EXPECT_EQ(at1, it3);
    EXPECT_EQ(at2, it);
    ASSERT_NE(r.end(), it);
    ASSERT_NE(r.begin(), it);
    EXPECT_EQ(0, comp(p3, *it));

    auto it4 = it++;
    EXPECT_EQ(at2, it4);
    EXPECT_EQ(at3, it);
    ASSERT_EQ(r.end(), it);
    ASSERT_NE(r.begin(), it);
}

TEST_F(iterable_record_store_test, record_ref) {
    mock_memory_resource memory{};
    test::record rec{2, 2.0};
    auto meta = rec.record_meta();
    iterable_record_store r{&memory, &memory, meta};
    auto res1 = r.append(rec.ref());
    compare_info cm{*meta};
    comparator comp{cm};
    auto it = r.begin();
    EXPECT_EQ(0, comp(res1, it.ref()));
}

TEST_F(iterable_record_store_test, record_of_length_zero) {
    // We support zero length record stored in the store.
    // In this case, one byte is allocated to advance the pointer while record size is returned as zero to the caller.
    mock_memory_resource memory{};

    // use record of length zero
    meta::record_meta meta{};
    accessor::record_ref rec{};

    iterable_record_store r{&memory, &memory, maybe_shared_ptr{&meta}};
    auto res1 = r.append(rec);
    auto res2 = r.append(rec);
    auto res3 = r.append(rec);

    // even if length is zero, the pointer should be different
    EXPECT_LT(res1.data(), res2.data());
    EXPECT_LT(res2.data(), res3.data());

    EXPECT_EQ(0, res1.size());
    EXPECT_EQ(0, res2.size());
    EXPECT_EQ(0, res3.size());

    compare_info cm{meta};
    comparator comp{cm};
    auto it = r.begin();
    EXPECT_EQ(0, comp(res1, it.ref()));
    ++it;
    EXPECT_NE(r.end(), it);
    EXPECT_EQ(0, comp(res2, it.ref()));
    ++it;
    EXPECT_NE(r.end(), it);
    EXPECT_EQ(0, comp(res3, it.ref()));
    ++it;
    EXPECT_EQ(r.end(), it);
}

}
