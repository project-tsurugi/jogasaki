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

using kind = meta::field_type_kind;
using record_pointer = fifo_record_store::record_pointer;

class fifo_record_store_test : public test_root {};

TEST_F(fifo_record_store_test, basic) {
    fifo_paged_memory_resource memory{&global::page_pool()};
    auto model = mock::create_record<kind::int4, kind::int4>();
    auto meta = model.record_meta();
    fifo_record_store r{&memory, &memory, meta};
    ASSERT_TRUE(r.empty());

    record_pointer p1{};
    record_pointer p2{};
    {
        auto rec = mock::create_record<kind::int4, kind::int4>(1, 10);
        p1 = r.push(rec.ref());
    }
    ASSERT_FALSE(r.empty());
    {
        auto rec = mock::create_record<kind::int4, kind::int4>(2, 20);
        p2 = r.push(rec.ref());
    }
    ASSERT_EQ(2, r.count());
    auto sz = meta->record_size();
    // verify with try_pop
    {
        record_ref res{};
        ASSERT_TRUE(r.try_pop(res));
        EXPECT_EQ((mock::create_record<kind::int4, kind::int4>(1, 10)), (mock::basic_record{res, meta}));
    }
    {
        record_ref res{};
        ASSERT_TRUE(r.try_pop(res));
        EXPECT_EQ((mock::create_record<kind::int4, kind::int4>(2, 20)), (mock::basic_record{res, meta}));
    }
    {
        record_ref res{};
        ASSERT_FALSE(r.try_pop(res));
    }

    // verify returned pointer
    {
        EXPECT_EQ((mock::create_record<kind::int4, kind::int4>(1, 10)), (mock::basic_record{record_ref{p1, sz}, meta}));
        EXPECT_EQ((mock::create_record<kind::int4, kind::int4>(2, 20)), (mock::basic_record{record_ref{p2, sz}, meta}));
    }
}

TEST_F(fifo_record_store_test, varlen_resource) {
    fifo_paged_memory_resource resource{&global::page_pool()};
    fifo_paged_memory_resource varlen_resource{&global::page_pool()};
    auto model = mock::create_record<kind::int4, kind::character, kind::character>();
    auto meta = model.record_meta();
    fifo_record_store r{&resource, &varlen_resource, meta};
    ASSERT_TRUE(r.empty());

    std::string_view text_data = "text data to verify varlen resource must long enough to avoid sso"sv;

    record_pointer p1{};
    record_pointer p2{};
    auto rec1 = mock::create_record<kind::int4, kind::character, kind::character>(1, text{text_data}, text{text_data});
    auto rec2 = mock::create_record<kind::int4, kind::character, kind::character>(2, text{text_data}, text{text_data});
    p1 = r.push(rec1.ref());
    ASSERT_FALSE(r.empty());
    p2 = r.push(rec2.ref());
    ASSERT_EQ(2, r.count());
    auto sz = meta->record_size();
    // verify with try_pop
    {
        record_ref res{};
        ASSERT_TRUE(r.try_pop(res));
        EXPECT_EQ(rec1, (mock::basic_record{res, meta}));
    }
    {
        record_ref res{};
        ASSERT_TRUE(r.try_pop(res));
        EXPECT_EQ(rec2, (mock::basic_record{res, meta}));
    }
    {
        record_ref res{};
        ASSERT_FALSE(r.try_pop(res));
    }

    // verify returned pointer
    {
        EXPECT_EQ(rec1, (mock::basic_record{record_ref{p1, sz}, meta}));
        EXPECT_EQ(rec2, (mock::basic_record{record_ref{p2, sz}, meta}));
    }
}

}

