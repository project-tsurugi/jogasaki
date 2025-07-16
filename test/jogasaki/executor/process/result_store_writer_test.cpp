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
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/result_store_writer.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/test_utils/types.h>

namespace jogasaki::executor::process {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;
using namespace std::chrono_literals;

using namespace jogasaki::memory;
using namespace jogasaki::mock;
using namespace boost::container::pmr;

class result_store_writer_test : public ::testing::Test {};

TEST_F(result_store_writer_test, basic) {
    mock_memory_resource record_resource{};
    mock_memory_resource varlen_resource{};
    using kind = meta::field_type_kind;
    auto meta = create_meta<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>();
    data::iterable_record_store store{
        &record_resource,
        &varlen_resource,
        meta
    };
    result_store_writer writer{store, meta};

    auto rec1 = create_record<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>(1, 10.0, 100, 1000.0, accessor::text{"111"});
    auto rec2 = create_record<kind::int4, kind::float8, kind::int8, kind::float4, kind::character>(2, 20.0, 200, 2000.0, accessor::text{"222"});
    auto record_size = meta->record_size();
    writer.write(rec1.ref());
    writer.write(rec2.ref());

    compare_info cm{*meta};
    comparator comp{cm};
    auto b = store.begin();
    EXPECT_EQ(0, comp(rec1.ref(), b.ref()));
    ++b;
    EXPECT_EQ(0, comp(rec2.ref(), b.ref()));
    ++b;
    EXPECT_EQ(store.end(), b);
}

TEST_F(result_store_writer_test, temporal_types) {
    mock_memory_resource record_resource{};
    mock_memory_resource varlen_resource{};
    using kind = meta::field_type_kind;
    auto meta = create_meta<kind::int4, kind::date, kind::time_of_day, kind::time_point>();
    data::iterable_record_store store{
        &record_resource,
        &varlen_resource,
        meta
    };
    result_store_writer writer{store, meta};

    auto rec1 = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(1, rtype<ft::date>{10}, rtype<ft::time_of_day>{100ns}, rtype<ft::time_point>{1000ns});
    auto rec2 = create_record<kind::int4, kind::date, kind::time_of_day, kind::time_point>(2, rtype<ft::date>{20}, rtype<ft::time_of_day>{200ns}, rtype<ft::time_point>{2000ns});
    auto record_size = meta->record_size();
    writer.write(rec1.ref());
    writer.write(rec2.ref());

    compare_info cm{*meta};
    comparator comp{cm};
    auto b = store.begin();
    EXPECT_EQ(0, comp(rec1.ref(), b.ref()));
    ++b;
    EXPECT_EQ(0, comp(rec2.ref(), b.ref()));
    ++b;
    EXPECT_EQ(store.end(), b);
}
}

