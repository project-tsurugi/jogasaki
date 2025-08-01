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
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::exchange::group {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace memory;
using namespace executor;
using namespace takatori::util;

class shuffle_flow_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(shuffle_flow_test, simple) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    auto context = std::make_shared<request_context>();

    flow f{rec_meta, std::vector<std::size_t>{0}, context.get(), nullptr, 1};
    f.setup_partitions(1);
    ASSERT_EQ(1, f.sink_count());
}

TEST_F(shuffle_flow_test, writers) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    auto context = std::make_shared<request_context>();
    flow f{rec_meta, std::vector<std::size_t>{0}, context.get(), nullptr, 1};
    f.setup_partitions(1);
    EXPECT_EQ(1, f.sink_count());
    auto& sink = f.sink_at(0);
    auto& source = f.source_at(0);
    auto& writer = sink.acquire_writer();

    page_pool pool{};
    monotonic_paged_memory_resource resource{&pool};
    auto offset_c1 = rec_meta->value_offset(0);
    auto offset_c2 = rec_meta->value_offset(0);
    for(std::size_t i = 0; i < 3; ++i) {
        auto sz = rec_meta->record_size();
        auto ptr = resource.allocate(sz, rec_meta->record_alignment());
        auto ref = accessor::record_ref(ptr, sz);
        ref.set_value<std::int64_t>(offset_c1, i);
        ref.set_value<double>(offset_c2, i);
        writer.write(ref);
    }
    writer.flush();
    f.transfer();
    auto reader_container = source.acquire_reader();
    auto& reader = *reader_container.reader<io::group_reader>();
    std::size_t count = 0;
    while(reader.next_group()) {
        while(reader.next_member()) {
            ++count;
        }
    }
    EXPECT_EQ(3, count);
}

}
