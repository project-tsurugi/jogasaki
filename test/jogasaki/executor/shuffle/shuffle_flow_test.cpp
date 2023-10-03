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

#include <jogasaki/executor/exchange/group/flow.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/exchange/group/group_info.h>

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
    const auto& [sinks, sources] = f.setup_partitions(1);
    ASSERT_EQ(1, sinks.size());
    (void)sinks;
    (void)sources;
}

TEST_F(shuffle_flow_test, writers) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    auto context = std::make_shared<request_context>();
    flow f{rec_meta, std::vector<std::size_t>{0}, context.get(), nullptr, 1};
    const auto& [sinks, sources] = f.setup_partitions(1);
    EXPECT_EQ(1, sinks.size());
    auto& sink = sinks[0];
    auto& source = sources[0];
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
