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
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/sink.h>
#include <jogasaki/executor/exchange/group/writer.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
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
using namespace executor;
using namespace memory;
using namespace takatori::util;

class shuffle_sink_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(shuffle_sink_test, simple) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    auto info = std::make_shared<group_info>(rec_meta, std::vector<std::size_t>{0});
    auto context = std::make_shared<request_context>();
    sink s{1UL, info, context.get()};
    auto key_meta = info->key_meta();

    page_pool pool{};
    monotonic_paged_memory_resource resource{&pool};
    auto offset_c1 = rec_meta->value_offset(0);
    auto offset_c2 = rec_meta->value_offset(0);
    auto& writer = s.acquire_writer();
    for(std::size_t i = 0; i < 3; ++i) {
        auto sz = rec_meta->record_size();
        auto ptr = resource.allocate(sz, rec_meta->record_alignment());
        auto ref = accessor::record_ref(ptr, sz);
        ref.set_value<std::int64_t>(offset_c1, i);
        ref.set_value<double>(offset_c2, i);
        writer.write(ref);
    }
    writer.flush();
}

}
