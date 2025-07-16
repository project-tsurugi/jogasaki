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

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/meta_type.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>

//#include <jogasaki/executor/exchange/group/shuffle_info.h>

namespace jogasaki::executor::exchange::group {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace memory;
using namespace executor;
using namespace takatori::util;

class shuffle_step_test : public ::testing::Test {};

using kind = field_type_kind;

TEST_F(shuffle_step_test, simple) {
    auto rec_meta = std::make_shared<record_meta>(std::vector<field_type>{
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    step s{rec_meta, std::vector<std::size_t>{0}, meta::variable_order{}, meta::variable_order{}};
//    s.activate();
    EXPECT_EQ(0, s.input_ports().size());
    EXPECT_EQ(0, s.output_ports().size());
}

}
