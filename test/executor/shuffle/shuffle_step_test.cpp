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

#include <executor/exchange/group/step.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>
#include <executor/exchange/group/shuffle_info.h>

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
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::float8>),
    },boost::dynamic_bitset<std::uint64_t>("00"s));
    step s{rec_meta, std::vector<std::size_t>{0}};
    s.activate();
    EXPECT_EQ(1, s.input_ports().size());
    EXPECT_EQ(1, s.output_ports().size());
    auto& in_port = s.input_ports()[0];
    auto& out_port = s.output_ports()[0];
    EXPECT_EQ(s, *in_port->owner());
    EXPECT_EQ(s, *out_port->owner());
}

}
