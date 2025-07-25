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
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/exception.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::process::impl::details {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class io_exchange_map_test : public test_root {};

TEST_F(io_exchange_map_test, basic) {
    io_exchange_map map{};

    exchange::forward::step in0{};
    exchange::forward::step in1{};
    exchange::forward::step out0{};
    exchange::forward::step out1{};
    map.add_input(&in0);
    map.add_output(&out0);
    map.add_input(&in1);
    map.add_output(&out1);

    EXPECT_EQ(0, map.input_index(&in0));
    EXPECT_EQ(1, map.input_index(&in1));
    EXPECT_EQ(&in0, map.input_at(0));
    EXPECT_EQ(&in1, map.input_at(1));

    EXPECT_EQ(0, map.output_index(&out0));
    EXPECT_EQ(1, map.output_index(&out1));
    EXPECT_EQ(&out0, map.output_at(0));
    EXPECT_EQ(&out1, map.output_at(1));
}

}

