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
#include <jogasaki/executor/process/impl/variable_value_map.h>

#include <string>

#include <yugawara/binding/factory.h>
#include <takatori/util/object_creator.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>

#include "mock/task_context.h"
#include "mock/process_executor.h"

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace yugawara::binding;

class variable_value_map_test : public test_root {

};

TEST_F(variable_value_map_test, basic) {
    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.stream_variable("v2");
    variable_value_map::entity_type map{};
    map[v1] = value_info{1, 1};
    map[v2] = value_info{2,2};

    variable_value_map m{std::move(map)};
    auto& i1 = m.at(v1);
    ASSERT_EQ(1, i1.value_offset());
    auto& i2 = m.at(v2);
    ASSERT_EQ(2, i2.value_offset());
}

}

