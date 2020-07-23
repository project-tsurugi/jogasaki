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
#include <jogasaki/plan/relation_step_map.h>

#include <string>

#include <yugawara/binding/factory.h>
#include <takatori/util/object_creator.h>
#include <takatori/plan/forward.h>
#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/executor/exchange/forward/step.h>

#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/mock/process_executor.h>

namespace jogasaki::plan {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;
using namespace yugawara::binding;

class relation_step_map_test : public test_root {

};

TEST_F(relation_step_map_test, basic) {
    factory f;
    ::takatori::plan::forward f1 {
        f.exchange_column(),
    };
    ::takatori::plan::forward f2 {
        f.exchange_column(),
    };
    auto e1 = f.exchange(f1);
    auto e2 = f.exchange(f2);

    executor::exchange::forward::step s1{};
    executor::exchange::forward::step s2{};

    relation_step_map::entity_type map{};
    map[e1] = &s1;
    map[e2] = &s2;

    relation_step_map m{std::move(map)};
    auto& i1 = m.at(e1);
    ASSERT_EQ(&s1, i1);
    auto& i2 = m.at(e2);
    ASSERT_EQ(&s2, i2);
}

}

