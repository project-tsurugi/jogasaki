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

#include <gtest/gtest.h>
#include <jogasaki/plan/statement_work_level.h>

#include <jogasaki/test_root.h>

namespace jogasaki::plan {

using namespace std::literals::string_literals;

class statement_work_level_test : public test_root {};

TEST_F(statement_work_level_test, basic) {
    statement_work_level w{};
    statement_work_level exp{statement_work_level_kind::undefined};
    statement_work_level zero{statement_work_level_kind::zero};
    ASSERT_EQ(exp, w);
    ASSERT_NE(zero, w);
}

TEST_F(statement_work_level_test, set_minimum) {
    statement_work_level w{};
    statement_work_level zero{statement_work_level_kind::zero};
    statement_work_level simple_write{statement_work_level_kind::simple_write};

    w.set_minimum(statement_work_level_kind::zero);
    ASSERT_EQ(zero, w);
    w.set_minimum(statement_work_level_kind::simple_write);
    ASSERT_EQ(simple_write, w);
    w.set_minimum(statement_work_level_kind::zero);
    ASSERT_EQ(simple_write, w);
}
}
