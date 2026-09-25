/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <stdexcept>

#include <gtest/gtest.h>

#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/step_state.h>
#include <jogasaki/scheduler/step_state_table.h>

namespace jogasaki::scheduler {

TEST(step_state_table_test, complete_registered_main_task) {
    step_state_table table{};
    table.assign_slot(model::task_kind::main, 1);
    table.register_task(model::task_kind::main, 0, 100);

    EXPECT_EQ(model::task_kind::main, table.task_state(100, task_state_kind::running));
    EXPECT_EQ(model::task_kind::main, table.task_state(100, task_state_kind::completed));
    EXPECT_TRUE(table.completed(model::task_kind::main));
}

TEST(step_state_table_test, reject_unknown_task_completion) {
    step_state_table table{};
    table.assign_slot(model::task_kind::main, 1);

    EXPECT_THROW(table.task_state(100, task_state_kind::completed), std::domain_error);
}

TEST(step_state_table_test, preserve_duplicate_task_completion) {
    step_state_table table{};
    table.assign_slot(model::task_kind::main, 1);
    table.register_task(model::task_kind::main, 0, 100);
    table.task_state(100, task_state_kind::running);
    table.task_state(100, task_state_kind::completed);

    EXPECT_NO_THROW(table.task_state(100, task_state_kind::completed));
    EXPECT_TRUE(table.completed(model::task_kind::main));
}

TEST(step_state_table_test, preserve_transition_after_completion) {
    step_state_table table{};
    table.assign_slot(model::task_kind::pre, 1);
    table.register_task(model::task_kind::pre, 0, 100);
    table.task_state(100, task_state_kind::running);
    table.task_state(100, task_state_kind::completed);

    EXPECT_NO_THROW(table.task_state(100, task_state_kind::running));
    EXPECT_FALSE(table.completed(model::task_kind::pre));
}

}
