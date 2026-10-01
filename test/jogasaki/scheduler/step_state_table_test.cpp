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
#include <initializer_list>
#include <stdexcept>
#include <string>

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

TEST(step_state_table_test, reject_updates_after_completion) {
    for (auto kind : {model::task_kind::main, model::task_kind::pre}) {
        for (auto next_state : {task_state_kind::uninitialized, task_state_kind::running, task_state_kind::completed}) {
            SCOPED_TRACE(to_string_view(kind));
            SCOPED_TRACE(to_string_view(next_state));
            step_state_table table{};
            table.state_ = step_state_kind::running;
            table.assign_slot(kind, 1);
            table.register_task(kind, 0, 100);
            EXPECT_EQ(kind, table.task_state(100, task_state_kind::running));
            EXPECT_EQ(kind, table.task_state(100, task_state_kind::completed));

            try {
                table.task_state(100, next_state);
                FAIL() << "completed task accepted a state update";
            } catch (std::logic_error const& error) {
                std::string message{error.what()};
                EXPECT_NE(std::string::npos, message.find("task_id=100"));
                EXPECT_NE(std::string::npos, message.find("task_kind=" + std::string{to_string_view(kind)}));
                EXPECT_NE(std::string::npos, message.find("next_state=" + std::string{to_string_view(next_state)}));
                EXPECT_NE(std::string::npos, message.find("step_state=running"));
                auto expected_slots = kind == model::task_kind::main ? "main_slots=1 pre_slots=0" : "main_slots=0 pre_slots=1";
                EXPECT_NE(std::string::npos, message.find(expected_slots));
            }
            EXPECT_TRUE(table.completed(kind));
        }
    }
}

}
