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

#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/parallel_task_scheduler.h>
#include <jogasaki/executor/common/task.h>

namespace jogasaki::testing {

using namespace scheduler;
using namespace executor;
using namespace model;
using namespace takatori::util;

class task_wrapper : public common::task {
public:
    task_wrapper() = default;
    task_wrapper(task_wrapper&& other) = delete;
    task_wrapper& operator=(task_wrapper&& other) = delete;
    task_wrapper(std::function<task_result()> original) : original_(std::move(original)) {}

    task_result operator()() override {
        return original_.operator()();
    }

private:
    std::function<task_result()> original_{};
};


class task_scheduler_test : public ::testing::Test {
public:
};

TEST_F(task_scheduler_test, single) {
    serial_task_scheduler executor{};
    bool run = false;
    auto t = std::make_shared<task_wrapper>([&]() {
        run = true;
        return task_result::complete;
    });
    job_context jctx{};
    executor.schedule_task(flat_task{t, &jctx});
    executor.wait_for_progress(jctx);
    ASSERT_TRUE(run);
}

TEST_F(task_scheduler_test, multi) {
    parallel_task_scheduler executor{thread_params(1)};
    std::atomic_flag run = false;
    job_context jctx{};
    auto t = std::make_shared<task_wrapper>([&]() {
        run.test_and_set() ;
        jctx.completion_latch().open();
        return task_result::complete;
    });
    executor.schedule_task(flat_task{t, &jctx});
    executor.wait_for_progress(jctx);
    executor.stop();
    ASSERT_TRUE(run.test_and_set());
}

}

