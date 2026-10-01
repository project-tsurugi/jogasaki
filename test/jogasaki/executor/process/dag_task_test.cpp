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
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler.h>

namespace jogasaki::executor::process {

class yielding_executor : public abstract::process_executor {
public:
    status run() override {
        return status::to_yield;
    }
};

class capturing_scheduler : public scheduler::serial_task_scheduler {
public:
    void do_schedule_task(scheduler::flat_task&& task, scheduler::schedule_option) override {
        tasks.emplace_back(std::move(task));
    }

    std::vector<scheduler::flat_task> tasks{};
};

class dag_task_test : public ::testing::TestWithParam<bool> {
public:
    void SetUp() override {
        previous_inplace_ = global::config_pool()->inplace_dag_schedule();
        global::config_pool()->inplace_dag_schedule(GetParam());
    }

    void TearDown() override {
        global::config_pool()->inplace_dag_schedule(previous_inplace_);
    }

private:
    bool previous_inplace_{};
};

TEST_P(dag_task_test, yield_schedules_events_without_completing_task) {
    capturing_scheduler scheduler{};
    scheduler::statement_scheduler statement_scheduler{std::make_shared<configuration>(), scheduler};
    scheduler::job_context job{};
    request_context context{};
    context.scheduler(takatori::util::maybe_shared_ptr{std::addressof(scheduler)});
    context.stmt_scheduler(takatori::util::maybe_shared_ptr{std::addressof(statement_scheduler)});
    context.job(takatori::util::maybe_shared_ptr{std::addressof(job)});
    exchange::forward::step step{};
    task yielding_task{
        std::addressof(context), std::addressof(step), std::make_shared<yielding_executor>(), nullptr,
        model::task_transaction_kind::none
    };

    // No DAG task is registered: sending a completion event here would be invalid.
    // Yield must allow the same task to run again and schedule DAG events only.
    for (std::size_t invocation = 1; invocation <= 2; ++invocation) {
        EXPECT_EQ(model::task_result::yield, yielding_task());
        auto expected_tasks = GetParam() ? 0 : invocation;
        ASSERT_EQ(expected_tasks, scheduler.tasks.size());
        EXPECT_EQ(expected_tasks, job.task_count().load());
        for (auto const& scheduled : scheduler.tasks) {
            EXPECT_EQ(scheduler::flat_task_kind::dag_events, scheduled.kind());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(scheduling_modes, dag_task_test, ::testing::Bool());

}
