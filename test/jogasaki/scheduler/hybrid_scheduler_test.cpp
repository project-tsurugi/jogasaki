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
#include <jogasaki/scheduler/hybrid_task_scheduler.h>

#include <gtest/gtest.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/broadcast/step.h>
#include <jogasaki/executor/exchange/deliver/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/scheduler/dag_controller.h>

#include <jogasaki/mock/simple_scan_process.h>
#include <jogasaki/mock/simple_emit_process.h>
#include <jogasaki/mock/simple_transform_process.h>
#include <jogasaki/test_process.h>
#include <jogasaki/test_root.h>

namespace jogasaki::scheduler {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::scheduler;

class hybrid_scheduler_test : public test_root {};

class test_task : public common::task {
public:
    explicit test_task(std::function<void(void)> body) : body_(std::move(body)) {}

    model::task_result operator()() override {
        body_();
        return model::task_result::complete;
    }
private:
    std::function<void(void)> body_{};

};

bool wait(std::atomic_bool& b, std::size_t timeout_ms) {
    auto cnt = 0;
    while(! b) {
        std::this_thread::sleep_for(1ms);
        ++cnt;
        if(timeout_ms < cnt) {
            return false;
        }
    }
    return true;
}

TEST_F(hybrid_scheduler_test, basic) {
    hybrid_task_scheduler s{};
    ASSERT_EQ(task_scheduler_kind::hybrid, s.kind());

    std::atomic_bool executed = false;
    auto task = std::make_shared<test_task>([&]() {
        executed = true;
    });
    job_context jctx{};
    request_context rctx{};
    rctx.job(maybe_shared_ptr{&jctx});
    s.start();
    s.schedule_task(flat_task{task_enum_tag<scheduler::flat_task_kind::wrapped>, &rctx, task, false});
    ASSERT_TRUE(wait(executed, 1000));
    s.stop();
}

TEST_F(hybrid_scheduler_test, non_transactional_request_runs_serial_scheduler) {
    hybrid_task_scheduler s{};
    ASSERT_EQ(task_scheduler_kind::hybrid, s.kind());

    std::atomic_bool executed = false;
    auto task = std::make_shared<test_task>([&]() {
        executed = true;
    });
    job_context jctx{};
    request_context rctx{};
    rctx.lightweight(true);
    rctx.job(maybe_shared_ptr{&jctx});
    auto jobid = jctx.id();
    s.start();
    s.schedule_task(flat_task{task_enum_tag<scheduler::flat_task_kind::wrapped>, &rctx, task, false});
    ASSERT_TRUE(wait(executed, 1000));
    s.stop();
    ASSERT_TRUE(executed);
}

TEST_F(hybrid_scheduler_test, simple_request_runs_serial_scheduler) {
    hybrid_task_scheduler s{};
    ASSERT_EQ(task_scheduler_kind::hybrid, s.kind());

    std::atomic_bool executed = false;
    auto task = std::make_shared<test_task>([&]() {
        executed = true;
    });
    job_context jctx{};
    auto tx = std::make_shared<transaction_context>(nullptr);
    request_context rctx{{}, {}, {}, tx};
    rctx.lightweight(true);
    rctx.job(maybe_shared_ptr{&jctx});
    s.start();
    s.schedule_task(flat_task{task_enum_tag<scheduler::flat_task_kind::wrapped>, &rctx, task, false});
    ASSERT_TRUE(wait(executed, 1000));
    s.stop();
    ASSERT_TRUE(executed);
}

TEST_F(hybrid_scheduler_test, serial_scheduler_called_recursively) {
    // verify behavior when scheduler is called within task
    hybrid_task_scheduler s{};
    ASSERT_EQ(task_scheduler_kind::hybrid, s.kind());

    job_context jctx{};
    auto tx = std::make_shared<transaction_context>(nullptr);
    request_context rctx{{}, {}, {}, tx};
    rctx.lightweight(true);
    rctx.job(maybe_shared_ptr{&jctx});
    auto jobid = jctx.id();

    std::atomic_bool executed0 = false;
    std::atomic_bool executed1 = false;

    auto task0 = std::make_shared<test_task>([&]() {
        executed0 = true;

        auto task1 = std::make_shared<test_task>([&]() {
            executed1 = true;
        });
        s.schedule_task(flat_task{
            task_enum_tag<scheduler::flat_task_kind::wrapped>,
            &rctx,
            task1,
            false
        });
    });

    s.start();
    s.schedule_task(flat_task{task_enum_tag<scheduler::flat_task_kind::wrapped>, &rctx, task0, false});
    ASSERT_TRUE(wait(executed0, 1000));
    ASSERT_TRUE(wait(executed1, 1000));
    s.stop();
}
}
