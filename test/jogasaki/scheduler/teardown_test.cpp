/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/task_scheduler.h>

#include "../api/api_test_base.h"

namespace jogasaki::scheduler {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using api::impl::get_impl;

class teardown_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(teardown_test, basic) {
    // verify teardown waits for on-going tasks and finally finishes the job correctly calling job callback
    auto& s = *get_impl(*db_).task_scheduler();

    std::atomic_bool executed = false;
    std::atomic_bool all_tasks_scheduled = false;
    std::atomic_bool teardown_task_submitted = false;
    std::atomic_size_t yield_count = 0;
    std::atomic_size_t completed_task_count = 0;
    s.start();
    auto rctx = api::impl::create_request_context(
        get_impl(*db_),
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {}
    );
    auto jctx = rctx->job();
    auto jobid = jctx->id();
    jctx->callback([&, rctx]() {
        std::cerr << "job callback called." << std::endl;
        executed = true;
    });
    static constexpr std::size_t num_tasks = 10000;
    for(std::size_t i=0; i < num_tasks; ++i) {
        s.schedule_task(
            create_custom_task(
                rctx.get(),
                [&, rctx] () {
                    if(! all_tasks_scheduled) {
                        ++yield_count;
                        return model::task_result::yield;
                    }
                    auto s = teardown_task_submitted.load();
                    if(!s && teardown_task_submitted.compare_exchange_strong(s, true)) {
                        if(check_or_submit_teardown(*rctx, true)) {
                            ++completed_task_count;
                            return model::task_result::complete_and_teardown;
                        }
                    }
                    ++completed_task_count;
                    return model::task_result::complete;
                },
                model::task_transaction_kind::none
            )
        );
    }
    all_tasks_scheduled = true;
    s.wait_for_progress(jobid);
    s.stop();
    EXPECT_TRUE(executed);
    EXPECT_EQ(num_tasks, completed_task_count);
    std::cerr << "yield_count:" << yield_count << std::endl;
}

}
