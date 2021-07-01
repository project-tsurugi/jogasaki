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
#include <tateyama/task_scheduler.h>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <tateyama/basic_task.h>

namespace tateyama::api {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace testing;

class scheduler_test : public ::testing::Test {
public:
};

class test_task {
public:
    test_task() = default;

    explicit test_task(std::function<void(context&)> body) : body_(std::move(body)) {}
    void operator()(context& ctx) {
        return body_(ctx);
    }
    std::function<void(context&)> body_{};
};

TEST_F(scheduler_test, basic) {
    task_scheduler_cfg cfg{};
    cfg.thread_count(1);
    task_scheduler<test_task> sched{cfg};
    bool executed = false;
    test_task t{[&](context& t) {
        executed = true;
    }};
    sched.start();
    std::this_thread::sleep_for(1ms);
    sched.schedule(std::move(t));
    std::this_thread::sleep_for(1ms);
    sched.stop();
    ASSERT_TRUE(executed);
}

class test_task2 {
public:
    test_task2() = default;

    explicit test_task2(std::function<void(context&)> body) : body_(std::move(body)) {}
    void operator()(context& ctx) {
        return body_(ctx);
    }
    std::function<void(context&)> body_{};
};

TEST_F(scheduler_test, multiple_task_impls) {
    using task = basic_task<test_task, test_task2>;
    task_scheduler_cfg cfg{};
    cfg.thread_count(1);
    task_scheduler<task> sched{cfg};
    bool executed = false;
    bool executed2 = false;
    test_task t{[&](context& t) {
        executed = true;
    }};
    test_task2 t2{[&](context& t) {
        executed2 = true;
    }};
    sched.start();
    std::this_thread::sleep_for(1ms);
    sched.schedule(task{std::move(t)});
    sched.schedule(task{std::move(t2)});
    std::this_thread::sleep_for(1ms);
    sched.stop();
    ASSERT_TRUE(executed);
    ASSERT_TRUE(executed2);
}

}
