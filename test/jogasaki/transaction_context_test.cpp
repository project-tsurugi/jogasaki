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
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki {

class transaction_context_test : public ::testing::Test {};

TEST_F(transaction_context_test, basic) {
    details::worker_manager mgr{};
    EXPECT_EQ(details::worker_manager::empty_worker, mgr.worker_id());
    EXPECT_EQ(0, mgr.use_count());
    {
        std::uint32_t my_worker = 100;
        EXPECT_TRUE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        std::uint32_t my_worker = 200;
        EXPECT_FALSE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        std::uint32_t my_worker = 100;
        EXPECT_TRUE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(2, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        EXPECT_FALSE(mgr.decrement_and_clear_on_zero());
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
    }
    {
        EXPECT_TRUE(mgr.decrement_and_clear_on_zero());
        EXPECT_EQ(details::worker_manager::empty_worker, mgr.worker_id());
        EXPECT_EQ(0, mgr.use_count());
    }
}


TEST_F(transaction_context_test, filling_error_info) {
    // verify original error will not be overwritten
    transaction_context c{};
    c.error_info(create_error_info(error_code::unique_constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::unique_constraint_violation_exception, c.error_info()->code());
    c.error_info(create_error_info(error_code::constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::unique_constraint_violation_exception, c.error_info()->code());
}

TEST_F(transaction_context_test, overwriting_error_info) {
    // verify nullptr or error_code::none are overwritten
    transaction_context c{};
    ASSERT_FALSE(c.error_info());
    c.error_info(create_error_info(error_code::none, "", status::err_unknown));
    ASSERT_TRUE(c.error_info());
    ASSERT_EQ(error_code::none, c.error_info()->code());
    c.error_info(create_error_info(error_code::constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::constraint_violation_exception, c.error_info()->code());
}

TEST_F(transaction_context_test, termination_state) {
    {
        termination_state ts{};
        EXPECT_EQ(0, static_cast<std::uint64_t>(ts));
        EXPECT_EQ(0, ts.task_use_count());
        EXPECT_TRUE(ts.task_empty());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(! ts.going_to_commit());
    }
    {
        termination_state ts{};
        ts.task_use_count(1);
        EXPECT_EQ(1, ts.task_use_count());
        EXPECT_TRUE(! ts.task_empty());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(! ts.going_to_commit());
    }
    {
        termination_state ts{};
        auto max = (1UL << 62) - 1;
        ts.task_use_count(max);
        EXPECT_EQ(max, ts.task_use_count());
        EXPECT_TRUE(! ts.task_empty());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(! ts.going_to_commit());
    }
    {
        termination_state ts{};
        ts.set_going_to_abort();
        EXPECT_EQ(0, ts.task_use_count());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(! ts.going_to_commit());
    }
    {
        termination_state ts{};
        ts.set_going_to_commit();
        EXPECT_EQ(0, ts.task_use_count());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(ts.going_to_commit());
    }
    {
        termination_state ts{};
        auto max = (1UL << 62) - 1;
        ts.task_use_count(max);
        ts.set_going_to_commit();
        ts.set_going_to_abort();
        EXPECT_EQ(max, ts.task_use_count());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(ts.going_to_commit());
        ts.clear();
        EXPECT_EQ(0, ts.task_use_count());
        EXPECT_TRUE(! ts.going_to_abort());
        EXPECT_TRUE(! ts.going_to_commit());
    }
}
TEST_F(transaction_context_test, termination_manager) {
    {
        // check initial state
        details::termination_manager mgr{};
        EXPECT_EQ(0, mgr.state().task_use_count());
        EXPECT_TRUE(! mgr.state().going_to_abort());
        EXPECT_TRUE(! mgr.state().going_to_commit());
    }
    {
        // increment and decrement task_use_count
        details::termination_manager mgr{};
        termination_state ts{};
        EXPECT_TRUE(mgr.try_increment_task_use_count(ts));
        EXPECT_EQ(1, mgr.state().task_use_count());
        EXPECT_EQ(1, ts.task_use_count());
        EXPECT_TRUE(mgr.try_increment_task_use_count(ts));
        EXPECT_EQ(2, mgr.state().task_use_count());
        EXPECT_EQ(2, ts.task_use_count());
        mgr.decrement_task_use_count(ts);
        EXPECT_EQ(1, mgr.state().task_use_count());
        EXPECT_EQ(1, ts.task_use_count());
        mgr.decrement_task_use_count(ts);
        EXPECT_EQ(0, mgr.state().task_use_count());
        EXPECT_EQ(0, ts.task_use_count());
    }
    {
        // set going_to_abort is possible only once
        details::termination_manager mgr{};
        termination_state ts{};
        EXPECT_TRUE(mgr.try_set_going_to_abort(ts));
        EXPECT_TRUE(mgr.state().going_to_abort());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_TRUE(! mgr.try_set_going_to_abort(ts));
        EXPECT_TRUE(! mgr.try_set_going_to_commit(ts));
        EXPECT_TRUE(! mgr.try_increment_task_use_count(ts));
    }
    {
        // set going_to_commit is possible only once
        details::termination_manager mgr{};
        termination_state ts{};
        EXPECT_TRUE(mgr.try_set_going_to_commit(ts));
        EXPECT_TRUE(mgr.state().going_to_commit());
        EXPECT_TRUE(ts.going_to_commit());
        EXPECT_TRUE(! mgr.try_set_going_to_abort(ts));
        EXPECT_TRUE(! mgr.try_set_going_to_commit(ts));
        EXPECT_TRUE(! mgr.try_increment_task_use_count(ts));
    }
    {
        // set going_to_commit while task_use_count > 0
        details::termination_manager mgr{};
        termination_state ts{};
        EXPECT_TRUE(mgr.try_increment_task_use_count(ts));
        EXPECT_TRUE(mgr.try_set_going_to_commit(ts));
        EXPECT_TRUE(! mgr.state().going_to_commit());
        EXPECT_TRUE(! ts.going_to_commit());
        EXPECT_TRUE(mgr.state().going_to_abort());
        EXPECT_TRUE(ts.going_to_abort());
        EXPECT_EQ(1, ts.task_use_count());
        EXPECT_EQ(1, mgr.state().task_use_count());
    }
    {
        // decrement task count is possible even if flags are set
        details::termination_manager mgr{};
        termination_state ts{};
        EXPECT_TRUE(mgr.try_increment_task_use_count(ts));
        EXPECT_TRUE(mgr.try_increment_task_use_count(ts));
        EXPECT_EQ(2, ts.task_use_count());
        EXPECT_TRUE(mgr.try_set_going_to_abort(ts));
        EXPECT_TRUE(mgr.state().going_to_abort());
        EXPECT_TRUE(ts.going_to_abort());
        mgr.decrement_task_use_count(ts);
        EXPECT_EQ(1, ts.task_use_count());
        EXPECT_TRUE(! mgr.try_increment_task_use_count(ts));
        EXPECT_EQ(1, ts.task_use_count());
        mgr.decrement_task_use_count(ts);
        EXPECT_EQ(0, ts.task_use_count());
        EXPECT_EQ(0, mgr.state().task_use_count());
    }
}


}

