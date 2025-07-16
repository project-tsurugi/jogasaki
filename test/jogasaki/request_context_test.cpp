/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <takatori/util/fail.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/model/port.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

namespace jogasaki {

using namespace scheduler;
using namespace executor;
using namespace model;
using namespace takatori::util;

class request_context_test : public ::testing::Test {};

TEST_F(request_context_test, basic) {
    // verify original error will not be overwritten
    request_context c{};
    ASSERT_EQ(status::ok, c.status_code());
    EXPECT_TRUE(c.status_code(status::ok, "msg"));
    EXPECT_TRUE(c.status_message().empty()); // status::ok cannot set msg
    EXPECT_TRUE(c.status_code(status::not_found, "msg"));
    EXPECT_EQ(status::not_found, c.status_code());
    EXPECT_EQ("msg", c.status_message());
    EXPECT_FALSE(c.status_code(status::err_not_found, "new msg"));
    EXPECT_EQ(status::not_found, c.status_code());
    EXPECT_EQ("msg", c.status_message());
}

TEST_F(request_context_test, filling_error_info) {
    // verify original error will not be overwritten
    request_context c{};
    c.error_info(create_error_info(error_code::unique_constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::unique_constraint_violation_exception, c.error_info()->code());
    c.error_info(create_error_info(error_code::constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::unique_constraint_violation_exception, c.error_info()->code());
}

TEST_F(request_context_test, overwriting_error_info) {
    // verify nullptr or error_code::none are overwritten
    request_context c{};
    ASSERT_FALSE(c.error_info());
    c.error_info(create_error_info(error_code::none, "", status::err_unknown));
    ASSERT_TRUE(c.error_info());
    ASSERT_EQ(error_code::none, c.error_info()->code());
    c.error_info(create_error_info(error_code::constraint_violation_exception, "", status::err_unknown));
    ASSERT_EQ(error_code::constraint_violation_exception, c.error_info()->code());
}

TEST_F(request_context_test, request_stats) {
    // verify stats
    request_context c{};
    EXPECT_FALSE(c.stats());
    c.enable_stats();
    EXPECT_TRUE(c.stats());
    c.stats()->counter(counter_kind::inserted).count(1);
    EXPECT_EQ(1, c.stats()->counter(counter_kind::inserted).count());
    c.stats()->counter(counter_kind::deleted).count(2);
    EXPECT_EQ(2, c.stats()->counter(counter_kind::deleted).count());
}
}

