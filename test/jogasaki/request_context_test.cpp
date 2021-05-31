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
#include <jogasaki/request_context.h>

#include <gtest/gtest.h>

#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/parallel_task_scheduler.h>
#include <jogasaki/executor/common/task.h>

namespace jogasaki {

using namespace scheduler;
using namespace executor;
using namespace model;
using namespace takatori::util;

class request_context_test : public ::testing::Test {};

TEST_F(request_context_test, basic) {
    request_context c{};
    ASSERT_EQ(status::ok, c.status_code());
    c.status_code(status::not_found);
    ASSERT_EQ(status::not_found, c.status_code());
    c.status_code(status::err_not_found);
    ASSERT_EQ(status::err_not_found, c.status_code());
    c.status_code(status::err_aborted_retryable);
    ASSERT_EQ(status::err_not_found, c.status_code());
}

}

