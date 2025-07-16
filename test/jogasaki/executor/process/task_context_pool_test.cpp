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
#include <string>
#include <string_view>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor::process::impl {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace testing;
using namespace jogasaki::memory;
using namespace boost::container::pmr;

class task_context_pool_test : public test_root {};

using kind = meta::field_type_kind;

TEST_F(task_context_pool_test, basic) {
    auto context1 = std::make_shared<mock::task_context>();
    auto context2 = std::make_shared<mock::task_context>();
    auto context3 = std::make_shared<mock::task_context>();

    task_context_pool pool{};
    pool.push(context1);
    pool.push(context2);
    pool.push(context3);

    auto pop1 = pool.pop();
    auto pop2 = pool.pop();
    auto pop3 = pool.pop();
    EXPECT_EQ(*pop1, *context1);
    EXPECT_EQ(*pop2, *context2);
    EXPECT_EQ(*pop3, *context3);
}

}

