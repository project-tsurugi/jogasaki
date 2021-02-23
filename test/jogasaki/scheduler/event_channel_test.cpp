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
#include <jogasaki/event_channel.h>

#include <gtest/gtest.h>
#include <jogasaki/event.h>

#include <jogasaki/test_root.h>

namespace jogasaki::scheduler {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

class event_channel_test : public test_root {};

TEST_F(event_channel_test, basic) {
    event_channel ch{};
    ch.emplace(event_enum_tag<event_kind::task_completed>, 10UL, 10UL);
    ch.emplace(event_enum_tag<event_kind::task_completed>, 20UL, 20UL);
    event e3{event_enum_tag<event_kind::task_completed>, 30UL, 30UL};
    ch.push(e3);
    event e;
    ASSERT_TRUE(ch.pop(e));
    EXPECT_EQ(10, e.task());
    ASSERT_TRUE(ch.pop(e));
    EXPECT_EQ(20, e.task());
    ASSERT_TRUE(ch.pop(e));
    EXPECT_EQ(30, e.task());
    ASSERT_FALSE(ch.pop(e));
}

TEST_F(event_channel_test, blocking_queue) {
    basic_channel<blocking_queue_type> ch{};
    event e;
    ch.emplace(event_enum_tag<event_kind::task_completed>, 10UL, 10UL);
    ASSERT_TRUE(ch.pop(e));
}

TEST_F(event_channel_test, non_blocking_queue) {
    basic_channel<non_blocking_queue_type> ch{};
    event e;
    ch.emplace(event_enum_tag<event_kind::task_completed>, 10UL, 10UL);
    ASSERT_TRUE(ch.pop(e));
}

}
