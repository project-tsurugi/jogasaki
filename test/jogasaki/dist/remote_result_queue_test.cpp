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
#include <chrono>
#include <future>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/dist/remote_result_queue.h>

namespace jogasaki::testing {
using namespace std::chrono_literals;
TEST(remote_result_queue_test, waits_for_consumer_space_and_preserves_order) {
    dist::remote_result_queue queue{4, 2};
    ASSERT_TRUE(queue.push("1234"));
    auto producer = std::async(std::launch::async, [&] { return queue.push("56"); });
    EXPECT_EQ(std::future_status::timeout, producer.wait_for(20ms));
    auto first = queue.take();
    EXPECT_EQ("1234", first.front());
    auto ready = producer.wait_for(5s);
    if (ready != std::future_status::ready) queue.cancel();
    EXPECT_EQ(std::future_status::ready, ready);
    EXPECT_TRUE(producer.get());
    auto second = queue.take();
    ASSERT_EQ(1, second.size());
    EXPECT_EQ("56", second.front());
}
TEST(remote_result_queue_test, bounds_empty_messages_by_count_and_cancel_unblocks_writer) {
    dist::remote_result_queue queue{4, 1};
    ASSERT_TRUE(queue.push(""));
    auto producer = std::async(std::launch::async, [&] { return queue.push(""); });
    EXPECT_EQ(std::future_status::timeout, producer.wait_for(20ms));
    queue.cancel();
    EXPECT_EQ(std::future_status::ready, producer.wait_for(5s));
    EXPECT_FALSE(producer.get());
    EXPECT_FALSE(queue.push("x"));
}
TEST(remote_result_queue_test, cancel_race_never_loses_wakeup) {
    for (int i = 0; i < 100; ++i) {
        dist::remote_result_queue queue{1, 1};
        ASSERT_TRUE(queue.push("x"));
        auto producer = std::async(std::launch::async, [&] { return queue.push("y"); });
        queue.cancel();
        EXPECT_EQ(std::future_status::ready, producer.wait_for(5s));
        EXPECT_FALSE(producer.get());
    }
}
TEST(remote_result_queue_test, rejects_one_message_larger_than_capacity_without_waiting) {
    dist::remote_result_queue queue{4, 1};
    EXPECT_FALSE(queue.push("12345"));
    EXPECT_TRUE(queue.empty());
}

TEST(remote_result_queue_test, rejects_zero_capacity) {
    EXPECT_THROW((void)dist::remote_result_queue(0, 1), std::logic_error);
    EXPECT_THROW((void)dist::remote_result_queue(1, 0), std::logic_error);
}
TEST(remote_result_queue_test, cancel_preserves_queued_messages_but_prevents_more_pushes) {
    dist::remote_result_queue queue{6, 2};
    ASSERT_TRUE(queue.push("first")); ASSERT_TRUE(queue.push("x"));
    queue.cancel(); queue.cancel();
    EXPECT_FALSE(queue.push(""));
    EXPECT_EQ((std::deque<std::string>{"first", "x"}), queue.take());
    EXPECT_TRUE(queue.empty()); EXPECT_TRUE(queue.take().empty());
    EXPECT_FALSE(queue.push("again"));
}

} // namespace jogasaki::testing
