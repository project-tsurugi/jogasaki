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
#include <jogasaki/utils/waitable_atomic.h>

#include <future>
#include <thread>

#include <gtest/gtest.h>

namespace jogasaki::utils {

using namespace std::chrono_literals;

class waitable_atomic_test : public ::testing::Test {};

TEST_F(waitable_atomic_test, simple) {
    waitable_atomic<int> ai{0};
    ai = 1;
    EXPECT_FALSE(ai.wait_for(10ms, 1));
}

TEST_F(waitable_atomic_test, store_load) {
    waitable_atomic<int> ai{};
    ai=3;
    ai.store(2);
    EXPECT_EQ(2, ai.load());
    EXPECT_EQ(2, static_cast<int>(ai));
    EXPECT_TRUE(ai.wait_for(10ms, 1));
}

TEST_F(waitable_atomic_test, simple_wait) {
    waitable_atomic<int> ai{0};
    ai.wait(1); // no wait
    auto f = std::async(std::launch::async, [&](){
        std::this_thread::sleep_for(1ms);
        ai = 1;
        ai.notify_one();
    });
    ai.wait(0);
    f.get();
}

TEST_F(waitable_atomic_test, wait_alternately) {
    waitable_atomic<int> ai{0};
    auto f = std::async(std::launch::async, [&](){
        std::this_thread::sleep_for(1ms);
        ai = 1;
        ai.notify_one();
        EXPECT_TRUE(ai.wait_for(2s, 1));
        ai = 1;
        ai.notify_one();
    });
    EXPECT_TRUE(ai.wait_for(2s, 0));
    ai=2;
    ai.notify_one();
    EXPECT_TRUE(ai.wait_for(2s, 2));
    ai=3;
    ai.notify_one();
    f.get();
}

}

