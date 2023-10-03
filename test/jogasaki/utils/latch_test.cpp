/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/utils/latch.h>

#include <future>
#include <thread>

#include <gtest/gtest.h>

namespace jogasaki::utils {

using namespace std::chrono_literals;

class latch_test : public ::testing::Test {};

TEST_F(latch_test, simple) {
    latch l{};
    std::atomic_bool opener_end = false;
    auto f = std::async(std::launch::async, [&](){
        std::this_thread::sleep_for(10ms);
        l.release();
        opener_end = true;
    });
    l.wait();
    f.get();
    EXPECT_TRUE(opener_end);
    EXPECT_TRUE(l.wait(1ms));
}

TEST_F(latch_test, wait_time_out) {
    latch l{};
    EXPECT_FALSE(l.wait(1ms));
    l.release();
    EXPECT_TRUE(l.wait(1ms));
}

TEST_F(latch_test, already_opened) {
    latch l{};
    l.release();
    EXPECT_TRUE(l.wait(1ms));
    l.wait();
}

TEST_F(latch_test, construct_released) {
    latch l{true};
    EXPECT_TRUE(l.wait(1ms));
    l.wait();
}

TEST_F(latch_test, reset) {
    latch l{};
    EXPECT_FALSE(l.wait(1ms));
    l.release();
    EXPECT_TRUE(l.wait(1ms));
    l.reset();
    EXPECT_FALSE(l.wait(1ms));
    l.release();
    EXPECT_TRUE(l.wait(1ms));
}
}

