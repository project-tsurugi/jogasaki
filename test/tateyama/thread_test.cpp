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
#include <tateyama/impl/thread_control.h>

#include <regex>
#include <gtest/gtest.h>

#include <thread>

namespace tateyama::impl {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;

class thread_test : public ::testing::Test {

};

using namespace std::string_view_literals;

TEST_F(thread_test, create_thread) {
    {
        int x = 0;
        thread_control t{[&](){ ++x; }};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(void)> f = [&](){ ++x; };
        thread_control t{f};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(void)> f = [&](){ ++x; };
        thread_control t{std::move(f)};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
}

TEST_F(thread_test, active) {
    bool active = false;
    thread_control t{[&](){
        active = t.active();
    }};
    EXPECT_FALSE(t.active());
    t.activate();
    std::this_thread::sleep_for(100us);
    EXPECT_FALSE(t.active());
    t.join();
    EXPECT_TRUE(active);
    EXPECT_FALSE(t.active());
}

TEST_F(thread_test, task_with_args) {
    {
        int x = 0;
        thread_control t{[&](int y){ ++x; }, 1};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(int)> f = [&](int y){ x += y; };
        thread_control t{f, 1};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(int)> f = [&](int y){ x += y; };
        thread_control t{std::move(f), 1};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
}

TEST_F(thread_test, vector_of_threads) {
    std::vector<thread_control> threads{};
    int x=0;
    threads.emplace_back([&](){ ++x; });
    auto& t = threads[0];
    t.activate();
    t.join();
    EXPECT_EQ(1, x);
}

TEST_F(thread_test, modifying_thread_input) {
    {
        int x = 0;
        thread_control t{[](int& x){ ++x; }, x};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(int& x)> f = [](int &x){ ++x; };
        thread_control t{f, x};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
    {
        int x = 0;
        std::function<void(int& x)> f = [](int &x){ ++x; };
        thread_control t{std::move(f), x};
        t.activate();
        t.join();
        EXPECT_EQ(1, x);
    }
}

}
