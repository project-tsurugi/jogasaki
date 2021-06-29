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
#include <tateyama/impl/queue.h>
#include <tateyama/impl/tbb_queue.h>
#include <tateyama/impl/mc_queue.h>

#include <regex>
#include <gtest/gtest.h>

namespace tateyama::impl {

using namespace std::literals::string_literals;

class queue_test : public ::testing::Test {

};

using namespace std::string_view_literals;

TEST_F(queue_test, tbb_queue) {
    tbb_queue<int> q{};
    q.push(1);
    int item{};
    ASSERT_TRUE(q.try_pop(item));
    ASSERT_EQ(1, item);
}

TEST_F(queue_test, mc_queue) {
    mc_queue<int> q{};
    q.push(1);
    int item{};
    ASSERT_TRUE(q.try_pop(item));
    ASSERT_EQ(1, item);
}

struct test_task {
    std::size_t count_;
    std::size_t count() { return count_; }
};

TEST_F(queue_test, basic) {
    basic_queue<test_task> q{};
    test_task tsk1{1};
    test_task tsk2{2};
    q.push(tsk1);
    EXPECT_EQ(1, q.size());
    q.push(tsk2);
    EXPECT_EQ(2, q.size());
    test_task popped{};
    ASSERT_TRUE(q.try_pop(popped));
    ASSERT_EQ(1, popped.count());
    EXPECT_EQ(1, q.size());
    ASSERT_TRUE(q.try_pop(popped));
    ASSERT_EQ(2, popped.count());
    EXPECT_EQ(0, q.size());
    ASSERT_TRUE(q.empty());
    ASSERT_FALSE(q.try_pop(popped));
}

TEST_F(queue_test, clear) {
    basic_queue<test_task> q{};
    test_task tsk{1};
    q.push(tsk);
    q.push(tsk);
    q.push(tsk);
    EXPECT_EQ(3, q.size());
    q.clear();
    EXPECT_EQ(0, q.size());
    ASSERT_TRUE(q.empty());
    test_task popped{};
    ASSERT_FALSE(q.try_pop(popped));
}

class mo_task {
public:
    mo_task() = default;
    ~mo_task() = default;
    mo_task(mo_task const& other) = delete;
    mo_task& operator=(mo_task const& other) = delete;
    mo_task(mo_task&& other) noexcept = default;
    mo_task& operator=(mo_task&& other) noexcept = default;

    explicit mo_task(std::size_t value) : value_(value) {}
    std::size_t value_{};
};

TEST_F(queue_test, move_only_type) {
    basic_queue<mo_task> q{};
    mo_task tsk{1};
    q.push(std::move(tsk));
    q.push(mo_task{2});
    EXPECT_EQ(2, q.size());

    mo_task popped{};
    ASSERT_TRUE(q.try_pop(popped));
    ASSERT_EQ(1, popped.value_);
    EXPECT_EQ(1, q.size());
    ASSERT_TRUE(q.try_pop(popped));
    ASSERT_EQ(2, popped.value_);
    EXPECT_EQ(0, q.size());
    ASSERT_TRUE(q.empty());
    ASSERT_FALSE(q.try_pop(popped));
}

}
