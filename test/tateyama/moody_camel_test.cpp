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
#include <concurrentqueue/concurrentqueue.h>

#include <regex>
#include <gtest/gtest.h>

#include <tateyama/context.h>

namespace tateyama::impl {

using namespace std::literals::string_literals;

class moody_camel_test : public ::testing::Test {

};

using namespace std::string_view_literals;

class test_task {
public:
    test_task() = default;

    explicit test_task(std::size_t id) : id_(id) {}

    void operator()(context& ctx) {
        (void)ctx;
    }
    std::size_t id_{};
};
TEST_F(moody_camel_test, basic) {
    ::moodycamel::ConcurrentQueue<test_task> q;
    q.enqueue(test_task{100});
    test_task item{};
    ASSERT_TRUE(q.try_dequeue(item));
    ASSERT_EQ(100, item.id_);
}

}
