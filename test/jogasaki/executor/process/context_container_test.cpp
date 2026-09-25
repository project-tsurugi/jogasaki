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
#include <memory>

#include <gtest/gtest.h>

#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>

namespace jogasaki::executor::process::impl::ops {

class context_container_test : public ::testing::Test {};

class test_context : public context_base {
public:
    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::unknown;
    }

    void release() override {}
};

TEST_F(context_container_test, try_emplace) {
    context_container container{1};
    auto context = std::make_unique<test_context>();
    auto* expected = context.get();

    auto [actual, inserted] = container.try_emplace(0, std::move(context));

    EXPECT_TRUE(inserted);
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(expected, container.at(0));
}

TEST_F(context_container_test, keep_first_assignment) {
    context_container container{1};
    auto context = std::make_unique<test_context>();
    auto* expected = context.get();
    (void) container.try_emplace(0, std::move(context));
    std::unique_ptr<context_base> replacement = std::make_unique<test_context>();

    auto [actual, inserted] = container.try_emplace(0, std::move(replacement));

    EXPECT_FALSE(inserted);
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(expected, container.at(0));
    EXPECT_NE(nullptr, replacement);
}

}  // namespace jogasaki::executor::process::impl::ops
