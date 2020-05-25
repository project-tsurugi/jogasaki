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

#include <gtest/gtest.h>
#include <accessor/text.h>

#include <jogasaki/mock_memory_resource.h>

namespace jogasaki::testing {

using namespace std::string_view_literals;
using namespace accessor;
using namespace memory;

class text_test : public ::testing::Test {};

TEST_F(text_test, default_construct) {
    text t{};
    std::string_view sv{t};
    EXPECT_EQ(0, sv.size());
    EXPECT_EQ(""sv, sv);
    EXPECT_TRUE(t.is_short());
}

TEST_F(text_test, long_version) {
    mock_memory_resource resource;
    std::string s{"A234567890123456"};
    text t{&resource, s.data(), s.size()};
    std::string_view sv{t};
    EXPECT_EQ(16, sv.size());
    EXPECT_EQ(s, sv);
    EXPECT_FALSE(t.is_short());
    EXPECT_EQ(16, resource.total_);
}

TEST_F(text_test, short_version) {
    mock_memory_resource resource;
    std::string s{"A23456789012345"};
    text t{&resource, s.data(), s.size()};
    std::string_view sv{t};
    EXPECT_EQ(15, sv.size());
    EXPECT_EQ(s, sv);
    EXPECT_TRUE(t.is_short());
    EXPECT_EQ(0, resource.total_);
}

TEST_F(text_test, comparison) {
    mock_memory_resource resource;
    std::string s0{"A2345678901234"};
    std::string s1{"A23456789012345"};
    std::string s2{"A234567890123456"};
    std::string s3{"A2345678901234567"};
    text t0{&resource, s0.data(), s0.size()};
    text t1{&resource, s1.data(), s1.size()};
    text t2{&resource, s2.data(), s2.size()};
    text t3{&resource, s3.data(), s3.size()};

    EXPECT_LT(t0, t1);
    EXPECT_LT(t1, t2);
    EXPECT_LT(t2, t3);

    EXPECT_LE(t0, t1);
    EXPECT_LE(t1, t2);
    EXPECT_LE(t2, t3);

    EXPECT_GT(t1, t0);
    EXPECT_GT(t2, t1);
    EXPECT_GT(t3, t2);

    EXPECT_GE(t1, t0);
    EXPECT_GE(t2, t1);
    EXPECT_GE(t3, t2);

    EXPECT_EQ(t1, t1);
    EXPECT_EQ(t2, t2);
    EXPECT_NE(t1, t2);
    EXPECT_NE(t2, t1);

    EXPECT_LE(t1, t1);
    EXPECT_GE(t1, t1);
    EXPECT_LE(t2, t2);
    EXPECT_GE(t2, t2);
}

TEST_F(text_test, create_from_sv) {
    mock_memory_resource resource;
    text t0{&resource, "ABC"sv};
    std::string_view sv{t0};
    EXPECT_EQ(3, sv.size());
    EXPECT_EQ("ABC", sv);
    EXPECT_TRUE(t0.is_short());
}

}

