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
#include <cstring>
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/memory/paged_memory_resource.h>
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
    EXPECT_EQ(0, t.size());
    EXPECT_EQ(""sv, sv);
    EXPECT_TRUE(t.is_short());
    EXPECT_TRUE(t.empty());
    EXPECT_FALSE(t);
}

TEST_F(text_test, long_version) {
    mock_memory_resource resource;
    std::string s{"A234567890123456"};
    text t{&resource, s.data(), s.size()};
    std::string_view sv{t};
    EXPECT_EQ(16, sv.size());
    EXPECT_EQ(16, t.size());
    EXPECT_EQ(s, sv);
    EXPECT_FALSE(t.is_short());
    EXPECT_FALSE(t.empty());
    EXPECT_TRUE(t);
    EXPECT_EQ(16, resource.total_bytes_allocated_);
}

TEST_F(text_test, short_version) {
    mock_memory_resource resource;
    std::string s{"A23456789012345"};
    text t{&resource, s.data(), s.size()};
    std::string_view sv{t};
    EXPECT_EQ(15, sv.size());
    EXPECT_EQ(15, t.size());
    EXPECT_EQ(s, sv);
    EXPECT_TRUE(t.is_short());
    EXPECT_FALSE(t.empty());
    EXPECT_TRUE(t);
    EXPECT_EQ(0, resource.total_bytes_allocated_);
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

TEST_F(text_test, print_content) {
    mock_memory_resource resource;
    text t0{&resource, "ABC"sv};
    text t1{&resource, "D23456789012345678901234567890"sv};
    std::stringstream ss;
    ss << t0;
    EXPECT_EQ("ABC", ss.str());
    ss << t1;
    EXPECT_EQ("ABCD23456789012345678901234567890", ss.str());

    text empty{};
    std::stringstream ss2;
    ss2 << empty;
    EXPECT_EQ("<empty>", ss2.str());
}

TEST_F(text_test, compare_default_constructed) {
    mock_memory_resource resource;
    text t0{&resource, "ABC"sv};
    std::string_view sv{t0};

    text e{};
    EXPECT_NE(e, t0);
    text z{&resource, ""sv};
    EXPECT_EQ(e, z);
    EXPECT_LE(e, t0);
    EXPECT_LT(e, t0);
    EXPECT_GE(t0, e);
    EXPECT_GT(t0, e);
}

TEST_F(text_test, concat) {
    mock_memory_resource resource;
    text t0{&resource, "A23"sv};
    text t1{&resource, "B23456789012345678901234567890"sv};
    EXPECT_EQ(3, t0.size());
    EXPECT_EQ(30, t1.size());
    std::string_view sv{t0};
    text t0t0{&resource, t0, t0};
    EXPECT_TRUE(t0t0.is_short());
    EXPECT_EQ("A23A23"sv, static_cast<std::string_view>(t0t0));
    EXPECT_EQ(6, t0t0.size());

    text t0t1{&resource, t0, t1};
    EXPECT_FALSE(t0t1.is_short());
    EXPECT_EQ("A23B23456789012345678901234567890"sv, static_cast<std::string_view>(t0t1));
    EXPECT_EQ(33, t0t1.size());

    text t1t0{&resource, t1, t0};
    EXPECT_FALSE(t1t0.is_short());
    EXPECT_EQ("B23456789012345678901234567890A23"sv, static_cast<std::string_view>(t1t0));
    EXPECT_EQ(33, t1t0.size());

    text t1t1{&resource, t1, t1};
    EXPECT_FALSE(t1t1.is_short());
    EXPECT_EQ("B23456789012345678901234567890B23456789012345678901234567890"sv, static_cast<std::string_view>(t1t1));
    EXPECT_EQ(60, t1t1.size());
}

TEST_F(text_test, construct_from_buffer) {
    mock_memory_resource resource;
    auto buf = resource.allocate(30, 1);
    auto src = "123456789012345678901234567890"sv;
    std::memcpy(buf, src.data(), 30);
    text t0{static_cast<char*>(buf), 3};
    text t1{static_cast<char*>(buf), 30};

    text e0{&resource, "123"sv};
    EXPECT_EQ(e0, t0);
    EXPECT_TRUE(t0.is_short());
    text e1{&resource, src};
    EXPECT_EQ(e1, t1);
    EXPECT_FALSE(t1.is_short());
}

TEST_F(text_test, literal) {
    text t0{"123"};
    EXPECT_EQ(3, t0.size());
    EXPECT_TRUE(t0.is_short());
    text t1{"123456789012345678901234567890"};
    EXPECT_FALSE(t1.is_short());
    mock_memory_resource resource;
    text t2{&resource, t1};
    ASSERT_EQ(t2, t1);
}

TEST_F(text_test, cast) {
    text t0{"123"};
    EXPECT_EQ(3, t0.size());
    EXPECT_TRUE(t0.is_short());
    text t1{"123456789012345678901234567890"};
    EXPECT_FALSE(t1.is_short());
    mock_memory_resource resource;

    EXPECT_EQ("123", static_cast<std::string_view>(t0));
    EXPECT_EQ("123456789012345678901234567890", static_cast<std::string_view>(t1));

    auto f0 = [=]() { return t0; };
    auto f1 = [=]() { return t1; };

//    EXPECT_EQ("123", static_cast<std::string_view>(f0())); // casting to string_view is allowed only for lvalue
    EXPECT_EQ("123", static_cast<std::string>(f0())); // casting to string_view is allowed only for lvalue
    EXPECT_EQ("123456789012345678901234567890", static_cast<std::string>(f1())); // casting to string_view is allowed only for lvalue

}
}

