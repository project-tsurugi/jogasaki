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

#include <cstring>
#include <iostream>
#include <string>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/test_root.h>

namespace jogasaki::data {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class aligned_buffer_test : public test_root {};

TEST_F(aligned_buffer_test, basic) {
    aligned_buffer buf{10};
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(10, buf.capacity());
    EXPECT_TRUE(buf);
    EXPECT_TRUE(buf.empty());
}

TEST_F(aligned_buffer_test, construct) {
    aligned_buffer buf{};
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(0, buf.size());
    EXPECT_EQ(0, buf.capacity());
    EXPECT_FALSE(buf);
    EXPECT_TRUE(buf.empty());
}

TEST_F(aligned_buffer_test, construct_from_sv) {
    aligned_buffer buf{"ABCDE"};
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(5, buf.size());
    EXPECT_EQ(5, buf.capacity());
    EXPECT_TRUE(buf);
    EXPECT_FALSE(buf.empty());
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDE");
}

TEST_F(aligned_buffer_test, compare) {
    aligned_buffer buf0{};
    EXPECT_EQ(buf0, buf0);
    aligned_buffer buf1{};
    EXPECT_NE(buf0, buf1);
    aligned_buffer buf2{10};
    aligned_buffer buf3{10};
    EXPECT_NE(buf2, buf3);
}

TEST_F(aligned_buffer_test, print) {
    aligned_buffer buf0{};
    std::cout << buf0 << std::endl;
    aligned_buffer buf1{10, 2};
    std::cout << buf1 << std::endl;
    aligned_buffer buf2{"ABCDE"};
    std::cout << buf2 << std::endl;
}

TEST_F(aligned_buffer_test, resize) {
    aligned_buffer buf{5, 1};
    auto p = buf.data();
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(5, buf.capacity());
    EXPECT_EQ(0, buf.size());
    EXPECT_TRUE(buf.empty());
    buf.resize(10);
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(10, buf.capacity());
    EXPECT_EQ(10, buf.size());
    EXPECT_FALSE(buf.empty());
    EXPECT_NE(p, buf.data());
}

TEST_F(aligned_buffer_test, alignment) {
    aligned_buffer buf{5, 16};
    auto p = buf.data();
    EXPECT_EQ(16, buf.alignment());
    // check data address is multiple of 16
    EXPECT_EQ(0, reinterpret_cast<std::size_t>(buf.data()) % 16U );
    buf.resize(10);
    EXPECT_EQ(0, reinterpret_cast<std::size_t>(buf.data()) % 16U );
}

TEST_F(aligned_buffer_test, copy) {
    aligned_buffer buf{5, 2};
    std::memcpy(buf.data(), "ABCDE", 5);
    buf.resize(5);
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDE");
    aligned_buffer copy{};
    copy.assign(buf);
    EXPECT_EQ(static_cast<std::string_view>(copy), "ABCDE");
    EXPECT_EQ(1, copy.alignment());  // assign doesn't change alignment
    EXPECT_EQ(5, copy.capacity());
}

TEST_F(aligned_buffer_test, assign_from_sv) {
    aligned_buffer buf{5, 2};
    std::memcpy(buf.data(), "ABCDE", 5);
    buf.resize(5);
    EXPECT_EQ(2, buf.alignment());
    EXPECT_EQ(5, buf.capacity());
    EXPECT_EQ(5, buf.size());
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDE");

    buf.assign("ABCDEF");
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDEF");
    EXPECT_EQ(2, buf.alignment());
    EXPECT_EQ(6, buf.capacity());
    EXPECT_EQ(6, buf.size());
}

TEST_F(aligned_buffer_test, shirink) {
    aligned_buffer buf{0, 2};
    buf.assign("ABCDE");
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDE");
    EXPECT_EQ(2, buf.alignment());
    EXPECT_EQ(5, buf.capacity());
    EXPECT_EQ(5, buf.size());
    buf.resize(3);
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABC");
    EXPECT_EQ(2, buf.alignment());
    EXPECT_EQ(5, buf.capacity());
    EXPECT_EQ(3, buf.size());
    buf.shrink_to_fit();
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABC");
    EXPECT_EQ(2, buf.alignment());
    EXPECT_EQ(3, buf.capacity());
    EXPECT_EQ(3, buf.size());
}
}

