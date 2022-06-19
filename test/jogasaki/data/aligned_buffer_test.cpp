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

#include <jogasaki/data/aligned_buffer.h>

#include <gtest/gtest.h>

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
    EXPECT_EQ(10, buf.size());
    EXPECT_TRUE(buf);
    EXPECT_FALSE(buf.empty());
}

TEST_F(aligned_buffer_test, construct) {
    aligned_buffer buf{};
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(0, buf.size());
    EXPECT_FALSE(buf);
    EXPECT_TRUE(buf.empty());
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
}

TEST_F(aligned_buffer_test, resize) {
    aligned_buffer buf{5, 1};
    auto p = buf.data();
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(5, buf.size());
    buf.resize(10);
    EXPECT_EQ(1, buf.alignment());
    EXPECT_EQ(10, buf.size());
    EXPECT_NE(p, buf.data());
}

TEST_F(aligned_buffer_test, alignment) {
    aligned_buffer buf{5, 16};
    auto p = buf.data();
    EXPECT_EQ(16, buf.alignment());
    EXPECT_EQ(0, reinterpret_cast<std::size_t>(buf.data()) % 16U );
    buf.resize(10);
    EXPECT_EQ(0, reinterpret_cast<std::size_t>(buf.data()) % 16U );
}

TEST_F(aligned_buffer_test, copy) {
    aligned_buffer buf{5, 2};
    std::memcpy(buf.data(), "ABCDE", 5);
    EXPECT_EQ(static_cast<std::string_view>(buf), "ABCDE");
    aligned_buffer copy{};
    copy.assign(buf);
    EXPECT_EQ(static_cast<std::string_view>(copy), "ABCDE");
    EXPECT_EQ(2, copy.alignment());
    EXPECT_EQ(5, copy.size());
}

}

