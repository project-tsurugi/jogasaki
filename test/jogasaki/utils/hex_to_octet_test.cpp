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
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/utils/hex_to_octet.h>

namespace jogasaki::utils {

class hex_to_octet_test : public ::testing::Test {};

TEST_F(hex_to_octet_test, simple) {
    std::string out{};
    ASSERT_TRUE(hex_to_octet("", out));
    EXPECT_TRUE(out.empty());
    ASSERT_TRUE(hex_to_octet("00", out));
    ASSERT_EQ(1, out.size());
    EXPECT_EQ('\0', out[0]);
    ASSERT_TRUE(hex_to_octet("0001", out));
    ASSERT_EQ(2, out.size());
    EXPECT_EQ('\0', out[0]);
    EXPECT_EQ('\1', out[1]);
}

TEST_F(hex_to_octet_test, all_lowercase_digits) {
    std::string in(2, '\0');
    std::string out{};
    for(std::size_t i=0; i < 16; ++i) {
        for(std::size_t j=0; j < 16; ++j) {
            in[0] = i < 10 ? ('0' + i) : ('a' + (i - 10));
            in[1] = j < 10 ? ('0' + j) : ('a' + (j - 10));
            ASSERT_TRUE(hex_to_octet(in, out));
            ASSERT_EQ(1, out.size());
            EXPECT_EQ(static_cast<char>(i*16+j), out[0]);
        }
    }
}

TEST_F(hex_to_octet_test, all_uppercase_digits) {
    std::string in(2, '\0');
    std::string out{};
    for(std::size_t i=0; i < 16; ++i) {
        for(std::size_t j=0; j < 16; ++j) {
            in[0] = i < 10 ? ('0' + i) : ('A' + (i - 10));
            in[1] = j < 10 ? ('0' + j) : ('A' + (j - 10));
            ASSERT_TRUE(hex_to_octet(in, out));
            ASSERT_EQ(1, out.size());
            EXPECT_EQ(static_cast<char>(i*16+j), out[0]);
        }
    }
}

}  // namespace jogasaki::utils
