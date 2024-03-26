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
#include <iostream>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/utils/assert.h>


namespace jogasaki::utils {

class assert_test : public ::testing::Test {};

TEST_F(assert_test, basic) {
    int x = 0;
    bool caught = false;
    try {
        assert_with_exception(x+1 != 1, x);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, multi_args) {
    int x = 0;
    bool caught = false;
    try {
        assert_with_exception(x+1 != 1, x, x+1, x+2, x+3);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, complex_condition) {
    int x = 0;
    bool caught = false;
    try {
        assert_with_exception(((x+1 != 1 && x-1 != 0) || (1+1 == 3)), x, x+1, x+2, x+3);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, max_args) {
    // verify maximum number of args
    int x = 0;
    bool caught = false;
    try {
        assert_with_exception(false, x, x, x, x, x, x, x, x, x);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, verify_stringify) {
    std::vector<std::string> exp{"x1", "x2", "x3"};
    std::vector<std::string> v{stringify_va_args(x1, x2, x3)};
    ASSERT_EQ(exp, v);
}

TEST_F(assert_test, verify_stringify_max) {
    std::vector<std::string> exp{"x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"};
    std::vector<std::string> v{stringify_va_args(x1, x2, x3, x4, x5, x6, x7, x8, x9)};
    ASSERT_EQ(exp, v);
}

}  // namespace jogasaki::utils
