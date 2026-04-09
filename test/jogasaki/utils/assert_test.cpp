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
#include <iostream>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/utils/assert.h>


namespace jogasaki::utils {

class assert_test : public ::testing::Test {};

TEST_F(assert_test, no_args) {
    bool caught = false;
    try {
        assert_with_exception(false);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, no_args_pass) {
    // should not throw when condition is true
    assert_with_exception(true);
}

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
        std::string msg{e.what()};
        std::cerr << msg << std::endl;
        EXPECT_NE(std::string::npos, msg.find("condition 'x+1 != 1' failed"));
        EXPECT_NE(std::string::npos, msg.find("x:0"));
        EXPECT_NE(std::string::npos, msg.find("x+1:1"));
        EXPECT_NE(std::string::npos, msg.find("x+2:2"));
        EXPECT_NE(std::string::npos, msg.find("x+3:3"));
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
    std::vector<std::string> v{utils_details_stringify(x1, x2, x3)};
    ASSERT_EQ(exp, v);
}

TEST_F(assert_test, verify_stringify_max) {
    std::vector<std::string> exp{"x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9"};
    std::vector<std::string> v{utils_details_stringify(x1, x2, x3, x4, x5, x6, x7, x8, x9)};
    ASSERT_EQ(exp, v);
}

TEST_F(assert_test, ptr_pass) {
    // assert_with_exception(ptr) where ptr is non-null: should not throw
    int value = 42;
    int* ptr = &value;
    assert_with_exception(ptr);
}

TEST_F(assert_test, ptr_fail) {
    // assert_with_exception(ptr) where ptr is null: should throw
    int* ptr = nullptr;
    bool caught = false;
    try {
        assert_with_exception(ptr);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(assert_test, not_ptr_pass) {
    // assert_with_exception(! ptr) where ptr is null: should not throw
    int* ptr = nullptr;
    assert_with_exception(! ptr);
}

TEST_F(assert_test, not_ptr_fail) {
    // assert_with_exception(! ptr) where ptr is non-null: should throw
    int value = 42;
    int* ptr = &value;
    bool caught = false;
    try {
        assert_with_exception(! ptr);
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

}  // namespace jogasaki::utils
