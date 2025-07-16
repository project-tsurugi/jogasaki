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

#include <jogasaki/utils/fail.h>

namespace jogasaki::utils {

class fail_test : public ::testing::Test {};

TEST_F(fail_test, basic) {
    bool caught = false;
    try {
        fail_with_exception();
    } catch(std::logic_error const& e) {
        caught = true;
        std::cerr << e.what() << std::endl;
    }
    ASSERT_TRUE(caught);
}

TEST_F(fail_test, no_throw) {
    ASSERT_NO_THROW({
        fail_no_exception();
    });
}

TEST_F(fail_test, throw_with_msg) {
    ASSERT_THROW({
        fail_with_exception_msg("test");
    }, std::logic_error);
}

TEST_F(fail_test, no_throw_with_msg) {
    ASSERT_NO_THROW({
        fail_no_exception_msg("test");
    });
}

}  // namespace jogasaki::utils
