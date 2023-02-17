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
#include <jogasaki/utils/base_filename.h>

#include <gtest/gtest.h>

namespace jogasaki::utils {

class base_filename_test : public ::testing::Test {};

TEST_F(base_filename_test, simple) {
    std::stringstream ss{};
    ss << basename("/test1/test2/test3.cpp");
    EXPECT_EQ("test3.cpp:26", ss.str());
}

TEST_F(base_filename_test, test_file_name) {
    std::stringstream ss{};
    ss << base_filename();
    EXPECT_EQ("base_filename_test.cpp:32", ss.str());
}

TEST_F(base_filename_test, only_file_name) {
    std::stringstream ss{};
    ss << basename("test3.cpp");
    EXPECT_EQ("test3.cpp:38", ss.str());
}
}

