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
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include "mock_memory_resource.h"


namespace jogasaki {

using namespace std::string_literals;
using namespace std::string_view_literals;

class mock_memory_resource_test : public ::testing::Test {};

TEST_F(mock_memory_resource_test, bytes_limit) {
    mock_memory_resource resource{8, 0};

    resource.allocate(4, 1);
    EXPECT_EQ(1, resource.resources_.size());
    resource.allocate(4, 1);
    EXPECT_EQ(1, resource.resources_.size());
    resource.allocate(4, 1);
    EXPECT_EQ(2, resource.resources_.size());
    resource.allocate(4, 1);
    EXPECT_EQ(2, resource.resources_.size());
    resource.allocate(8, 1);
    EXPECT_EQ(3, resource.resources_.size());
}

TEST_F(mock_memory_resource_test, count_limit) {
    mock_memory_resource resource{0, 3};

    resource.allocate(8, 1);
    resource.allocate(8, 1);
    resource.allocate(8, 1);
    EXPECT_EQ(1, resource.resources_.size());
    resource.allocate(8, 1);
    EXPECT_EQ(2, resource.resources_.size());
}

TEST_F(mock_memory_resource_test, flush) {
    mock_memory_resource resource{0, 1};

    resource.allocate(8, 1);
    resource.allocate(8, 1);
    EXPECT_EQ(2, resource.resources_.size());
    resource.end_current_page();
    EXPECT_EQ(3, resource.resources_.size());
    resource.allocate(8, 1);
    EXPECT_EQ(3, resource.resources_.size());
    resource.allocate(8, 1);
    EXPECT_EQ(4, resource.resources_.size());
}
}

