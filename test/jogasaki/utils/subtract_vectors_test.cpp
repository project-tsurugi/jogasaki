/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/utils/subtract_vectors.h>

#include <iostream>
#include <string>
#include <gtest/gtest.h>

namespace jogasaki::utils {

class subtract_vectors_test : public ::testing::Test {};

TEST_F(subtract_vectors_test, basic) {
    std::vector<int> x{1, 2, 3, 4, 5};
    std::vector<int> y{2, 3, 4};
    EXPECT_EQ((std::vector<int>{1, 5}), utils::subtract_vectors(x, y));
}

TEST_F(subtract_vectors_test, diff) {
    std::vector<int> x{1, 2, 3, 4, 5};
    std::vector<int> y{6, 5, 4};
    EXPECT_EQ((std::vector<int>{1, 2, 3}), utils::subtract_vectors(x, y));
}

TEST_F(subtract_vectors_test, no_intersection) {
    std::vector<int> x{1, 2, 3};
    std::vector<int> y{6, 5, 4};
    EXPECT_EQ((std::vector<int>{1, 2, 3}), utils::subtract_vectors(x, y));
}

TEST_F(subtract_vectors_test, multisets) {
    std::vector<int> x{1, 2, 2, 3, 3, 3};
    std::vector<int> y{1, 2, 3};
    EXPECT_EQ((std::vector<int>{2, 3, 3}), utils::subtract_vectors(x, y));
}

}  // namespace jogasaki::utils
