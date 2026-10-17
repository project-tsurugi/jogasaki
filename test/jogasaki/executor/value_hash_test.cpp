/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/executor/equal_to.h>
#include <jogasaki/executor/value_hash.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor {

class value_hash_test : public test_root {};

template <class T>
void expect_same_hash(T x, T y) {
    ASSERT_TRUE(equal_to(x, y));
    EXPECT_EQ(value_hash<T>{}(x), value_hash<T>{}(y));
}

TEST_F(value_hash_test, decimal_different_representations) {
    using triple = takatori::decimal::triple;
    expect_same_hash(triple{1, 0, 1, 0}, triple{1, 0, 10, -1});
    expect_same_hash(triple{1, 0, 1, 0}, triple{1, 0, 100, -2});
    expect_same_hash(triple{0, 0, 0, 0}, triple{0, 0, 0, -3});
}

}  // namespace jogasaki::executor
