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
#include <ostream>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/utils/iterator_incrementer.h>
#include <jogasaki/utils/iterator_pair.h>

namespace jogasaki::utils {

class iterator_incrementer_test : public ::testing::Test {
public:
    using iterator = std::string::iterator;
    using iterator_pair = utils::iterator_pair<iterator>;
    std::string check_result(std::vector<iterator_pair> const& iterators) {
        std::stringstream ss{};
        for(auto p : iterators) {
            if (p.first == p.second) {
                ss << "-";
            } else {
                ss << *p.first;
            }
        }
        return ss.str();
    }
};

TEST_F(iterator_incrementer_test, simple) {

    std::string c1("abc"), c2("x"), c3("12");
    std::vector<iterator_pair> pairs {
        {c1.begin(), c1.end()},
        {c2.begin(), c2.end()},
        {c3.begin(), c3.end()},
    };
    iterator_incrementer incr{pairs};

    EXPECT_EQ("ax1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("ax2", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("bx1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("bx2", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("cx1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("cx2", check_result(incr.current()));
    ASSERT_FALSE(incr.increment());
}

TEST_F(iterator_incrementer_test, empty_element) {

    std::string c1("abc"), c2(""), c3("1");
    std::vector<iterator_pair> pairs {
        {c1.begin(), c1.end()},
        {c2.begin(), c2.end()},
        {c3.begin(), c3.end()},
    };
    iterator_incrementer incr{pairs};

    EXPECT_EQ("a-1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("b-1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("c-1", check_result(incr.current()));
    ASSERT_FALSE(incr.increment());
}

TEST_F(iterator_incrementer_test, multiple_empty_elements) {

    std::string c1("abc"), c2(""), c3("");
    std::vector<iterator_pair> pairs {
        {c1.begin(), c1.end()},
        {c2.begin(), c2.end()},
        {c3.begin(), c3.end()},
    };
    iterator_incrementer incr{pairs};

    EXPECT_EQ("a--", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("b--", check_result(incr.current()));
    ASSERT_TRUE(incr.increment());
    EXPECT_EQ("c--", check_result(incr.current()));
    ASSERT_FALSE(incr.increment());
}

TEST_F(iterator_incrementer_test, increment_with_position) {
    std::string c1("abc"), c2("12");
    std::vector<iterator_pair> pairs {
        {c1.begin(), c1.end()},
        {c2.begin(), c2.end()},
    };
    iterator_incrementer incr{pairs};

    EXPECT_EQ("a1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment(1));
    EXPECT_EQ("a2", check_result(incr.current()));
    ASSERT_FALSE(incr.increment(1));
    EXPECT_EQ("a1", check_result(incr.current()));

    ASSERT_TRUE(incr.increment(0));
    EXPECT_EQ("b1", check_result(incr.current()));
    ASSERT_TRUE(incr.increment(0));
    EXPECT_EQ("c1", check_result(incr.current()));
    ASSERT_FALSE(incr.increment(0));
    EXPECT_EQ("a1", check_result(incr.current()));
}

}

