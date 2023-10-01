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
#include <cstdlib>

#include <gtest/gtest.h>

namespace jogasaki::testing {

bool contains(std::string_view src, std::string_view element) {
    if(src.find(element) != std::string::npos) {
        return true;
    }
    return false;
}

class lsan_suppress_test : public ::testing::Test {};

TEST_F(lsan_suppress_test, simple) {
    // verify lsan suppressions option works in this environment
    auto v = std::getenv("LSAN_OPTIONS");
    if(! (v != nullptr && contains(v, "suppressions"))) {
        GTEST_SKIP() << "Test should run only when LSAN is configured to suppress leaks from the testcase.";
    }
    malloc(7);
}

}

