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
#include <tateyama/api/endpoint/buffer.h>
#include "api_test_impls.h"

#include <regex>
#include <gtest/gtest.h>

namespace tateyama::api::endpoint {

using namespace std::literals::string_literals;

class api_buffer_test : public ::testing::Test {

};

using namespace std::string_view_literals;


TEST_F(api_buffer_test, basic) {
    std::array<char, 100> s{};
    test_buffer buf{s.data(), s.size()};
}

TEST_F(api_buffer_test, simple) {
    fixed_buffer<100> buf{};
}
}
