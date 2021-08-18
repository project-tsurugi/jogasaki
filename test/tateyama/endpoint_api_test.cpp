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
#include <tateyama/api/endpoint/writer.h>
#include "endpoint_impls.h"

#include <regex>
#include <gtest/gtest.h>

namespace tateyama::api::endpoint {

using namespace std::literals::string_literals;

class endpoint_api_test : public ::testing::Test {

};

using namespace std::string_view_literals;

TEST_F(endpoint_api_test, writer) {
    std::array<char, 100> s{};
    test_writer wrt{s.data(), s.size()};
}

TEST_F(endpoint_api_test, fixed_buffer_writer) {
    fixed_buffer_writer<100> buf{};
}

TEST_F(endpoint_api_test, request) {
    test_request req{"ABC"};
    test_response res{};

    // do service

    auto s = req.payload();
    EXPECT_EQ("ABC", s);



    res.complete();

}
}
