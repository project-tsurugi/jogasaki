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
#include <iosfwd>
#include <string>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/test_root.h>

namespace jogasaki::testing {

class configuration_test : public test_root {};

TEST_F(configuration_test, print_default) {
    configuration c{};
    std::stringstream ss{};
    ss << c;
    EXPECT_EQ("", ss.str());
}

TEST_F(configuration_test, print_non_default_values) {
    configuration c{};
    c.single_thread(true);
    c.thread_pool_size(123);
    std::stringstream ss{};
    ss << c;
    EXPECT_EQ("single_thread:true thread_pool_size:123 ", ss.str());
}

TEST_F(configuration_test, print_request_cancel_cfg) {
    configuration c{};
    c.single_thread(true);
    c.thread_pool_size(123);
    auto req_cancel_cfg = std::make_shared<request_cancel_config>();
    req_cancel_cfg->enable(request_cancel_kind::write);
    req_cancel_cfg->enable(request_cancel_kind::scan);
    c.req_cancel_config(std::move(req_cancel_cfg));

    std::stringstream ss{};
    ss << c;
    EXPECT_EQ("single_thread:true thread_pool_size:123 req_cancel_config:write,scan ", ss.str());
}
}  // namespace jogasaki::testing
