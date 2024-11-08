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

#include <initializer_list>
#include <iostream>
#include <string>
#include <vector>
#include <boost/container/container_fwd.hpp>
#include <gtest/gtest.h>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/test_root.h>

#include "../../third_party/nlohmann/json.hpp"

namespace jogasaki::error {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class error_info_factory_test : public test_root {};

TEST_F(error_info_factory_test, basic) {
    request_context rctx{};
    set_error(rctx, error_code::sql_service_exception, "msg", status::ok);
    auto errinfo = rctx.error_info();
    ASSERT_TRUE(errinfo);
    LOG(INFO) << *errinfo;
}

TEST_F(error_info_factory_test, stacktrace) {
    request_context rctx{};
    jogasaki::error::set_error_impl(rctx, error_code::sql_service_exception, "msg", __FILE__, line_number_string, status::ok, true);
    auto errinfo = rctx.error_info();
    ASSERT_TRUE(errinfo);
    LOG(INFO) << *errinfo;

    // manually check if stacktrace is readable with correct line breaks
    auto j = nlohmann::json::parse(errinfo->supplemental_text());
    std::cerr << j["stacktrace"].get<std::string>();
}
}

