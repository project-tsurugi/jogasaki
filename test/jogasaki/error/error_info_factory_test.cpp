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

#include <jogasaki/error/error_info_factory.h>

#include <gtest/gtest.h>

#include <jogasaki/error/code.h>
#include <jogasaki/test_root.h>

namespace jogasaki::error {

using namespace testing;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

class error_info_factory_test : public test_root {};

TEST_F(error_info_factory_test, basic) {
    auto errinfo = create_error_info(code::none);
}

}

