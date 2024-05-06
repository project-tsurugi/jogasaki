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

#include <jogasaki/request_cancel_config.h>
#include <jogasaki/test_root.h>

namespace jogasaki::testing {

class request_cancel_config_test : public test_root {};

TEST_F(request_cancel_config_test, basic) {
    request_cancel_config c{};
    c.enable(request_cancel_kind::write);
    c.enable(request_cancel_kind::scan);
    c.enable(request_cancel_kind::find);
    std::stringstream ss{};
    ss << c;
    EXPECT_EQ("write,scan,find", ss.str());
}

}  // namespace jogasaki::testing
