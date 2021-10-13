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
#include "../common/utils/loader.h"

#include <jogasaki/api.h>

#include <regex>
#include <gtest/gtest.h>

namespace tateyama::bootstrap {

using namespace std::literals::string_literals;

class loader_test : public ::testing::Test {

};

using namespace std::string_view_literals;

TEST_F(loader_test, DISABLED_simple) {
    auto env = tateyama::utils::create_environment();
    env->initialize();
    auto cfg = std::make_shared<jogasaki::configuration>();
    auto db = tateyama::utils::create_database(cfg.get());
    ASSERT_TRUE(db);
    db->start();
    db->stop();
}

}
