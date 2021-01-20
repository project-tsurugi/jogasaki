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
#include <jogasaki/api/database.h>
#include <jogasaki/api/result_set.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

/**
 * @brief test database api
 */
class database_test : public ::testing::Test {

};

TEST_F(database_test, simple) {
    std::string sql = "select * from T0";
    auto db = api::create_database();
    db->start();
    ASSERT_TRUE(db->execute("INSERT INTO T0 (C0, C1) VALUES(1, 10.0)"));
    ASSERT_TRUE(db->execute("INSERT INTO T0 (C0, C1) VALUES(2, 20.0)"));
    std::unique_ptr<api::result_set> rs{};
    ASSERT_TRUE(db->execute(sql, rs));
    auto it = rs->iterator();
    std::size_t count = 0;
    while(it->has_next()) {
        std::stringstream ss{};
        auto* record = it->next();
        ss << *record;
        LOG(INFO) << ss.str();
        ++count;
    }
    EXPECT_EQ(2, count);
}

}
