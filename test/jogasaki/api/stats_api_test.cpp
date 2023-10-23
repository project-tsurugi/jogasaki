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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "runner.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

// write related error handling testcases
class stats_api_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
    void execute_statement_with_stats(
        std::string_view query,
        std::shared_ptr<request_statistics>& stats
    ) {
        status result{};
        ASSERT_EQ("",
            builder()
                .text(query)
                .st(result)
                .stats(stats)
                .expect_error(false)
                .run()
                .report()
        );
    }
};

using namespace std::string_view_literals;

TEST_F(stats_api_test, insert) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement_with_stats("INSERT INTO T VALUES (1)", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(1, stats->counter(counter_kind::inserted).count());
    EXPECT_EQ(0, stats->counter(counter_kind::merged).count());
}

TEST_F(stats_api_test, insert_skip) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("INSERT IF NOT EXISTS INTO T VALUES (1)", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(0, stats->counter(counter_kind::inserted).count());
    EXPECT_EQ(0, stats->counter(counter_kind::merged).count());
}

TEST_F(stats_api_test, insert_replace) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("INSERT OR REPLACE INTO T VALUES (1)", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(0, stats->counter(counter_kind::inserted).count());
    EXPECT_EQ(1, stats->counter(counter_kind::merged).count());
}

TEST_F(stats_api_test, update) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("UPDATE T SET C0=2 WHERE C0=1", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(1, stats->counter(counter_kind::updated).count());
}

TEST_F(stats_api_test, update_multiple_rows) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_statement("INSERT INTO T VALUES (5)");
    execute_statement_with_stats("UPDATE T SET C0=C0+1", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(3, stats->counter(counter_kind::updated).count());
}

TEST_F(stats_api_test, delete) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_statement("INSERT INTO T VALUES (5)");
    execute_statement_with_stats("DELETE FROM T WHERE C0 > 1", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(2, stats->counter(counter_kind::deleted).count());
}

}
