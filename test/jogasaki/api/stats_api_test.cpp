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

#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/runner.h>

#include "api_test_base.h"

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
    void execute_query_with_stats(
        std::string_view query,
        std::shared_ptr<request_statistics>& stats
    ) {
        status result{};
        std::vector<mock::basic_record> recs{};
        ASSERT_EQ("",
            builder()
                .text(query)
                .st(result)
                .stats(stats)
                .expect_error(false)
                .output_records(recs)
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
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, insert_skip) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("INSERT IF NOT EXISTS INTO T VALUES (1)", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(0, stats->counter(counter_kind::inserted).count());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, insert_replace) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("INSERT OR REPLACE INTO T VALUES (1)", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_EQ(1, stats->counter(counter_kind::merged).count());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, insert_replace_with_secondary) {
    // test code path for INSERT OR REPLACE with a secondary index
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T(C1)");
    execute_statement_with_stats("INSERT OR REPLACE INTO T VALUES (1,10)", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_EQ(1, stats->counter(counter_kind::merged).count());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());

    execute_statement_with_stats("INSERT OR REPLACE INTO T VALUES (1,10)", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_EQ(1, stats->counter(counter_kind::merged).count());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, update) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("UPDATE T SET C0=2 WHERE C0=1", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_EQ(1, stats->counter(counter_kind::updated).count());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, update_wo_change) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement_with_stats("UPDATE T SET C0=2 WHERE C0=10", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_EQ(0, stats->counter(counter_kind::updated).count());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, update_multiple_rows) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory causes problem updating multiple rows";
    }
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_statement("INSERT INTO T VALUES (5)");
    execute_statement_with_stats("UPDATE T SET C0=C0+1", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_EQ(3, stats->counter(counter_kind::updated).count());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, delete) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_statement("INSERT INTO T VALUES (5)");
    execute_statement_with_stats("DELETE FROM T WHERE C0 > 1", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_EQ(2, stats->counter(counter_kind::deleted).count());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, delete_wo_change) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement_with_stats("DELETE FROM T WHERE C0 = 10", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_EQ(0, stats->counter(counter_kind::deleted).count());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}

TEST_F(stats_api_test, fetched) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_query_with_stats("select * from T", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_EQ(2, stats->counter(counter_kind::fetched).count());
}

TEST_F(stats_api_test, fetched_multi_partitions) {
    // verify fetched count when emit runs on multiple partitions
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("INSERT INTO T VALUES (2)");
    execute_statement("INSERT INTO T VALUES (3)");
    execute_statement("INSERT INTO T VALUES (4)");
    execute_statement("INSERT INTO T VALUES (5)");
    execute_query_with_stats("select DISTINCT C0 from T", stats);
    ASSERT_TRUE(stats);
    EXPECT_TRUE(! stats->counter(counter_kind::inserted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_EQ(5, stats->counter(counter_kind::fetched).count());
}

TEST_F(stats_api_test, insert_from_select) {
    std::shared_ptr<request_statistics> stats{};
    execute_statement("CREATE TABLE T0(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T0 VALUES (1)");
    execute_statement("INSERT INTO T0 VALUES (2)");
    execute_statement("CREATE TABLE T1(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement_with_stats("insert into T1 select * from T0", stats);
    ASSERT_TRUE(stats);
    EXPECT_EQ(2, stats->counter(counter_kind::inserted).count());
    EXPECT_TRUE(! stats->counter(counter_kind::updated).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::merged).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::deleted).has_value());
    EXPECT_TRUE(! stats->counter(counter_kind::fetched).has_value());
}


}
