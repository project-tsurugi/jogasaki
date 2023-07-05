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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
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

class secondary_index_ddl_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

TEST_F(secondary_index_ddl_test, basic) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1=10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,10)), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM T WHERE C1=10", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

TEST_F(secondary_index_ddl_test, pkless_table) {
    execute_statement("CREATE TABLE T (C0 INT, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1=10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,10)), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM T WHERE C1=10", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

TEST_F(secondary_index_ddl_test, descending) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1 DESC)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1=10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,10)), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM T WHERE C1=10", plan);
        // verify column C1 is defined as key in descending order
        EXPECT_TRUE(contains(plan,
            R"({"column":"C1","direction":"descendant"})"
        ));
    }
}

TEST_F(secondary_index_ddl_test, drop) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("DROP INDEX I");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1=10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,10)), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM T WHERE C1=10", plan);
        // verify 1. scan op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

}
