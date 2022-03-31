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
#include <jogasaki/executor/process/impl/expression/any.h>

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

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class ddl_test:
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
        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(ddl_test, simple_create_table) {
    execute_statement( "CREATE TABLE T (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    execute_statement( "INSERT INTO T (C0, C1) VALUES(1,1.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
    }
}

TEST_F(ddl_test, simple_drop_table) {
    execute_statement("CREATE TABLE T (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE, C2 INT)");
    execute_statement("INSERT INTO T (C0, C1, C2) VALUES(1,1.0,1)");
    execute_statement("DROP TABLE T");
    execute_statement("CREATE TABLE T (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,1.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,1.0)), result[0]);
    }
}

TEST_F(ddl_test, simple_create_table_int) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement( "INSERT INTO T (C0, C1) VALUES(1,1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,1)), result[0]);
    }
}

TEST_F(ddl_test, create_table_varieties_types) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 BIGINT, C3 FLOAT, C4 DOUBLE, C5 CHAR(5), C6 VARCHAR(6))");
    execute_statement( "INSERT INTO T (C0, C1, C2, C3, C4, C5, C6) VALUES(1, 1, 10, 100.0, 1000.0, '10000', '100000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::create_nullable_record<kind::int4, kind::int4, kind::int8, kind::float4, kind::float8, kind::character, kind::character>(
            std::forward_as_tuple(1, 1, 10, 100.0, 1000.0, accessor::text("10000"), accessor::text("100000")),
            {false, false, false, false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, create_table_varieties_types_non_nullable) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT NOT NULL, C2 BIGINT NOT NULL, C3 FLOAT NOT NULL, C4 DOUBLE NOT NULL, C5 CHAR(5) NOT NULL, C6 VARCHAR(6) NOT NULL)");
    execute_statement( "INSERT INTO T (C0, C1, C2, C3, C4, C5, C6) VALUES(1, 1, 10, 100.0, 1000.0, '10000', '100000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::create_nullable_record<kind::int4, kind::int4, kind::int8, kind::float4, kind::float8, kind::character, kind::character>(
            std::forward_as_tuple(1, 1, 10, 100.0, 1000.0, accessor::text("10000"), accessor::text("100000")),
            {false, false, false, false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, existing_table) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    ASSERT_EQ(status::err_translator_error,db_->prepare("CREATE TABLE T0 (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)", variables, prepared));
}

TEST_F(ddl_test, drop_missing_table) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    ASSERT_EQ(status::err_translator_error,db_->prepare("DROP TABLE DUMMY111", variables, prepared));
}
}
