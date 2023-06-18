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
#include <jogasaki/kvs/id.h>
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

class sql_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        cfg->stealing_enabled(false);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sql_test, cross_join) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (4, 40.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (5, 50.0)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0, T10", result);
    ASSERT_EQ(6, result.size());
}

TEST_F(sql_test, update_by_part_of_primary_key) {
    execute_statement( "INSERT INTO T20 (C0, C2, C4) VALUES (1, 100.0, '111')");
    execute_statement( "UPDATE T20 SET C2=200.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T20", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_TRUE(rec.is_null(1));
    EXPECT_DOUBLE_EQ(200.0, rec.get_value<double>(2));
    EXPECT_FALSE(rec.is_null(2));
}

TEST_F(sql_test, update_primary_key) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "UPDATE T0 SET C0=3, C1=30.0 WHERE C1=10.0");
    wait_epochs(2);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(2, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    EXPECT_EQ(3, result[1].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, result[1].get_value<double>(1));
}

TEST_F(sql_test, count_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_empty_records_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, sum_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, sum_empty_records_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, count_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, sum_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, count_distinct) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_distinct_empty) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_distinct_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_rows) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, max) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(3, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, rec.get_value<double>(1));
}

TEST_F(sql_test, min) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C0), MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, rec.get_value<double>(1));
}

TEST_F(sql_test, count_rows_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_rows_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, avg_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT AVG(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, avg_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT AVG(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, max_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, max_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, min_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, min_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_test, aggregate_decimals) {
    execute_statement("CREATE TABLE TT(C0 DECIMAL(5,3) NOT NULL PRIMARY KEY)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal}
    };
    auto ps = api::create_parameter_set();
    auto v10 = decimal_v{1, 0, 10, 0}; // 10
    auto v20 = decimal_v{1, 0, 20, 0}; // 20
    ps->set_decimal("p0", v10);
    ps->set_decimal("p1", v20);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps);
    execute_statement("INSERT INTO TT (C0) VALUES (:p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MIN(C0), COUNT(C0), AVG(C0) FROM TT", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_FALSE(rec.is_null(2));
    EXPECT_FALSE(rec.is_null(3));
    auto v15 = decimal_v{1, 0, 15, 0}; // 15

    auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto i64 = meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::decimal, kind::decimal, kind::int8, kind::decimal
    >(
        std::tuple{
            dec, dec, i64, dec
        },
        {
            v20, v10, 2, v15
        }
    )), result[0]);
}

TEST_F(sql_test, decimals_indefinitive_precscale) {
    execute_statement("CREATE TABLE TT(C0 DECIMAL(5,3) NOT NULL PRIMARY KEY)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal}
    };
    auto ps = api::create_parameter_set();
    auto v1 = decimal_v{1, 0, 1, 0}; // 1
    ps->set_decimal("p0", v1);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0*C0 as C0 FROM TT", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));

    auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{dec}, {v1})), result[0]);
}

TEST_F(sql_test, update_delete_secondary_index) {
    execute_statement( "INSERT INTO TSECONDARY (C0, C1) VALUES (1, 100)");
    execute_statement( "INSERT INTO TSECONDARY (C0, C1) VALUES (2, 200)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TSECONDARY WHERE C1=200", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::int8, kind::int8>(2, 200)), result[0]);
    }
    execute_statement( "UPDATE TSECONDARY SET C1=300 WHERE C0=1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TSECONDARY WHERE C1=300", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::int8, kind::int8>(1, 300)), result[0]);
    }
    execute_statement( "UPDATE TSECONDARY SET C0=3 WHERE C0=1");
    wait_epochs(2);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TSECONDARY WHERE C1=300", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::int8, kind::int8>(3, 300)), result[0]);
    }
    execute_statement( "DELETE FROM TSECONDARY WHERE C1=300");
    wait_epochs();
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TSECONDARY WHERE C1=300", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement( "INSERT INTO TSECONDARY (C0, C1) VALUES (3, 300)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TSECONDARY WHERE C1=300", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::int8, kind::int8>(3, 300)), result[0]);
    }
}

TEST_F(sql_test, update_char_columns) {
    execute_statement( "INSERT INTO CHAR_TAB(C0, CH, VC) VALUES (0, '000', '000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CH, VC FROM CHAR_TAB", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::character, kind::character>(accessor::text{"000  "sv}, accessor::text{"000"sv})), result[0]);
    }
    execute_statement("UPDATE CHAR_TAB SET CH='11', VC='11' WHERE C0=0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CH, VC FROM CHAR_TAB", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_record<kind::character, kind::character>(accessor::text{"11   "sv}, accessor::text{"11"sv})), result[0]);
    }
}

TEST_F(sql_test, read_null) {
    execute_statement("INSERT INTO T0(C0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>({0, 0.0}, {false, true})), result[0]);
    }
}

TEST_F(sql_test, update_by_null) {
    execute_statement("INSERT INTO T0(C0, C1) VALUES (0, 0.0)");
    execute_statement("UPDATE T0 SET C1=NULL WHERE C0=0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>({0, 0.0}, {false, true})), result[0]);
    }
}

// join with on clause fails for now TODO
TEST_F(sql_test, DISABLED_join_condition_on_clause) {
    execute_statement( "CREATE TABLE TT0 (C0 INT NOT NULL, C1 INT NOT NULL, PRIMARY KEY(C0,C1))");
    execute_statement( "CREATE TABLE TT1 (C0 INT NOT NULL, C1 INT NOT NULL, PRIMARY KEY(C0,C1))");
    execute_statement( "INSERT INTO TT0 (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO TT1 (C0, C1) VALUES (10, 2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT0, TT1 WHERE TT0.C0=TT1.C0 AND TT0.C1 < TT1.C1", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT0 INNER JOIN TT1 ON TT0.C0=TT1.C0 WHERE TT0.C1 < TT1.C1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_test, cast) {
    execute_statement("create table TT (C0 int primary key, C1 bigint, C2 float, C3 double)");
    execute_statement("INSERT INTO TT (C0, C1, C2, C3) VALUES (CAST('1' AS INT), CAST('10' AS BIGINT), CAST('100.0' AS FLOAT), CAST('1000.0' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, C2, C3 FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8>({1, 10, 100.0, 1000.0}, {false, false, false, false})), result[0]);
    }
}

TEST_F(sql_test, cast_failure) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('BADVALUE' AS INT))", status::err_expression_evaluation_failure);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(0, result.size());
    }
}

// regression test scenario - once updating sequence stuck on 4th insert
TEST_F(sql_test, pkless_insert) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table TT (C0 int, C1 int)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(4, result.size());
    }
}

TEST_F(sql_test, insert_without_explicit_column) {
    std::unique_ptr<api::executable_statement> stmt0{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 VALUES (1, 20.0)", stmt0));
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->execute(*stmt0));
    ASSERT_EQ(status::ok, tx->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
}

TEST_F(sql_test, pkless_insert_without_explicit_column) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table TT (C0 int, C1 int)");
    execute_statement("INSERT INTO TT VALUES (2,20)");
    execute_statement("INSERT INTO TT VALUES (2,20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TT", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,20)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,20)), result[1]);
    }
}

// jogasaki should catch runtime exception from compiler
TEST_F(sql_test, DISABLED_subquery) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from (select * from TT t00, TT t01) t1", result);
        ASSERT_EQ(4, result.size());
    }
}

TEST_F(sql_test, select_distinct) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int, C2 int)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (1,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (2,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (3,1,2)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (4,1,NULL)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (5,1,NULL)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1 from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1, C2 from TT", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(sql_test, select_group_by_for_distinct) {
    // same as select_distinct using group by
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int, C2 int)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (1,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (2,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (3,1,2)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (4,1,NULL)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (5,1,NULL)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from TT group by C1", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1, C2 from TT group by C1, C2", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(sql_test, select_constant) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select 1 from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select true from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select false from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

// like expression not yet supported
TEST_F(sql_test, DISABLED_select_boolean_expression) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 VARCHAR(10))");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 'ABC')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select C1 like 'A%' from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

// like expression not yet supported
TEST_F(sql_test, DISABLED_like_expression) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 VARCHAR(10))");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 'ABC')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from TT where C1 like 'A%'", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from TT where NOT C1 like 'A%'", result);
        ASSERT_EQ(0, result.size());
    }
}

// current compiler doesn't read double literal correctly
TEST_F(sql_test, DISABLED_double_literal) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 VARCHAR(10))");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 'ABC')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select 1e2 from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

// currently we want to support sql up to 2GB, but failed with oom in syntax verification. Check after compiler is upgraded.
TEST_F(sql_test, DISABLED_long_sql) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    {
        std::string blanks(2*1024*1024*1024UL - 20UL, ' ');
        std::vector<mock::basic_record> result{};
        execute_query("select * " + blanks + "from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

// IS NULL / IS NOT NULL is not yet supported by compiler
TEST_F(sql_test, DISABLED_is_null) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (2, 20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 IS NULL ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 IS NOT NULL ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
    }
}

// shakujo based compiler doesn't return parse error with invalid toke TODO
TEST_F(sql_test, DISABLED_literal_with_invalid_char) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C0=$1", result);
        ASSERT_EQ(0, result.size());
    }
}

}
