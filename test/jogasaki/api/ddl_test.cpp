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
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include <jogasaki/test_utils/secondary_index.h>
#include <jogasaki/kvs/id.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using kind = meta::field_type_kind;
using api::impl::get_impl;

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
    }

    void TearDown() override {
        db_teardown();
    }

    void test_prepare_err(std::string_view stmt, error_code expected) {
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{};
        std::shared_ptr<error::error_info> error{};
        auto st = get_impl(*db_).prepare(stmt, variables, prepared, error);
        ASSERT_TRUE(error);
        std::cerr << *error << std::endl;
        ASSERT_EQ(expected, error->code());
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
        auto exp = mock::typed_nullable_record<kind::int4, kind::int4, kind::int8, kind::float4, kind::float8, kind::character, kind::character>(
            std::tuple{int4_type(), int4_type(), int8_type(), float4_type(), float8_type(), character_type(false, 5), character_type(true, 6)},
            std::forward_as_tuple(1, 1, 10, 100.0, 1000.0, accessor::text("10000"), accessor::text("100000")),
            {false, false, false, false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, create_table_temporal_types) {
    execute_statement("CREATE TABLE T (C0 DATE NOT NULL PRIMARY KEY, C1 TIME, C2 TIME WITH TIME ZONE, C3 TIMESTAMP, C4 TIMESTAMP WITH TIME ZONE)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_of_day}, //TODO with time zone
        {"p3", api::field_type_kind::time_point},
        {"p4", api::field_type_kind::time_point}, //TODO with time zone
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto ps = api::create_parameter_set();
    ps->set_date("p0", d2000_1_1);
    ps->set_time_of_day("p1", t12_0_0);
    ps->set_time_of_day("p2", t12_0_0);
    ps->set_time_point("p3", tp2000_1_1_12_0_0);
    ps->set_time_point("p4", tp2000_1_1_12_0_0);
    execute_statement( "INSERT INTO T (C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
        auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        auto todtz = meta::field_type{std::make_shared<meta::time_of_day_field_option>(true)};
        auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
        EXPECT_EQ((mock::typed_nullable_record<
            kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point
        >(
            std::tuple{
                dat, tod, todtz, tp, tptz,
            },
            {
                d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
            }
        )), result[0]);
    }
}

TEST_F(ddl_test, create_table_decimals) {
    execute_statement("CREATE TABLE T (C0 DECIMAL(3, 0) NOT NULL PRIMARY KEY, C1 DECIMAL(5, 3), C2 DECIMAL(10,1))");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal},
        {"p2", api::field_type_kind::decimal},
    };
    auto ps = api::create_parameter_set();
    auto v111 = decimal_v{1, 0, 111, 0}; // 111
    auto v11_111 = decimal_v{1, 0, 11111, -3}; // 11.111
    auto v11111_1 = decimal_v{1, 0, 111111, -1}; // 11111.1

    ps->set_decimal("p0", v111);
    ps->set_decimal("p1", v11_111);
    ps->set_decimal("p2", v11111_1);
    execute_statement( "INSERT INTO T (C0, C1, C2) VALUES (:p0, :p1, :p2)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T", result);
    ASSERT_EQ(1, result.size());

    auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
    auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::decimal, kind::decimal, kind::decimal
    >(
        std::tuple{
            dec_3_0, dec_5_3, dec_10_1,
        },
        {
            v111, v11_111, v11111_1,
        }
    )), result[0]);
}

TEST_F(ddl_test, create_table_varieties_types_non_nullable) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT NOT NULL, C2 BIGINT NOT NULL, C3 FLOAT NOT NULL, C4 DOUBLE NOT NULL, C5 CHAR(5) NOT NULL, C6 VARCHAR(6) NOT NULL)");
    execute_statement( "INSERT INTO T (C0, C1, C2, C3, C4, C5, C6) VALUES(1, 1, 10, 100.0, 1000.0, '10000', '100000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::typed_nullable_record<kind::int4, kind::int4, kind::int8, kind::float4, kind::float8, kind::character, kind::character>(
            std::tuple{int4_type(), int4_type(), int8_type(), float4_type(), float8_type(), character_type(false, 5), character_type(true, 6)},
            std::forward_as_tuple(1, 1, 10, 100.0, 1000.0, accessor::text("10000"), accessor::text("100000")),
            {false, false, false, false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, existing_table) {
    execute_statement("CREATE TABLE T (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    test_prepare_err("CREATE TABLE T (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)", error_code::compile_exception);
}

TEST_F(ddl_test, duplicate_table_name) {
    // test compile time error and runtime error with existing table
    api::statement_handle prepared0{}, prepared1{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    ASSERT_EQ(status::ok,db_->prepare("CREATE TABLE TTT (C0 INT PRIMARY KEY)", variables, prepared0));
    ASSERT_EQ(status::ok,db_->prepare("CREATE TABLE TTT (C0 INT PRIMARY KEY)", variables, prepared1));
    execute_statement( "CREATE TABLE TTT (C0 INT PRIMARY KEY)");
    test_prepare_err("CREATE TABLE TTT (C0 INT PRIMARY KEY)", error_code::compile_exception);
    test_stmt_err(prepared1, error_code::target_already_exists_exception);
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared0));
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared1));
}

TEST_F(ddl_test, drop_missing_table) {
    test_prepare_err("DROP TABLE DUMMY111", error_code::symbol_analyze_exception);
}

TEST_F(ddl_test, drop_missing_table_runtime) {
    // test runtime error with missing table
    execute_statement("CREATE TABLE TTT (C0 INT PRIMARY KEY)");
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    ASSERT_EQ(status::ok,db_->prepare("DROP TABLE TTT", variables, prepared));
    execute_statement("DROP TABLE TTT");
    test_stmt_err(prepared, error_code::target_not_found_exception);
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
}

TEST_F(ddl_test, complex_primary_key) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, C3 INT, PRIMARY KEY(C0,C1))");
    execute_statement( "INSERT INTO T (C0, C1, C2, C3) VALUES(1, 1, 10, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
            std::forward_as_tuple(1, 1, 10, 10),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, primary_key_column_only) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL, PRIMARY KEY(C0))");
    execute_statement( "INSERT INTO T (C0) VALUES(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::create_nullable_record<kind::int4>(
            std::forward_as_tuple(1),
            {false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}
TEST_F(ddl_test, primary_key_columns_only) {
    execute_statement( "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, PRIMARY KEY(C0,C1))");
    execute_statement( "INSERT INTO T (C0, C1) VALUES(1, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto exp = mock::create_nullable_record<kind::int4, kind::int4>(
            std::forward_as_tuple(1, 10),
            {false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(ddl_test, without_primary_key) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    execute_statement("CREATE TABLE T (C0 BIGINT NOT NULL, C1 DOUBLE)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1, 1.0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(3, 3.0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2, 2.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(1, 1.0), {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(2, 2.0), {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(3, 3.0), {false, false})), result[2]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0,C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(1, 1.0), {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(2, 2.0), {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(3, 3.0), {false, false})), result[2]);
    }
}

TEST_F(ddl_test, dml_pkless) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    execute_statement("CREATE TABLE T (C0 BIGINT, C1 DOUBLE)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1, 1.0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2, 2.0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(3, 3.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(1, 1.0), {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(2, 2.0), {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(3, 3.0), {false, false})), result[2]);
    }
    execute_statement("DELETE FROM T");
    wait_epochs(2);
    execute_statement("INSERT INTO T (C0) VALUES(2)");
    execute_statement("INSERT INTO T (C0) VALUES(3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(2, 0.0), {false, true})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(3, 0.0), {false, true})), result[1]);
    }
    execute_statement("DELETE FROM T WHERE C0=2");
    wait_epochs(2);
    execute_statement("INSERT INTO T (C1) VALUES(1.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(0, 1.0), {true, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(3, 0.0), {false, true})), result[1]);
    }
    execute_statement("UPDATE T SET C0=5, C1=6.0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(5, 6.0), {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>( std::forward_as_tuple(5, 6.0), {false, false})), result[1]);
    }
}

TEST_F(ddl_test, drop_pkless) {
    // verify dropping pkless tables has no problem. (hopefully generated primary key value can be checked in the future)
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T (C0) VALUES(0)");
    execute_statement("INSERT INTO T (C0) VALUES(1)");
    execute_statement("INSERT INTO T (C0) VALUES(2)");
    execute_statement("DROP TABLE T");
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T (C0) VALUES(1)");
    execute_statement("INSERT INTO T (C0) VALUES(2)");
    execute_statement("INSERT INTO T (C0) VALUES(3)");
}

TEST_F(ddl_test, type_name_variants) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE DBLPREC (C0 DOUBLE PRECISION PRIMARY KEY)", variables, prepared));
}

TEST_F(ddl_test, unsupported_types) {
    test_prepare_err("CREATE TABLE T (C0 BOOLEAN PRIMARY KEY)", error_code::syntax_exception);
    test_prepare_err("CREATE TABLE T (C0 TINYINT PRIMARY KEY)", error_code::syntax_exception);
    test_prepare_err("CREATE TABLE T (C0 SMALLINT PRIMARY KEY)", error_code::syntax_exception);
    test_prepare_err("CREATE TABLE T (C0 BINARY VARYING(4) PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_test, decimal_args) {
    // verify decimal args variation doesn't hit compiler error
    // this is to test compiler behavior, run-time behavior will be tested separate cases below
    // TODO re-org. similar cases covered in ddl_metadata_test.cpp.
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT0 (C0 DECIMAL PRIMARY KEY)", variables, prepared));
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT1 (C0 DECIMAL(*,*) PRIMARY KEY)", variables, prepared));
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT2 (C0 DECIMAL(*,3) PRIMARY KEY)", variables, prepared));
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT3 (C0 DECIMAL(3,*) PRIMARY KEY)", variables, prepared));
}

TEST_F(ddl_test, decimal_with_no_arg) {
    // verify decimal without arg will be DECIMAL(38,0)
    execute_statement("CREATE TABLE T (C0 DECIMAL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES(CAST('1' AS DECIMAL))");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type(38, 0)},
        std::forward_as_tuple(decimal_v{1, 0, 1, 0}))), result[0]);
}

TEST_F(ddl_test, decimal_with_aster_aster) {
    // verify DDL with decimal(*, *) will be an error
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(*,*) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_test, decimal_with_aster_prec) {
    // verify decimal(*, 3) will be DECIMAL(38,0)
    execute_statement("CREATE TABLE T (C0 DECIMAL(*, 3) PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES(CAST('1' AS DECIMAL(*,3)))");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type(38, 3)},
        std::forward_as_tuple(decimal_v{1, 0, 1, 0}))), result[0]);
}

TEST_F(ddl_test, decimal_with_aster_scale) {
    // verify DDL with decimal(3, *) will be an error
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(3,*) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_test, string_args) {
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{};
    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT0 (C0 CHAR PRIMARY KEY)", variables, prepared));

    test_prepare_err("CREATE TABLE TT1 (C0 CHAR(*) PRIMARY KEY)", error_code::syntax_exception);
    test_prepare_err("CREATE TABLE TT2 (C0 VARCHAR PRIMARY KEY)", error_code::syntax_exception);
//    test_prepare_err("CREATE TABLE TT2 (C0 VARCHAR(0) PRIMARY KEY)", error_code::syntax_exception);  // varchar(0) should be error  //TODO

    EXPECT_EQ(status::ok, db_->prepare("CREATE TABLE TT3 (C0 VARCHAR(*) PRIMARY KEY)", variables, prepared));
}

TEST_F(ddl_test, default_value) {
    // NOT NULL clause and DEFAULT clause cannot be specified at the same time
    // TODO check with new compiler
    test_prepare_err("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT NOT NULL DEFAULT 100)", error_code::syntax_exception);
}

TEST_F(ddl_test, negative_default_value) {
    // DEFAULT clause with negative integer cannot be specified
    // TODO check with new compiler
    test_prepare_err("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT DEFAULT -100)", error_code::syntax_exception);
}

TEST_F(ddl_test, drop_indices_cascade) {
    execute_statement("CREATE TABLE T (C0 INT, C1 INT)");
    auto stg0 = utils::create_secondary_index(*db_impl(), "S0", "T", {1}, {});
    ASSERT_TRUE(stg0);
    auto stg1 = utils::create_secondary_index(*db_impl(), "S1", "T", {1}, {});
    ASSERT_TRUE(stg1);
    execute_statement("DROP TABLE T");
    {
        auto provider = db_impl()->tables();
        auto s0 = provider->find_index("S0");
        ASSERT_FALSE(s0);
        ASSERT_FALSE(db_impl()->kvs_db()->get_storage("S0"));
        auto s1 = provider->find_index("S0");
        ASSERT_FALSE(s1);
        ASSERT_FALSE(db_impl()->kvs_db()->get_storage("S1"));
    }
}

TEST_F(ddl_test, long_char_data) {
    std::size_t len = 30*1024 / 2 - 4; // 4 for nullity bits
    std::string strlen = std::to_string(len);
    std::string c0(len, '0');
    std::string c1(len, '1');
    std::string c2(len, '2');
    std::string c3(len, '3');
    execute_statement("CREATE TABLE T (C0 CHAR("+strlen+") NOT NULL,  C1 VARCHAR("+strlen+") NOT NULL, C2 CHAR("+strlen+"), C3 VARCHAR("+strlen+"), PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO T (C0, C1, C2, C3) VALUES('"+c0+"','"+c1+"', '"+c2+"', '"+c3+"')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character, kind::character, kind::character>(
            std::tuple{character_type(false, len), character_type(true, len), character_type(false, len), character_type(true, len)},
            std::forward_as_tuple(accessor::text{c0}, accessor::text{c1}, accessor::text{c2}, accessor::text{c3}), {false, false, false, false})), result[0]);
    }
}

TEST_F(ddl_test, max_key_len) {
    std::size_t len = 30*1024 - 4; // 4 for nullity bits
    std::string strlen = std::to_string(len);
    std::string c0(len, '0');
    std::string c1(len, '1');
    execute_statement("CREATE TABLE T (C0 VARCHAR("+strlen+") NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T (C0) VALUES('"+c0+"')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C0='"+c0+"'", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
            std::tuple{character_type(true, len)},
            std::forward_as_tuple(accessor::text{c0}), {false})), result[0]);
    }
    execute_statement("UPDATE T SET C0='"+c1+"' WHERE C0='"+c0+"'");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C0='"+c1+"'", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
            std::tuple{character_type(true, len)},
            std::forward_as_tuple(accessor::text{c1}), {false})), result[0]);
    }
    execute_statement("DELETE FROM T WHERE C0='"+c1+"'");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(ddl_test, insert_exceeding_max_key_len) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has no limit";
    }
    std::size_t maxlen = 30*1024 - 4; // 4 for nullity bits
    std::string strlen = std::to_string(maxlen);
    std::string c0(maxlen+1, '0');
    execute_statement("CREATE TABLE T (C0 VARCHAR("+strlen+") NOT NULL PRIMARY KEY)");
    test_stmt_err("INSERT INTO T (C0) VALUES('"+c0+"')", error_code::value_too_long_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(ddl_test, insert_exceeding_max_key_len_on_secondary) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has no limit";
    }
    // insert is successful on primary, but fails on secondary
    std::size_t len = 30*1024 - 4; // 4 for nullity bits
    std::string strlen = std::to_string(len);
    std::string c0(len, '0');
    execute_statement("CREATE TABLE T (C0 VARCHAR("+strlen+") NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T (C1)");
    test_stmt_err("INSERT INTO T (C0, C1) VALUES('"+c0+"', 1)", error_code::value_too_long_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(ddl_test, query_exceeding_max_key_len) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has no limit";
    }
    std::size_t len = 30*1024 - 4; // 4 for nullity bits
    std::string strlen = std::to_string(len);
    std::string c0(len+1, '0');
    std::string c1(len+1, '1');
    std::string dt(len, '0');
    execute_statement("CREATE TABLE T (C0 VARCHAR("+strlen+") NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T (C0) VALUES('"+dt+"')");
    std::vector<mock::basic_record> result{};
    test_stmt_err("SELECT * FROM T WHERE C0='"+c0+"'", error_code::value_too_long_exception);
    test_stmt_err("SELECT * FROM T WHERE C0>'"+c0+"' AND C0<'"+c1+"'", error_code::value_too_long_exception);
    test_stmt_err("UPDATE T SET C0='"+c1+"' WHERE C0='"+c0+"'", error_code::value_too_long_exception);
    test_stmt_err("DELETE FROM T WHERE C0='"+c1+"'", error_code::value_too_long_exception);
}

TEST_F(ddl_test, update_exceeding_max_key_len_on_secondary) {
    // update is successful on primary, but fails on secondary
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has no limit";
    }
    std::size_t len = 30*1024 - 4 - 10; // 4 for nullity bits, 10 for approx. C1 length (but not enough considering terminator)
    std::string strlen = std::to_string(len);
    std::string c0(len, '0');
    std::string c1(len, '1');
    std::string dt(len, '0');
    execute_statement("CREATE TABLE T (C0 VARCHAR("+strlen+") NOT NULL PRIMARY KEY, C1 VARCHAR(10))");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES('"+c0+"', 'x')");
    test_stmt_err("UPDATE T SET C1='xxxxxxx' WHERE C1='x'", error_code::value_too_long_exception);
}
}
