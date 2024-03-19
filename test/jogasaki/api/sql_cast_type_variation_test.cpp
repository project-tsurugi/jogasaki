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

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/process/impl/expression/details/constants.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/storage_data.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_cast_type_variation_test :
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
};

using namespace std::string_view_literals;

// TODO add cases for BOOLEAN, TINYINT, SMALLINT after compiler support is ready

// from int4

TEST_F(sql_cast_type_variation_test, int4_to_int4) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_int8) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_float4) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_float8) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_decimal) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_char) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 5)},
           std::forward_as_tuple(accessor::text{"-123 "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int4_to_varchar) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 5)},
           std::forward_as_tuple(accessor::text{"-123"}))), result[0]);
    }
}

// from int8

TEST_F(sql_cast_type_variation_test, int8_to_int4) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_int8) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_float4) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_float8) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_decimal) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_char) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 5)},
           std::forward_as_tuple(accessor::text{"-123 "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, int8_to_varchar) {
    execute_statement("create table TT (C0 BIGINT primary key)");
    execute_statement("INSERT INTO TT VALUES (-123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 5)},
           std::forward_as_tuple(accessor::text{"-123"}))), result[0]);
    }
}

// from float4

TEST_F(sql_cast_type_variation_test, float4_to_int4) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_int8) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_float4) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_float8) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_decimal) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_char) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 15)},
           std::forward_as_tuple(accessor::text{"-123.000000    "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float4_to_varchar) {
    execute_statement("create table TT (C0 REAL primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 15)},
           std::forward_as_tuple(accessor::text{"-123.000000"}))), result[0]);
    }
}

// from float8

TEST_F(sql_cast_type_variation_test, float8_to_int4) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_int8) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_float4) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_float8) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_decimal) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_char) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 15)},
           std::forward_as_tuple(accessor::text{"-123.000000    "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, float8_to_varchar) {
    execute_statement("create table TT (C0 DOUBLE primary key)");
    execute_statement("INSERT INTO TT VALUES (-123.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 15)},
           std::forward_as_tuple(accessor::text{"-123.000000"}))), result[0]);
    }
}

// from decimal

TEST_F(sql_cast_type_variation_test, decimal_to_int4) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_int8) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_float4) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_float8) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_decimal) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_char) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 15)},
           std::forward_as_tuple(accessor::text{"-123.000       "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, decimal_to_varchar) {
    execute_statement("create table TT (C0 DECIMAL(6,3) primary key)");
    execute_statement("INSERT INTO TT VALUES (CAST(-123 AS DECIMAL(6,3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 15)},
           std::forward_as_tuple(accessor::text{"-123.000"}))), result[0]);
    }
}

// from char

TEST_F(sql_cast_type_variation_test, char_to_int4) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, char_to_int8) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, char_to_float4) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, char_to_float8) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, char_to_decimal) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, char_to_char) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 15)},
           std::forward_as_tuple(accessor::text{"-123           "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, char_to_varchar) {
    execute_statement("create table TT (C0 CHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 15)},
           std::forward_as_tuple(accessor::text{"-123      "}))), result[0]);
    }
}

// from varchar

TEST_F(sql_cast_type_variation_test, varchar_to_int4) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_int8) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS BIGINT) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>({-123}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_float4) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_float8) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({-123.0}, {false})), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_decimal) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS DECIMAL(6,3)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(6, 3)}, triple{-1, 0, 123, 0})),
            result[0]
        );
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_char) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS CHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 15)},
           std::forward_as_tuple(accessor::text{"-123           "}))), result[0]);
    }
}

TEST_F(sql_cast_type_variation_test, varchar_to_varchar) {
    execute_statement("create table TT (C0 VARCHAR(10) primary key)");
    execute_statement("INSERT INTO TT VALUES ('-123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(15)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, 15)},
           std::forward_as_tuple(accessor::text{"-123"}))), result[0]);
    }
}

}  // namespace jogasaki::testing
