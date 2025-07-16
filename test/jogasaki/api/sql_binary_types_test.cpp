/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_binary_types_test :
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

using jogasaki::accessor::text;

TEST_F(sql_binary_types_test, insert_select) {
    execute_statement("CREATE TABLE T (C0 VARBINARY(3), C1 BINARY(3))");
    execute_statement("INSERT INTO T VALUES (CAST('00' AS VARBINARY(3)), CAST('00' AS BINARY(3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
            std::tuple{
                meta::octet_type(true, 3),
                meta::octet_type(false, 3),
            }, {
                accessor::binary{"\x00"sv},
                accessor::binary{"\x00\x00\x00"sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_binary_types_test, order_by) {
    execute_statement("CREATE TABLE T (PK INT PRIMARY KEY, C0 VARBINARY(3), C1 BINARY(3))");
    execute_statement("INSERT INTO T VALUES (0, CAST('00' AS VARBINARY(3)), CAST('02' AS BINARY(3)))");
    execute_statement("INSERT INTO T VALUES (1, CAST('0001' AS VARBINARY(3)), CAST('0001' AS BINARY(3)))");
    execute_statement("INSERT INTO T VALUES (2, CAST('0002' AS VARBINARY(3)), CAST('0000' AS BINARY(3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT PK FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[2]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT PK FROM T ORDER BY C1", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(0)), result[2]);
    }
}

TEST_F(sql_binary_types_test, update) {
    execute_statement("CREATE TABLE T (C0 VARBINARY(3), C1 BINARY(3))");
    execute_statement("INSERT INTO T VALUES (CAST('00' AS VARBINARY(3)), CAST('00' AS BINARY(3)))");
    execute_statement("UPDATE T SET C0=CAST('000102' AS VARBINARY(3)), C1=CAST('000102' AS BINARY(3))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
            std::tuple{
                meta::octet_type(true, 3),
                meta::octet_type(false, 3),
            }, {
                accessor::binary{"\x00\x01\x02"sv},
                accessor::binary{"\x00\x01\x02"sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_binary_types_test, comparison) {
    execute_statement("CREATE TABLE T (PK INT PRIMARY KEY, C0 VARBINARY(3), C1 VARBINARY(3))");
    execute_statement("INSERT INTO T VALUES (0, CAST('00' AS VARBINARY(3)), CAST('02' AS VARBINARY(3)))");
    execute_statement("INSERT INTO T VALUES (1, CAST('0002' AS VARBINARY(3)), CAST('0001' AS VARBINARY(3)))");
    execute_statement("INSERT INTO T VALUES (2, CAST('0000' AS VARBINARY(3)), CAST('0000' AS VARBINARY(3)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT PK FROM T WHERE C0 < C1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT PK FROM T WHERE C0 > C1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT PK FROM T WHERE C0 = C1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(sql_binary_types_test, insert_by_literal_cast_on_context) {
    execute_statement("CREATE TABLE T (C0 VARBINARY(3), C1 BINARY(3))");
    execute_statement("INSERT INTO T VALUES ('000102', '000304')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
            std::tuple{
                meta::octet_type(true, 3),
                meta::octet_type(false, 3),
            }, {
                accessor::binary{"\x00\x01\x02"sv},
                accessor::binary{"\x00\x03\x04"sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_binary_types_test, length_unspecified_for_types) {
    execute_statement("CREATE TABLE T (C0 VARBINARY, C1 BINARY)");
    execute_statement("INSERT INTO T VALUES ('000102', '00')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
            std::tuple{
                meta::octet_type(true),
                meta::octet_type(false, 1),
            }, {
                accessor::binary{"\x00\x01\x02"sv},
                accessor::binary{"\x00"sv},
            }
        )), result[0]);
    }
}

// TODO enable after fixing insufficient storage by encoder
TEST_F(sql_binary_types_test, DISABLED_scan_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 BINARY(3), C1 BINARY(3), PRIMARY KEY(C0,C1))");
    execute_statement("INSERT INTO T VALUES ('000000', '000000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = CAST('00000000' AS BINARY(4))", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_binary_types_test, find_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 BINARY(3), C1 BINARY(3), PRIMARY KEY(C0))");
    execute_statement("INSERT INTO T VALUES ('000000', '000000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = CAST('00000000' AS BINARY(4))", result);
        ASSERT_EQ(0, result.size());
    }
}

}  // namespace jogasaki::testing
