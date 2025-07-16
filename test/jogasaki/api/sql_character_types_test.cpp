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

class sql_character_types_test :
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

TEST_F(sql_character_types_test, insert_select) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(3), C1 CHAR(3))");
    execute_statement("INSERT INTO T VALUES (' ', ' ')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{
                meta::character_type(true, 3),
                meta::character_type(false, 3),
            }, {
                accessor::text{" "sv},
                accessor::text{"   "sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_character_types_test, order_by) {
    execute_statement("CREATE TABLE T (PK INT PRIMARY KEY, C0 VARCHAR(3), C1 CHAR(3))");
    execute_statement("INSERT INTO T VALUES (0, '0', '2')");
    execute_statement("INSERT INTO T VALUES (1, '01', '01')");
    execute_statement("INSERT INTO T VALUES (2, '02', '00')");
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

TEST_F(sql_character_types_test, update) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(3), C1 CHAR(3))");
    execute_statement("INSERT INTO T VALUES (' ', ' ')");
    execute_statement("UPDATE T SET C0='012', C1='012'");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{
                meta::character_type(true, 3),
                meta::character_type(false, 3),
            }, {
                accessor::text{"012"sv},
                accessor::text{"012"sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_character_types_test, comparison) {
    execute_statement("CREATE TABLE T (PK INT PRIMARY KEY, C0 VARCHAR(3), C1 VARCHAR(3))");
    execute_statement("INSERT INTO T VALUES (0, '0', '2')");
    execute_statement("INSERT INTO T VALUES (1, '02', '01')");
    execute_statement("INSERT INTO T VALUES (2, '00', '00')");
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

TEST_F(sql_character_types_test, insert_by_literal_cast_on_context) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(3), C1 CHAR(3))");
    execute_statement("INSERT INTO T VALUES (12, 34)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{
                meta::character_type(true, 3),
                meta::character_type(false, 3),
            }, {
                accessor::text{"12"sv},
                accessor::text{"34 "sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_character_types_test, length_unspecified_for_types) {
    execute_statement("CREATE TABLE T (C0 VARCHAR, C1 CHAR)");
    execute_statement("INSERT INTO T VALUES ('012', '0')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{
                meta::character_type(true),
                meta::character_type(false, 1),
            }, {
                accessor::text{"012"sv},
                accessor::text{"0"sv},
            }
        )), result[0]);
    }
}

TEST_F(sql_character_types_test, scan_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 VARCHAR(3), C1 VARCHAR(3), PRIMARY KEY(C0,C1))");
    execute_statement("INSERT INTO T VALUES ('000', '000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = '0000'", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_character_types_test, find_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 VARCHAR(3), C1 VARCHAR(3), PRIMARY KEY(C0))");
    execute_statement("INSERT INTO T VALUES ('000', '000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = '0000'", result);
        ASSERT_EQ(0, result.size());
    }
}

}  // namespace jogasaki::testing
