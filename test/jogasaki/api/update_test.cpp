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

class update_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(update_test, update_by_part_of_primary_key) {
    execute_statement("INSERT INTO T20 (C0, C2, C4) VALUES (1, 100.0, '111')");
    execute_statement("UPDATE T20 SET C2=200.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T20", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_TRUE(rec.is_null(1));
    EXPECT_DOUBLE_EQ(200.0, rec.get_value<double>(2));
    EXPECT_FALSE(rec.is_null(2));
}

TEST_F(update_test, update_primary_key) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement("UPDATE T0 SET C0=3, C1=30.0 WHERE C1=10.0");
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

TEST_F(update_test, update_delete_secondary_index) {
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

TEST_F(update_test, update_char_columns) {
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

TEST_F(update_test, update_by_null) {
    execute_statement("INSERT INTO T0(C0, C1) VALUES (0, 0.0)");
    execute_statement("UPDATE T0 SET C1=NULL WHERE C0=0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>({0, 0.0}, {false, true})), result[0]);
    }
}

TEST_F(update_test, hitting_existing_pk) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on abort";
    }
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (1, 1)");
    test_stmt_err("UPDATE T SET C0=C0+1 WHERE C0=0", error_code::unique_constraint_violation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(0, 0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[1]);
    }
}

TEST_F(update_test, multiple_rows_hitting_existing_pk) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on abort";
    }
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (1, 1)");
    execute_statement("INSERT INTO T VALUES (2, 2)");
    test_stmt_err("UPDATE T SET C0=C0+1", error_code::unique_constraint_violation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(0, 0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 2)), result[2]);
    }
}

TEST_F(update_test, multiple_rows_wo_hitting_existing_pk) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory behaves differently on conflicting pk";
    }
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (2, 2)");
    execute_statement("INSERT INTO T VALUES (4, 4)");
    execute_statement("UPDATE T SET C0=C0+1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 2)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(5, 4)), result[2]);
    }
}

// once occ hit serialization failure on commit
TEST_F(update_test, multiple_rows_by_minus_one) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (0)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("UPDATE T SET C0=C0-1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[1]);
    }
}

// once occ hit serialization failure
TEST_F(update_test, multiple_rows_by_minus_11) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (1, 1)");
    execute_statement("INSERT INTO T VALUES (2, 2)");
    execute_statement("INSERT INTO T VALUES (3, 3)");
    execute_statement("INSERT INTO T VALUES (4, 4)");
    execute_statement("INSERT INTO T VALUES (5, 5)");
    execute_statement("INSERT INTO T VALUES (6, 6)");
    execute_statement("INSERT INTO T VALUES (7, 7)");
    execute_statement("INSERT INTO T VALUES (8, 8)");
    execute_statement("INSERT INTO T VALUES (9, 9)");
    execute_statement("UPDATE T SET C0=C0-11");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(10, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(-11, 0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(-2, 9)), result[9]);
    }
}

TEST_F(update_test, verify_error_abort_tx) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on abort";
    }
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 DECIMAL(5,2))");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
    };
    auto ps = api::create_parameter_set();
    auto v1 = decimal_v{1, 0, 1, 0}; // 1
    ps->set_decimal("p0", v1);
    execute_statement("INSERT INTO T VALUES (1, :p0)", variables, *ps);
    test_stmt_err("UPDATE T SET C1=C1 / 3 WHERE C0=1", error_code::value_evaluation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 2)};
        auto i4 = meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::decimal>(std::tuple{i4, dec}, {1, v1})), result[0]);
    }
}


}
