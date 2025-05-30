/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_like_escape_test :
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

struct TestCase {
    int from_value;
    std::optional<int> for_value;
    std::optional<std::string> expected;
};

using namespace std::string_view_literals;

TEST_F(sql_like_escape_test, ok) {
    std::string res    = "いa_é𐍈b%字🧡z%%한_bü";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::string> like = {
        /* Exact Match */
        "いac_é𐍈bc%字🧡zc%c%한c_bü",
        /* Exact Match2 */
        "%"
        /* Prefix Match */
        "い%",
        /* Prefix Match2 */
        "いac_é𐍈%",
        /* Suffix Match */
        "%ü",
        /* Suffix Match2 */
        "%한_bü",
        /* Substring Match */
        "%字%",
        /* One or more characters match */
        "%_",
        /* One or more characters match2 */
        "_%",
        /* many WildcardAny */
        "%%字🧡zc%c%%bü",
        /* minor case */
        /* first WildcardOne*/
        "_a_%",
        /* Substring Match2 */
        "い%字🧡%",
        /* MIX */
        "いa__𐍈b%%",
        /* MIX2 */
        "%é𐍈b%%",
        /* useless WildcardAny */
        "%い%a%c_%é%𐍈%b%c%%字%🧡%z%c%%c%%한%_%b%ü%",
        /* all WildcardOne */
        "________________"
    };
    // "いa_é𐍈b%字🧡z%%한_bü";
    for (const auto& pattern : like) {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + pattern +
                            std::string("' ESCAPE 'c'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        accessor::text expected_text(res);
        EXPECT_EQ((create_nullable_record<kind::character>(expected_text)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(sql_like_escape_test, ng) {
    std::string res    = "いa_é𐍈b%字🧡z%%한_bü";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::string> like = {
        /* Exact Match */
        "いac_é𐍈bc%字🧡z川%c%ac_bü",
        /* Prefix Match */
        "🧡%",
        /* Suffix Match */
        "%字",
        /* Substring Match */
        "%字d한%",
        /* many WildcardAny */
        "%%字🧡zk%c%%bü",
        /* unknown charactor */
        "い%漢",
        /* い is not second */
        "_い%",
        /* ü is not second end */
        "%ü_",
        /* less WildcardOne */
        "_______",
        /* more WildcardOne */
        "____________________",
        /* useless WildcardAny */
        "%い%a%c_%é%𐍈%b%c%%字%🧡%z%c%%c%%한%_%é%ü%",
        };
    for (const auto& pattern : like) {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + pattern +
                            std::string("' ESCAPE 'c'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(0, result.size()) << "Query failed: " << query;
    }
}
TEST_F(sql_like_escape_test, null) {
    std::string res    = "いa_é𐍈b%字🧡z%%한_bü";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE NULL ESCAPE 'c'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(0, result.size()) << "Query failed: " << query;
    }
    {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE 'い%' ESCAPE NULL ");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(0, result.size()) << "Query failed: " << query;
    }
}
TEST_F(sql_like_escape_test, input_null) {
    execute_statement("create table t1 (c0 varchar)");
    execute_statement("insert into t1 values (NULL)");
    {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE 'c'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(0, result.size()) << "Query failed: " << query;
    }
    {
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE 'c' ESCAPE 'a' ");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(0, result.size()) << "Query failed: " << query;
    }
}
TEST_F(sql_like_escape_test, escape) {
    std::string res    = "abcd";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::pair<std::string, std::string>> like_with_escape = {{"@a@b@c@d", "@"},
        {"éaébécéd", "é"}, {"𐍈a𐍈b𐍈c𐍈d", "𐍈"}, {"🧡a🧡b🧡c🧡d", "🧡"},
        {"한a한b한c한d", "한"}, {"üaübücüd", "ü"}, {"%a%b%c%d", "%"}, {"_a_b_c_d", "_"}};
    for (const auto& pattern : like_with_escape) {
        const auto& like   = pattern.first;
        const auto& escape = pattern.second;
        std::string query  = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + like +
                            std::string("' ESCAPE '") + escape + std::string("'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        accessor::text expected_text(res);
        EXPECT_EQ((create_nullable_record<kind::character>(expected_text)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(sql_like_escape_test, escape_error_not_one_escape) {
    std::string res    = "abcd";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::pair<std::string, std::string>> like_with_escape = {{"@a@b@c@d", "@@"},
        {"éaébécéd", "éé"}, {"𐍈a𐍈b𐍈c𐍈d", "𐍈𐍈"}, {"🧡a🧡b🧡c🧡d", "🧡🧡"},
        {"한a한b한c한d", "한한"}, {"üaübücüd", "üü"}, {"%a%b%c%d", "%%"}, {"_a_b_c_d", "___"},
    {"@a@b@c@d", "@@@"}};
    for (const auto& pattern : like_with_escape) {
        const auto& like   = pattern.first;
        const auto& escape = pattern.second;
        std::string query  = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + like +
                            std::string("' ESCAPE '") + escape + std::string("'");
        test_stmt_err(query, error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(sql_like_escape_test, escape_end) {
    std::string res    = "abcd";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::pair<std::string, std::string>> like_with_escape = {{"@a@b@c@d@", "@"},
        {"éaébécédé", "é"}, {"𐍈a𐍈b𐍈c𐍈d𐍈", "𐍈"}, {"🧡a🧡b🧡c🧡d🧡", "🧡"},
        {"한a한b한c한d한", "한"}, {"üaübücüdü", "ü"}, {"%a%b%c%d%", "%"}, {"_a_b_c_d_", "_"}};
    for (const auto& pattern : like_with_escape) {
        const auto& like   = pattern.first;
        const auto& escape = pattern.second;
        std::string query  = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + like +
                            std::string("' ESCAPE '") + escape + std::string("'");
        test_stmt_err(query, error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(sql_like_escape_test, escape_equal_like) {
    std::string res    = "abcd";
    std::string insert = "insert into t1 values ('" + res + "')";
    execute_statement("create table t1 (c0 varchar)");
    execute_statement(insert);
    std::vector<std::pair<std::string, std::string>> like_with_escape = {{"@", "@"},
        {"é", "é"}, {"𐍈", "𐍈"}, {"🧡", "🧡"},
        {"한", "한"}, {"ü", "ü"}, {"%", "%"}, {"_", "_"}};
    for (const auto& pattern : like_with_escape) {
        const auto& like   = pattern.first;
        const auto& escape = pattern.second;
        std::string query  = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + like +
                            std::string("' ESCAPE '") + escape + std::string("'");
        test_stmt_err(query, error_code::unsupported_runtime_feature_exception);
    }
}

TEST_F(sql_like_escape_test, escape_input_twice) {
    std::vector<std::string> results = {"a", "é", "𐍈", "🧡", "한", "ü", "%", "_"};
    for (const auto& res : results) {
        std::string insert = "insert into t1 values ('" + res + "')";
        execute_statement("create table t1 (c0 varchar)");
        execute_statement(insert);
        std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE '") + res + res +
                            std::string("' ESCAPE '") + res + std::string("'");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        accessor::text expected_text(res);
        EXPECT_EQ((create_nullable_record<kind::character>(expected_text)), result[0])
            << "Failed query: " << query;
        execute_statement("drop table t1");
    }
}

TEST_F(sql_like_escape_test, all_column) {
    std::string res = "😁öa出";
    execute_statement("create table t1 (c0 varchar,c1 varchar ,c2 varchar)");
    execute_statement("INSERT INTO t1 VALUES('😁öa出','%aa%','a')");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0 FROM t1 WHERE c0 LIKE c1 ESCAPE c2", result);

    ASSERT_EQ(1, result.size()) ;
    accessor::text expected_text(res);
    EXPECT_EQ((create_nullable_record<kind::character>(expected_text)), result[0]);
    execute_statement("drop table t1");
}

TEST_F(sql_like_escape_test, invalid_utf8_input) {
    execute_statement("create table t1 (c0 varchar)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xF4\x27\x80\x80");
    execute_statement("INSERT INTO t1 (c0) VALUES (:p0)", variables, *ps);
    std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE 'c'");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(0, result.size()) << "Query failed: " << query;
}
TEST_F(sql_like_escape_test, invalid_utf8_like) {
    execute_statement("create table t1 (c0 varchar,c1 varchar)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xF4\x27\x80\x80");
    execute_statement("INSERT INTO t1 (c0,c1) VALUES ('abc',:p0)", variables, *ps);
    std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE c1");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(0, result.size()) << "Query failed: " << query;
}
TEST_F(sql_like_escape_test, invalid_utf8_escape) {
    execute_statement("create table t1 (c0 varchar,c1 varchar,c2 varchar)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\x80");
    execute_statement("INSERT INTO t1 (c0,c1,c2) VALUES ('abc','a%',:p0)", variables, *ps);
    std::string query = std::string("SELECT c0 FROM t1 WHERE c0 LIKE c1 ESCAPE c2");
    std::vector<mock::basic_record> result{};
    test_stmt_err(query, error_code::unsupported_runtime_feature_exception);
}
} // namespace jogasaki::testing
