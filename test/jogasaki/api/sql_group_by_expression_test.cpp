/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_view_literals;

using kind = meta::field_type_kind;
using mock::create_nullable_record;
using mock::typed_nullable_record;

using triple = takatori::decimal::triple;
using text = accessor::text;
using binary = accessor::binary;

/**
 * @brief tests for GROUP BY with expressions (not only plain columns) as grouping keys
 * @details the SQL compiler extracts the group key expressions into a `project` operator placed before the
 * aggregation (tsurugi-issues #1568). Most aggregate functions are executed by the aggregate exchange, while
 * COUNT(DISTINCT ...) is executed by the group exchange and the aggregate_group operator, so both code paths are
 * tested here.
 */
class sql_group_by_expression_test :
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

    void prepare_data() {
        execute_statement(
            "create table t (pk int primary key, c0 int, c1 varchar(100), c2 decimal(5,1), c3 date, c4 char(5), c5 varbinary(10))"
        );
        execute_statement("insert into t values (1, 1, 'aaa1', 1.0, date'2024-01-01', 'ab', x'01')");
        execute_statement("insert into t values (2, 2, 'aaa2', 2.0, date'2024-02-01', 'ab ', x'0100')");
        execute_statement("insert into t values (3, 3, 'bbb1', null, date'2025-01-01', 'cd', x'02')");
        execute_statement("insert into t values (4, null, 'bbb2', 1.0, null, null, null)");
    }

    // create rows whose decimal expression `case when pk < 10 then c0 * 1 else c1 end` evaluates to 1 with
    // different representations - (10, -1) for the rows pk < 10 and (1, 0) for others
    void prepare_decimal_data() {
        execute_statement("create table t (pk int primary key, c0 decimal(5,1), c1 decimal(5,2))");
        for(std::size_t i = 0; i < 20; ++i) {
            execute_statement("insert into t values (" + std::to_string(i) + ", 1.0, 1.00)");
        }
    }

    std::vector<mock::basic_record> query(std::string_view sql) {
        std::vector<mock::basic_record> result{};
        execute_query(sql, result);
        std::sort(result.begin(), result.end());
        return result;
    }
};

TEST_F(sql_group_by_expression_test, arithmetic) {
    prepare_data();
    auto result = query("SELECT c0 % 2, count(*), sum(pk) FROM t GROUP BY c0 % 2");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int4>(std::nullopt, 1, 4)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int4>(0, 1, 2)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int4>(1, 2, 4)), result[2]);
}

TEST_F(sql_group_by_expression_test, group_key_used_in_select_expression) {
    // the group key expression appears as a sub-expression in the select list
    prepare_data();
    auto result = query("SELECT (c0 + 1) * 10, sum(pk) FROM t GROUP BY c0 + 1");
    ASSERT_EQ(4, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int4>(std::nullopt, 4)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int4>(20, 1)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int4>(30, 2)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int4>(40, 3)), result[3]);
}

TEST_F(sql_group_by_expression_test, string_function) {
    prepare_data();
    auto result = query("SELECT substring(c1 from 1 for 3), count(*), max(c1) FROM t GROUP BY substring(c1 from 1 for 3)");
    ASSERT_EQ(2, result.size());
    auto types = std::tuple{meta::character_type(true), meta::int8_type(), meta::character_type(true)};
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8, kind::character>(types, text{"aaa"}, 2, text{"aaa2"})), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8, kind::character>(types, text{"bbb"}, 2, text{"bbb2"})), result[1]);
}

TEST_F(sql_group_by_expression_test, long_string_key) {
    // verify varlen data created by the project operator is correctly carried as the group key
    // (long enough not to be inlined in accessor::text)
    prepare_data();
    auto result = query(
        "SELECT upper(substring(c1 from 1 for 3)) || '-0123456789012345678901234567890123456789', count(*) "
        "FROM t GROUP BY upper(substring(c1 from 1 for 3)) || '-0123456789012345678901234567890123456789'"
    );
    ASSERT_EQ(2, result.size());
    auto types = std::tuple{meta::character_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"AAA-0123456789012345678901234567890123456789"}, 2)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"BBB-0123456789012345678901234567890123456789"}, 2)), result[1]);
}

TEST_F(sql_group_by_expression_test, extract) {
    // motivating example of the issue #1568
    prepare_data();
    auto result = query("SELECT extract(year from c3), count(*) FROM t GROUP BY extract(year from c3)");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(std::nullopt, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(2024, 2)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(2025, 1)), result[2]);
}

TEST_F(sql_group_by_expression_test, boolean_key) {
    prepare_data();
    auto result = query("SELECT c0 > 1, count(*) FROM t GROUP BY c0 > 1");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::boolean, kind::int8>(std::nullopt, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::boolean, kind::int8>(false, 1)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::boolean, kind::int8>(true, 2)), result[2]);
}

TEST_F(sql_group_by_expression_test, searched_case) {
    // searched case can be referred from the select list (simple case cannot, see tsurugidb docs/sql-features.md)
    prepare_data();
    auto result = query("SELECT case when c0 > 1 then 'big' else 'small' end, count(*) FROM t GROUP BY case when c0 > 1 then 'big' else 'small' end");
    ASSERT_EQ(2, result.size());
    auto types = std::tuple{meta::character_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"big"}, 2)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"small"}, 2)), result[1]);
}

TEST_F(sql_group_by_expression_test, cast_to_character) {
    prepare_data();
    {
        // 'ab' and 'ab ' are both padded in char(5) column
        auto result = query("SELECT cast(c4 as varchar(10)), count(*) FROM t GROUP BY cast(c4 as varchar(10))");
        ASSERT_EQ(3, result.size());
        auto types = std::tuple{meta::character_type(true, 10), meta::int8_type()};
        EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, std::nullopt, 1)), result[0]);
        EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"ab   "}, 2)), result[1]);
        EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"cd   "}, 1)), result[2]);
    }
    {
        auto result = query("SELECT cast(c1 as char(3)), count(*) FROM t GROUP BY cast(c1 as char(3))");
        ASSERT_EQ(2, result.size());
        auto types = std::tuple{meta::character_type(false, 3), meta::int8_type()};
        EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"aaa"}, 2)), result[0]);
        EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"bbb"}, 2)), result[1]);
    }
}

TEST_F(sql_group_by_expression_test, binary_key) {
    prepare_data();
    auto result = query("SELECT substring(c5 from 1 for 1), count(*) FROM t GROUP BY substring(c5 from 1 for 1)");
    ASSERT_EQ(3, result.size());
    auto types = std::tuple{meta::octet_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::octet, kind::int8>(types, std::nullopt, 1)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::octet, kind::int8>(types, binary{"\x01"}, 2)), result[1]);
    EXPECT_EQ((typed_nullable_record<kind::octet, kind::int8>(types, binary{"\x02"}, 1)), result[2]);
}

TEST_F(sql_group_by_expression_test, decimal_key) {
    prepare_data();
    auto result = query("SELECT c2 * 2, count(*) FROM t GROUP BY c2 * 2");
    ASSERT_EQ(3, result.size());
    auto types = std::tuple{meta::decimal_type(), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::decimal, kind::int8>(types, std::nullopt, 1)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::decimal, kind::int8>(types, triple{2}, 2)), result[1]);
    EXPECT_EQ((typed_nullable_record<kind::decimal, kind::int8>(types, triple{4}, 1)), result[2]);
}

TEST_F(sql_group_by_expression_test, multiple_keys) {
    // mixing a plain column and expressions, including the ones depending on each other
    prepare_data();
    auto result = query("SELECT c0 % 2, c0 % 2 + 1, substring(c1 from 1 for 1), count(*) FROM t GROUP BY c0 % 2, c0 % 2 + 1, substring(c1 from 1 for 1)");
    ASSERT_EQ(4, result.size());
    auto types = std::tuple{meta::int8_type(), meta::int8_type(), meta::character_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::int8, kind::int8, kind::character, kind::int8>(types, std::nullopt, std::nullopt, text{"b"}, 1)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::int8, kind::int8, kind::character, kind::int8>(types, 0, 1, text{"a"}, 1)), result[1]);
    EXPECT_EQ((typed_nullable_record<kind::int8, kind::int8, kind::character, kind::int8>(types, 1, 2, text{"a"}, 1)), result[2]);
    EXPECT_EQ((typed_nullable_record<kind::int8, kind::int8, kind::character, kind::int8>(types, 1, 2, text{"b"}, 1)), result[3]);
}

TEST_F(sql_group_by_expression_test, having_and_order_by) {
    prepare_data();
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0 % 2, count(*) FROM t GROUP BY c0 % 2 HAVING c0 % 2 IS NOT NULL ORDER BY c0 % 2 DESC", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(0, 1)), result[1]);
}

TEST_F(sql_group_by_expression_test, without_aggregate_function) {
    prepare_data();
    auto result = query("SELECT c0 % 2 FROM t GROUP BY c0 % 2");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8>(std::nullopt)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8>(0)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[2]);
}

TEST_F(sql_group_by_expression_test, placeholder_in_key) {
    prepare_data();
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 2);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0 % :p0, count(*) FROM t GROUP BY c0 % :p0", variables, *ps, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(std::nullopt, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(0, 1)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 2)), result[2]);
}

TEST_F(sql_group_by_expression_test, empty_input) {
    prepare_data();
    EXPECT_EQ(0, query("SELECT c0 % 2, count(*) FROM t WHERE pk > 100 GROUP BY c0 % 2").size());
    EXPECT_EQ(0, query("SELECT c0 % 2, count(distinct c1) FROM t WHERE pk > 100 GROUP BY c0 % 2").size());
}

TEST_F(sql_group_by_expression_test, with_rtx) {
    // RTX runs parallel scan, so the exchange receives records from multiple upstream partitions
    prepare_data();
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_, true, false);
    execute_query("SELECT substring(c1 from 1 for 3), count(*) FROM t GROUP BY substring(c1 from 1 for 3)", *tx, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(2, result.size());
    auto types = std::tuple{meta::character_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"aaa"}, 2)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"bbb"}, 2)), result[1]);
}

// following tests are for COUNT(DISTINCT ...), which is processed by the group exchange and aggregate_group operator

TEST_F(sql_group_by_expression_test, count_distinct_arithmetic) {
    prepare_data();
    auto result = query("SELECT c0 % 2, count(distinct c1) FROM t GROUP BY c0 % 2");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(std::nullopt, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(0, 1)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 2)), result[2]);
}

TEST_F(sql_group_by_expression_test, count_distinct_string_function) {
    prepare_data();
    auto result = query("SELECT substring(c1 from 1 for 3), count(distinct c2) FROM t GROUP BY substring(c1 from 1 for 3)");
    ASSERT_EQ(2, result.size());
    auto types = std::tuple{meta::character_type(true), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"aaa"}, 2)), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::character, kind::int8>(types, text{"bbb"}, 1)), result[1]);
}

TEST_F(sql_group_by_expression_test, count_distinct_with_expression_argument) {
    // both group key and the count distinct argument are expressions
    prepare_data();
    auto result = query("SELECT extract(year from c3), count(distinct substring(c1 from 1 for 3)) FROM t GROUP BY extract(year from c3)");
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(std::nullopt, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(2024, 1)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(2025, 1)), result[2]);
}

TEST_F(sql_group_by_expression_test, count_distinct_having_and_order_by) {
    prepare_data();
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0 % 2, count(distinct c1) FROM t GROUP BY c0 % 2 HAVING count(distinct c1) > 0 AND c0 % 2 IS NOT NULL ORDER BY c0 % 2", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(0, 1)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 2)), result[1]);
}

// following tests verify decimal values that are numerically equal but have different representations are grouped
// together when such values are created by the group key expression. See also sql_decimal_representation_test for
// the cases that do not use expressions in GROUP BY.

TEST_F(sql_group_by_expression_test, decimal_key_with_different_representations) {
    prepare_decimal_data();
    auto result = query("SELECT case when pk < 10 then c0 * 1 else c1 end, count(*) FROM t GROUP BY case when pk < 10 then c0 * 1 else c1 end");
    ASSERT_EQ(1, result.size());
    auto types = std::tuple{meta::decimal_type(), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::decimal, kind::int8>(types, triple{1}, 20)), result[0]);
}

TEST_F(sql_group_by_expression_test, decimal_key_with_different_representations_count_distinct) {
    prepare_decimal_data();
    auto result = query("SELECT case when pk < 10 then c0 * 1 else c1 end, count(distinct pk) FROM t GROUP BY case when pk < 10 then c0 * 1 else c1 end");
    ASSERT_EQ(1, result.size());
    auto types = std::tuple{meta::decimal_type(), meta::int8_type()};
    EXPECT_EQ((typed_nullable_record<kind::decimal, kind::int8>(types, triple{1}, 20)), result[0]);
}

}  // namespace jogasaki::testing
