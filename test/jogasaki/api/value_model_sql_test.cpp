/*
 * Copyright 2026 Project Tsurugi.
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
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/error_code.h>
#include <jogasaki/mock/basic_record.h>
#include "api_test_base.h"

namespace jogasaki::testing {
using namespace jogasaki::mock;
using kind = meta::field_type_kind;
class value_model_sql_test : public ::testing::Test, public api_test_base {
public:
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(std::make_shared<configuration>()); }
    void TearDown() override { db_teardown(); }
};

TEST_F(value_model_sql_test, insert_prepared_tuples_with_distinct_types_and_defaults) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v BIGINT, tag VARCHAR(64) DEFAULT 'fallback')");
    execute_statement("INSERT INTO t (v,k) VALUES (CAST(10 AS INT),1),(20,2),(NULL,3)");
    std::vector<basic_record> rows;
    execute_query("SELECT k,v,tag FROM t ORDER BY k", rows);
    ASSERT_EQ(rows.size(), 3);
    auto types = std::make_tuple(meta::field_type{meta::field_enum_tag<kind::int4>},
        meta::field_type{meta::field_enum_tag<kind::int8>},
        meta::field_type{std::make_shared<meta::character_field_option>(true, 64)});
    EXPECT_EQ((typed_nullable_record<kind::int4,kind::int8,kind::character>(types,1,10,accessor::text{"fallback"})), rows[0]);
    EXPECT_EQ((typed_nullable_record<kind::int4,kind::int8,kind::character>(types,2,20,accessor::text{"fallback"})), rows[1]);
    EXPECT_EQ((typed_nullable_record<kind::int4,kind::int8,kind::character>(types,3,std::nullopt,accessor::text{"fallback"})), rows[2]);
}
TEST_F(value_model_sql_test, insert_prepared_not_null_violation) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v BIGINT NOT NULL)");
    test_stmt_err("INSERT INTO t VALUES (1,NULL)", error_code::not_null_constraint_violation_exception);
}
TEST_F(value_model_sql_test, project_cast_lengths_and_typed_null) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT, tag VARCHAR(4))");
    execute_statement("INSERT INTO t VALUES (1,42,'Ab')");
    std::vector<basic_record> rows;
    execute_query("SELECT CAST(v AS BIGINT),CAST(tag AS VARCHAR(128)),CAST(NULL AS BIGINT) FROM t", rows);
    ASSERT_EQ(rows.size(), 1);
    auto types = std::make_tuple(meta::field_type{meta::field_enum_tag<kind::int8>},
        meta::field_type{std::make_shared<meta::character_field_option>(true, 128)},
        meta::field_type{meta::field_enum_tag<kind::int8>});
    EXPECT_EQ((typed_nullable_record<kind::int8,kind::character,kind::int8>(types,42,accessor::text{"Ab"},std::nullopt)), rows[0]);
}
TEST_F(value_model_sql_test, integer_key_out_of_range_is_not_a_wrapped_match) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (-2147483648),(0),(2147483647)");
    std::string plan;
    explain_statement("SELECT k FROM t WHERE k=2147483648", plan);
    EXPECT_NE(plan.find("find"), std::string::npos) << plan;
    for (auto query : {"SELECT k FROM t WHERE k=2147483648", "SELECT k FROM t WHERE k=-2147483649",
                       "SELECT k FROM t WHERE k>2147483648"}) {
        std::vector<basic_record> rows;
        execute_query(query, rows);
        EXPECT_TRUE(rows.empty()) << query;
    }
    std::vector<basic_record> rows;
    execute_query("SELECT k FROM t WHERE k<2147483648 ORDER BY k", rows);
    EXPECT_EQ(rows.size(), 3);
}
TEST_F(value_model_sql_test, mixed_exact_and_approximate_comparison_remains_rejected) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    for (auto op : {"=", ">", "<"}) {
        test_stmt_err(std::string{"SELECT k FROM t WHERE k"} + op + "CAST(0.5 AS DOUBLE)",
            error_code::type_analyze_exception);
    }
}
TEST_F(value_model_sql_test, unsupported_sql_types_remain_rejected) {
    test_stmt_err("CREATE TABLE t (k BOOLEAN PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (k SMALLINT PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}
TEST_F(value_model_sql_test, decimal_search_key_conversion_remains_unsupported) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    test_stmt_err("SELECT k FROM t WHERE k=0.5", error_code::unsupported_runtime_feature_exception);
}
TEST_F(value_model_sql_test, prepared_insert_uses_current_parameters) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v BIGINT)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"k", api::field_type_kind::int8}, {"v", api::field_type_kind::int8}};
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO t VALUES (:k,:v)", variables, prepared));
    auto params = api::create_parameter_set();
    for (std::int64_t i = 1; i <= 2; ++i) {
        params->set_int8("k", i);
        params->set_int8("v", i * 10);
        execute_statement(prepared, variables, *params);
    }
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
    std::vector<basic_record> rows;
    execute_query("SELECT k,v FROM t ORDER BY k", rows);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ((create_nullable_record<kind::int4,kind::int8>(1,10)), rows[0]);
    EXPECT_EQ((create_nullable_record<kind::int4,kind::int8>(2,20)), rows[1]);
}

TEST_F(value_model_sql_test, project_untyped_null) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");
    std::vector<basic_record> rows;
    execute_query("SELECT NULL FROM t", rows);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].record_meta()->at(0).kind(), kind::unknown);
    EXPECT_TRUE(rows[0].is_null(0));
}
TEST_F(value_model_sql_test, descending_secondary_key_out_of_range) {
    execute_statement("CREATE TABLE t (id INT PRIMARY KEY, k INT)");
    execute_statement("CREATE INDEX i ON t (k DESC)");
    execute_statement("INSERT INTO t VALUES (1,-2147483648),(2,0),(3,2147483647),(4,NULL)");
    std::string plan;
    explain_statement("SELECT k FROM t WHERE k<2147483648", plan);
    EXPECT_NE(plan.find("\"i\""), std::string::npos) << plan;
    for (auto predicate : {"k<2147483648", "k<=2147483648", "k>-2147483649", "k>=-2147483649"}) {
        SCOPED_TRACE(predicate);
        std::vector<basic_record> rows;
        execute_query(std::string{"SELECT k FROM t WHERE "} + predicate, rows);
        EXPECT_EQ(rows.size(), 3);
    }
    for (auto predicate : {"k>2147483648", "k>=2147483648", "k<-2147483649", "k<=-2147483649"}) {
        SCOPED_TRACE(predicate);
        std::vector<basic_record> rows;
        execute_query(std::string{"SELECT k FROM t WHERE "} + predicate, rows);
        EXPECT_TRUE(rows.empty());
    }
}
TEST_F(value_model_sql_test, composite_prefix_and_trailing_range) {
    execute_statement("CREATE TABLE t (a INT, b INT, PRIMARY KEY(a,b))");
    execute_statement("INSERT INTO t VALUES (1,0),(1,2147483647),(2,0)");
    std::vector<basic_record> rows;
    execute_query("SELECT b FROM t WHERE a=1 AND b<2147483648 ORDER BY b", rows);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(create_nullable_record<kind::int4>(0), rows[0]);
    EXPECT_EQ(create_nullable_record<kind::int4>(2147483647), rows[1]);
    for (auto predicate : {"a=1 AND b>2147483648", "a=2147483648 AND b>0"}) {
        rows.clear();
        execute_query(std::string{"SELECT b FROM t WHERE "} + predicate, rows);
        EXPECT_TRUE(rows.empty());
    }
}
TEST_F(value_model_sql_test, left_join_find_out_of_range_is_no_match) {
    execute_statement("CREATE TABLE small (k INT PRIMARY KEY)");
    execute_statement("CREATE TABLE big (k BIGINT PRIMARY KEY)");
    execute_statement("INSERT INTO small VALUES (0),(1)");
    execute_statement("INSERT INTO big VALUES (-4294967296),(0),(4294967296)");
    std::string query = "SELECT l.k,r.k FROM big l LEFT JOIN small r ON l.k=r.k ORDER BY l.k";
    std::string plan;
    explain_statement(query, plan);
    EXPECT_NE(plan.find("join_find"), std::string::npos) << plan;
    std::vector<basic_record> rows;
    execute_query(query, rows);
    ASSERT_EQ(rows.size(), 3);
    EXPECT_EQ((create_nullable_record<kind::int8,kind::int4>(-4294967296LL,std::nullopt)), rows[0]);
    EXPECT_EQ((create_nullable_record<kind::int8,kind::int4>(0,0)), rows[1]);
    EXPECT_EQ((create_nullable_record<kind::int8,kind::int4>(4294967296LL,std::nullopt)), rows[2]);
}
TEST_F(value_model_sql_test, approximate_key_narrowing_preserves_comparison) {
    execute_statement("CREATE TABLE t (id INT PRIMARY KEY, k REAL)");
    execute_statement("CREATE INDEX i ON t (k)");
    execute_statement("INSERT INTO t VALUES (1,CAST(0.1 AS REAL)),(2,CAST(0.01 AS REAL))");
    std::string plan;
    explain_statement("SELECT id FROM t WHERE k=CAST(0.1 AS DOUBLE)", plan);
    EXPECT_NE(plan.find("\"i\""), std::string::npos) << plan;
    std::vector<basic_record> rows;
    execute_query("SELECT id FROM t WHERE k=CAST(0.1 AS DOUBLE)", rows);
    EXPECT_TRUE(rows.empty());
    execute_query("SELECT id FROM t WHERE k<=CAST(0.1 AS DOUBLE)", rows);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(create_nullable_record<kind::int4>(2), rows[0]);
    rows.clear();
    execute_query("SELECT id FROM t WHERE k>=CAST(0.1 AS DOUBLE)", rows);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(create_nullable_record<kind::int4>(1), rows[0]);
}
TEST_F(value_model_sql_test, placeholder_search_keys_out_of_range) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (0),(2147483647)");
    auto params = api::create_parameter_set();
    params->set_int8("p", 4294967296LL);
    std::unordered_map<std::string, api::field_type_kind> variables{{"p",api::field_type_kind::int8}};
    std::vector<basic_record> rows;
    execute_query("SELECT k FROM t WHERE k=:p", variables, *params, rows);
    EXPECT_TRUE(rows.empty());
    execute_query("SELECT k FROM t WHERE k<:p ORDER BY k", variables, *params, rows);
    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(create_nullable_record<kind::int4>(0), rows[0]);
    EXPECT_EQ(create_nullable_record<kind::int4>(2147483647), rows[1]);
}
TEST_F(value_model_sql_test, rounded_empty_interval) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (0),(2147483647)");
    std::vector<basic_record> rows;
    execute_query("SELECT k FROM t WHERE k>3000000000 AND k<4000000000", rows);
    EXPECT_TRUE(rows.empty());
}
TEST_F(value_model_sql_test, insert_identity_defaults_keep_tuple_order) {
    execute_statement("CREATE TABLE t (k BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v INT)");
    execute_statement("INSERT INTO t (v) VALUES (7),(8),(9)");
    std::vector<basic_record> rows;
    execute_query("SELECT k,v FROM t ORDER BY k", rows);
    ASSERT_EQ(rows.size(), 3);
    for (std::size_t i = 0; i < rows.size(); ++i) {
        auto const& meta = *rows[i].record_meta();
        EXPECT_EQ(rows[i].ref().get_value<std::int32_t>(meta.value_offset(1)), 7 + i);
        if (i != 0) {
            EXPECT_EQ(rows[i].ref().get_value<std::int64_t>(meta.value_offset(0)),
                rows[i-1].ref().get_value<std::int64_t>(rows[i-1].record_meta()->value_offset(0)) + 1);
        }
    }
}
TEST_F(value_model_sql_test, insert_expression_error_keeps_error_code) {
    execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v BIGINT)");
    test_stmt_err("INSERT INTO t VALUES (1,1/0)", error_code::value_evaluation_exception);
}

TEST_F(value_model_sql_test, real_key_overflow_remains_unsupported) {
    execute_statement("CREATE TABLE t (id INT PRIMARY KEY, k REAL)");
    execute_statement("CREATE INDEX i ON t (k)");
    execute_statement("INSERT INTO t VALUES (1,CAST(0.1 AS REAL)),(2,CAST(-0.1 AS REAL))");
    std::unordered_map<std::string, api::field_type_kind> variables{{"p", api::field_type_kind::float8}};
    auto params = api::create_parameter_set();
    for (double value : {1.0e39, -1.0e39}) {
        params->set_float8("p", value);
        auto sql = std::string{"SELECT id FROM t WHERE k=:p"};
        SCOPED_TRACE(std::to_string(value));
        std::string plan;
        explain_statement(sql, plan, *params, variables);
        EXPECT_NE(plan.find("\"i\""), std::string::npos) << plan;
        test_stmt_err(sql, variables, *params, error_code::unsupported_runtime_feature_exception);
    }
}
} // namespace jogasaki::testing
