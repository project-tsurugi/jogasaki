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

#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/decimal.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/enum_set.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_feature.h>
#include <yugawara/storage/table.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
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

class ddl_metadata_test :
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
    void test_decimal(std::string_view coldef,
        bool has_precision,
        bool has_scale,
        std::size_t precision,
        std::size_t scale
    );
    void test_character(std::string_view coldef,
        bool has_len,
        std::size_t len,
        bool varying
    );
};

using namespace std::string_view_literals;

yugawara::storage::table const* find_table(api::database& db, std::string_view name) {
    auto tables = get_impl(db).tables();
    if(! tables) {
        return {};
    }
    auto t = tables->find_table(name);
    return t.get();
}

yugawara::storage::column const* find_column(yugawara::storage::table const& t, std::string_view name) {
    for(auto && c : t.columns()) {
        if(c.simple_name() == name) {
            return std::addressof(c);
        }
    }
    return {};
}

template <class T>
T const& as(takatori::type::data const& t) {
    return unsafe_downcast<T const&>(t);
}

void ddl_metadata_test::test_decimal(std::string_view coldef,
    bool has_precision,
    bool has_scale,
    std::size_t precision, // meaningful only when has_precision = true
    std::size_t scale // meaningful only when has_scale = true
) {
    execute_statement("CREATE TABLE T (C0 " + std::string{coldef} + " PRIMARY KEY)");
    auto t = find_table(*db_, "T");
    ASSERT_TRUE(t);
    auto c = find_column(*t, "C0");
    ASSERT_TRUE(c);
    ASSERT_EQ(takatori::type::type_kind::decimal, c->type().kind());
    auto p = as<takatori::type::decimal>(c->type()).precision();
    auto s = as<takatori::type::decimal>(c->type()).scale();
    if(has_precision) {
        ASSERT_TRUE(p.has_value());
        EXPECT_EQ(precision, *p);
    } else {
        ASSERT_FALSE(p.has_value());
    }
    if(has_scale) {
        ASSERT_TRUE(s.has_value());
        EXPECT_EQ(scale, *s);
    } else {
        ASSERT_FALSE(s.has_value());
    }
}

TEST_F(ddl_metadata_test, decimal) {
    test_decimal("DECIMAL(5,3)", true, true, 5, 3);
}

TEST_F(ddl_metadata_test, decimal_precision_only) {
    test_decimal("DECIMAL(5)", true, true, 5, 0);
}

TEST_F(ddl_metadata_test, decimal_prec_smaller_than_scale) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(3,4) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, decimal_wo_ps) {
    test_decimal("DECIMAL", true, true, 38, 0);
}

TEST_F(ddl_metadata_test, decimal_wildcard) {
    test_decimal("DECIMAL(*)", true, true, 38, 0);
}

TEST_F(ddl_metadata_test, decimal_precision_wildcard) {
    test_decimal("DECIMAL(*, 3)", true, true, 38, 3);
}

TEST_F(ddl_metadata_test, decimal_ps_wildcards) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(*,*) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, decimal_scale_wildcard) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(5,*) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, decimal_zero) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(0) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, decimal_prec_minus) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(-1) PRIMARY KEY)", error_code::syntax_exception);
}
TEST_F(ddl_metadata_test, decimal_scale_minus) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL(5, -1) PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, decimal_paren_no_len) {
    test_stmt_err("CREATE TABLE T (C0 DECIMAL() PRIMARY KEY)", error_code::syntax_exception);
}

void ddl_metadata_test::test_character(std::string_view coldef,
    bool has_len,
    std::size_t len, // meaningful only when has_len = true
    bool varying
) {
    execute_statement("CREATE TABLE T (C0 " + std::string{coldef} + " PRIMARY KEY)");
    auto t = find_table(*db_, "T");
    ASSERT_TRUE(t);
    auto c = find_column(*t, "C0");
    ASSERT_TRUE(c);
    ASSERT_EQ(takatori::type::type_kind::character, c->type().kind());
    auto l = as<takatori::type::character>(c->type()).length();
    auto var = as<takatori::type::character>(c->type()).varying();
    if(has_len) {
        ASSERT_TRUE(l.has_value());
        EXPECT_EQ(len, *l);
    } else {
        ASSERT_FALSE(l.has_value());
    }
    if(varying) {
        ASSERT_TRUE(var);
    } else {
        ASSERT_FALSE(var);
    }
}

TEST_F(ddl_metadata_test, char_minus) {
    test_stmt_err("CREATE TABLE T (C0 CHAR(-1) PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, char_0) {
    test_stmt_err("CREATE TABLE T (C0 CHAR(0) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, char_wo_len) {
    test_character("CHAR", true, 1, false);
}

TEST_F(ddl_metadata_test, char_wildcard) {
    test_stmt_err("CREATE TABLE T (C0 CHAR(*) PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, char_paren_wo_len) {
    test_stmt_err("CREATE TABLE T (C0 CHAR() PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, varchar_minus) {
    test_stmt_err("CREATE TABLE T (C0 VARCHAR(-1) PRIMARY KEY)", error_code::syntax_exception);
}
TEST_F(ddl_metadata_test, varchar_wo_len) {
    test_stmt_err("CREATE TABLE T (C0 VARCHAR PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, varchar_paren_wo_len_) {
    test_stmt_err("CREATE TABLE T (C0 VARCHAR() PRIMARY KEY)", error_code::syntax_exception);
}

TEST_F(ddl_metadata_test, varchar_wildcard) {
    test_character("VARCHAR(*)", false, -1, true);
}

TEST_F(ddl_metadata_test, varchar_0) {
    test_stmt_err("CREATE TABLE T (C0 VARCHAR(0) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, varchar_exceeding_limit) {
    test_stmt_err("CREATE TABLE T (C0 VARCHAR(30717) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, char_exceeding_limit) {
    test_stmt_err("CREATE TABLE T (C0 CHAR(30717) PRIMARY KEY)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(ddl_metadata_test, genpk_column_features) {
    execute_statement("CREATE TABLE T (C0 INT)");
    auto t = find_table(*db_, "T");
    ASSERT_TRUE(t);
    {
        auto c = find_column(*t, "__generated_rowid___T");
        ASSERT_TRUE(c);
        ASSERT_EQ(takatori::type::type_kind::int8, c->type().kind());
        auto features = c->features();
        EXPECT_TRUE(features.contains(yugawara::storage::column_feature::synthesized));
        EXPECT_TRUE(features.contains(yugawara::storage::column_feature::hidden));
    }
    {
        // verify non-generated column has no features
        auto c = find_column(*t, "C0");
        ASSERT_TRUE(c);
        ASSERT_EQ(takatori::type::type_kind::int4, c->type().kind());
        auto features = c->features();
        EXPECT_FALSE(features.contains(yugawara::storage::column_feature::synthesized));
        EXPECT_FALSE(features.contains(yugawara::storage::column_feature::hidden));
    }
}


}
