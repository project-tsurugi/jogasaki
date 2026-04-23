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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

class sql_find_secondary_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    bool has_find(std::string_view query);
    bool uses_secondary(std::string_view query);
    bool has_filter(std::string_view query);

    void run_index(std::string const& dir);
    void run_index_with_null(std::string const& dir);
    void run_composite_index(std::string const& c1_dir, std::string const& c2_dir);
};

using namespace std::string_view_literals;

static bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_find_secondary_test::has_find(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "find") && ! contains(plan, "join_find");
}

bool sql_find_secondary_test::uses_secondary(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"i1\"");
}

bool sql_find_secondary_test::has_filter(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"filter\"");
}

void sql_find_secondary_test::run_index(std::string const& dir) {
    // data: c1 has matching rows, one extra non-matching row, and a NULL row
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1 " + dir + ")");
    execute_statement("INSERT INTO t VALUES (10, 1), (20, 2), (30, 1), (40, NULL)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 = 1";
    EXPECT_TRUE(has_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(30, 1)), result[1]);
}

void sql_find_secondary_test::run_index_with_null(std::string const& dir) {
    // verify that NULL values in the indexed column are not matched by equality find
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1 " + dir + ")");
    execute_statement("INSERT INTO t VALUES (10, NULL), (11, 1), (12, NULL)");

    auto query = "SELECT c0, c1 FROM t WHERE c1 = 1";
    EXPECT_TRUE(has_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(11, 1)), result[0]);
}

void sql_find_secondary_test::run_composite_index(
    std::string const& c1_dir, std::string const& c2_dir) {
    // data includes rows matching neither key, only one key each, and NULL in each key column
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1 " + c1_dir + ", c2 " + c2_dir + ")");
    execute_statement(
        "INSERT INTO t VALUES "
        "(10, 1, 10), (20, 1, 20), (30, 2, 10), (40, NULL, 10), (50, 1, NULL)"
    );

    auto query = "SELECT c0, c1, c2 FROM t WHERE c1 = 1 AND c2 = 10";
    EXPECT_TRUE(has_find(query));
    EXPECT_TRUE(uses_secondary(query));
    EXPECT_TRUE(! has_filter(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(10, 1, 10)), result[0]);
}

TEST_F(sql_find_secondary_test, index) {
    run_index("ASC");
}

TEST_F(sql_find_secondary_test, desc) {
    run_index("DESC");
}

TEST_F(sql_find_secondary_test, with_null) {
    run_index_with_null("ASC");
}

TEST_F(sql_find_secondary_test, with_null_desc) {
    run_index_with_null("DESC");
}

TEST_F(sql_find_secondary_test, composite_key_asc_asc) {
    run_composite_index("ASC", "ASC");
}

TEST_F(sql_find_secondary_test, composite_key_asc_desc) {
    run_composite_index("ASC", "DESC");
}

TEST_F(sql_find_secondary_test, composite_key_desc_asc) {
    run_composite_index("DESC", "ASC");
}

TEST_F(sql_find_secondary_test, composite_key_desc_desc) {
    run_composite_index("DESC", "DESC");
}

} // namespace jogasaki::testing
