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
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/secondary_index.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;

using takatori::decimal::triple;

using kind = meta::field_type_kind;
using api::impl::get_impl;

/**
 * @brief tests on the descending key elements of the secondary index
 * @details a secondary index key element can be declared as descending, which affects the way the
 * key is encoded into the storage. This test covers the scenarios where the descending key element
 * is combined with the other properties of the index such as the origin of the column, the arity of
 * the index, the nullability and the column type, and verifies both that the DML operations keep
 * the index consistent and that the entries are actually stored in descending order.
 */
class secondary_index_desc_test :
    public ::testing::Test,
    public api_test_base {

public:
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

    bool uses_secondary(std::string_view query, std::string_view index_name);

    /**
     * @brief retrieve the entries of the secondary index "I1" in the storage order
     */
    template <class SecondaryKey, class PrimaryKey>
    auto secondary_entries(SecondaryKey const& secondary_key, PrimaryKey const& primary_key) {
        return utils::get_secondary_entries(
            *get_impl(*db_).kvs_db(),
            *get_impl(*db_).tables()->find_index("T"),
            *get_impl(*db_).tables()->find_index("I1"),
            secondary_key,
            primary_key);
    }
};

static bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

/**
 * @details verifying this ensures the assertions on the query result really exercise the secondary
 * index rather than silently falling back to the primary index. Note that the planner picks the
 * primary index when the query condition determines the primary key uniquely, so the conditions
 * used with this function are designed to leave the primary key undetermined.
 */
bool secondary_index_desc_test::uses_secondary(
    std::string_view query,
    std::string_view index_name
) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(
        plan, R"("kind":"index","table":"T","simple_name":")" + std::string{index_name} + R"(")");
}

// ---------------------------------------------------------------------------
// the origin of the descending column
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_on_non_key_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_on_primary_key_column) {
    // regression: the primary key column is implicitly ascending, so the engine must not require
    // the direction to match when it resolves the index key element to the primary key column.
    // The planner uses the primary index for the query below because the condition determines the
    // primary key uniquely, so the secondary index content is verified via its raw entries.
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C0 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C1 FROM T WHERE C0 = 2";
        // pin the premise above - the entry assertions below are the ones covering the secondary
        // index, so they would lose their meaning silently if the planner started using it here
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), result[0]);
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(2, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), entries[1].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_trailing_primary_key_column_only) {
    // the index refers to a primary key column, and the condition on it leaves the rest of the
    // primary key undetermined, so the secondary index is actually used for the query
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C2 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_on_composite_primary_key_columns) {
    // the index covers the whole primary key, so the planner always prefers the primary index and
    // the secondary index content is verified via its raw entries
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C0 DESC, C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C2 FROM T WHERE C0 = 2 AND C1 = 20";
        // pin the premise above - the entry assertions below are the ones covering the secondary
        // index, so they would lose their meaning silently if the planner started using it here
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[0]);
    }
    {
        auto entries = secondary_entries(
            (create_nullable_record<kind::int4, kind::int4>()),
            (create_nullable_record<kind::int4, kind::int4>()));
        ASSERT_EQ(2, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 20)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), entries[1].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_primary_key_column_not_covered_by_primary_key) {
    // unlike desc_on_primary_key_column, the index key contains a non-key column, so the index is
    // not covered by the primary key and the query really goes through the secondary index. This
    // exercises the read path for a descending key element resolved to a primary key column.
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC, C0 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 10)");
    execute_statement("INSERT INTO T VALUES(3, 30)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 30";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    {
        // both key elements are descending, and the primary key column takes part in the order
        auto entries = secondary_entries(
            (create_nullable_record<kind::int4, kind::int4>()),
            create_nullable_record<kind::int4>());
        ASSERT_EQ(3, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(30, 3)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 2)), entries[1].first);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), entries[2].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_composite_primary_key_columns_not_covered_by_primary_key) {
    // same as above for the composite primary key case covered by
    // desc_on_composite_primary_key_columns
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C2 DESC, C0 DESC, C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C2 = 200";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        // all the key elements are descending, including the primary key columns
        auto entries = secondary_entries(
            (create_nullable_record<kind::int4, kind::int4, kind::int4>()),
            (create_nullable_record<kind::int4, kind::int4>()));
        ASSERT_EQ(2, entries.size());
        EXPECT_EQ(
            (create_nullable_record<kind::int4, kind::int4, kind::int4>(200, 2, 20)),
            entries[0].first);
        EXPECT_EQ(
            (create_nullable_record<kind::int4, kind::int4, kind::int4>(100, 1, 10)),
            entries[1].first);
    }
}

// ---------------------------------------------------------------------------
// mixing descending and ascending key elements
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_mixed_with_asc) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1, C2 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 10, 200)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 10 AND C2 = 200";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_asc_alternating_over_key_and_non_key_columns) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, C3 INT, PRIMARY KEY(C0, C1))");
    // the directions alternate while the origin of the columns also alternates
    execute_statement("CREATE INDEX I1 ON T (C2 DESC, C0, C3 DESC, C1)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100, 1000)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200, 2000)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C1 FROM T WHERE C2 = 200 AND C0 = 2 AND C3 = 2000";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_index_together_with_asc_index) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("CREATE INDEX I2 ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    // both of the indices must be maintained on delete
    execute_statement("DELETE FROM T WHERE C0 = 2");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), entries[0].first);
    }
}

// ---------------------------------------------------------------------------
// index maintenance with descending key elements
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_index_updated_on_index_key_update) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("UPDATE T SET C1 = 20 WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 10";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), entries[0].first);
    }
}

TEST_F(secondary_index_desc_test, desc_index_updated_on_primary_key_update) {
    // the index is on the whole primary key, so the query goes through the primary index and the
    // secondary index content is verified via its raw entries
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C0 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("UPDATE T SET C0 = 2 WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C1 FROM T WHERE C0 = 1";
        // pin the premise above - the entry assertions below are the ones covering the secondary
        // index, so they would lose their meaning silently if the planner started using it here
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C1 FROM T WHERE C0 = 2";
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), entries[0].first);
    }
}

TEST_F(secondary_index_desc_test, desc_index_updated_on_primary_key_update_not_covered_by_pk) {
    // unlike desc_index_updated_on_primary_key_update, the index is not covered by the primary key,
    // so the updated entry is verified through a query that really uses the secondary index
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC, C0 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("UPDATE T SET C0 = 2 WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 10";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto entries = secondary_entries(
            (create_nullable_record<kind::int4, kind::int4>()),
            create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 2)), entries[0].first);
    }
}

TEST_F(secondary_index_desc_test, desc_index_entry_removed_on_delete) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    execute_statement("DELETE FROM T WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 10";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), entries[0].second);
    }
}

// ---------------------------------------------------------------------------
// the entries are actually encoded in the descending order
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_entries_stored_in_descending_order) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 30)");
    execute_statement("INSERT INTO T VALUES(3, 20)");
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(3, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(30)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), entries[1].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), entries[2].first);
    }
}

TEST_F(secondary_index_desc_test, asc_entries_stored_in_ascending_order) {
    // the counterpart of the test above, to make sure the order really comes from the direction
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 30)");
    execute_statement("INSERT INTO T VALUES(3, 20)");
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(3, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), entries[1].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(30)), entries[2].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_primary_key_column_stored_in_descending_order) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C0 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(3, 30)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(3, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), entries[1].first);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), entries[2].first);
    }
}

// ---------------------------------------------------------------------------
// nullability
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_index_key_with_null) {
    // IS NULL is not turned into a secondary index lookup, so the entries are verified directly
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T (C0) VALUES(1)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 IS NULL";
        // pin the premise above - IS NULL falls back to a scan on the primary index
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int4>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(2, entries.size());
    }
}

TEST_F(secondary_index_desc_test, desc_index_key_updated_to_null) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("UPDATE T SET C1 = NULL WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 10";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 IS NULL";
        // IS NULL falls back to a scan on the primary index, so it does not exercise the index
        EXPECT_FALSE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

// ---------------------------------------------------------------------------
// column types
// ---------------------------------------------------------------------------

TEST_F(secondary_index_desc_test, desc_on_bigint_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 BIGINT)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 100000000000)");
    execute_statement("INSERT INTO T VALUES(2, 200000000000)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 200000000000";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto entries = secondary_entries(
            create_nullable_record<kind::int8>(), create_nullable_record<kind::int4>());
        ASSERT_EQ(2, entries.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(200000000000)), entries[0].first);
        EXPECT_EQ((create_nullable_record<kind::int8>(100000000000)), entries[1].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_varchar_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 VARCHAR(10))");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 'AAA')");
    execute_statement("INSERT INTO T VALUES(2, 'CCC')");
    execute_statement("INSERT INTO T VALUES(3, 'BBB')");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 'CCC'";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto varchar10 = std::tuple{meta::character_type(true, 10)};
        auto entries = secondary_entries(
            typed_nullable_record<kind::character>(varchar10, accessor::text{}),
            create_nullable_record<kind::int4>());
        ASSERT_EQ(3, entries.size());
        EXPECT_EQ((typed_nullable_record<kind::character>(varchar10, accessor::text{"CCC"})),
            entries[0].first);
        EXPECT_EQ((typed_nullable_record<kind::character>(varchar10, accessor::text{"BBB"})),
            entries[1].first);
        EXPECT_EQ((typed_nullable_record<kind::character>(varchar10, accessor::text{"AAA"})),
            entries[2].first);
    }
}

TEST_F(secondary_index_desc_test, desc_on_decimal_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 DECIMAL(5,2))");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, 1.10)");
    execute_statement("INSERT INTO T VALUES(2, 3.30)");
    execute_statement("INSERT INTO T VALUES(3, 2.20)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = 3.30";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_on_date_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 DATE)");
    execute_statement("CREATE INDEX I1 ON T (C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, DATE'2000-01-01')");
    execute_statement("INSERT INTO T VALUES(2, DATE'2000-03-03')");
    execute_statement("INSERT INTO T VALUES(3, DATE'2000-02-02')");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C1 = DATE'2000-03-03'";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_desc_test, desc_on_time_column) {
    // the original issue was reported with a TIME column in the primary key
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 TIME NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C2, C1 DESC)");
    execute_statement("INSERT INTO T VALUES(1, TIME'00:00:10', 100)");
    execute_statement("INSERT INTO T VALUES(1, TIME'00:00:20', 200)");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C2 = 200 AND C1 = TIME'00:00:20'";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

}  // namespace jogasaki::testing
