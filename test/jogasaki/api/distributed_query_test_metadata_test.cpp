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
#include <vector>
#include <gtest/gtest.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include "api_test_base.h"

namespace jogasaki::testing {

class distributed_query_test_metadata_test : public ::testing::Test, public api_test_base {
protected:
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(std::make_shared<configuration>()); }
    void TearDown() override { db_teardown(); }

    static constexpr std::string_view columns =
        "(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,2), C3 BIGINT)";

    void check_identity(std::string_view name, bool fixture) {
        auto table = db_impl()->tables()->find_table(name);
        auto index = db_impl()->tables()->find_index(name);
        ASSERT_TRUE(table);
        ASSERT_TRUE(index);
#ifdef ENABLE_DISTRIBUTED_QUERY_TEST_METADATA
        if (fixture) {
            EXPECT_EQ(8101, table->definition_id());
            EXPECT_EQ(9101, index->definition_id());
            EXPECT_EQ(table.get(), std::addressof(index->table()));
            return;
        }
#else
        (void)fixture;
#endif
        EXPECT_FALSE(table->definition_id());
        EXPECT_FALSE(index->definition_id());
    }
};

TEST_F(distributed_query_test_metadata_test, exact_table_uses_compiled_option) {
    // Keyword spelling and whitespace are irrelevant: match parsed declarations.
    execute_statement("create table C1_DUMMY_T " + std::string{columns});
    check_identity("C1_DUMMY_T", true);
    execute_statement("INSERT INTO C1_DUMMY_T VALUES (900, NULL, 12.34, 0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM C1_DUMMY_T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(900, result[0].get_value<std::int64_t>(0));
}

TEST_F(distributed_query_test_metadata_test, ordinary_table_is_unchanged) {
    execute_statement("CREATE TABLE T " + std::string{columns});
    check_identity("T", false);
}

TEST_F(distributed_query_test_metadata_test, name_prefix_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T_OTHER " + std::string{columns});
    check_identity("C1_DUMMY_T_OTHER", false);
}

TEST_F(distributed_query_test_metadata_test, different_length_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(41), C2 DECIMAL(10,2), C3 BIGINT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_scale_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,3), C3 BIGINT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_nullability_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40) NOT NULL, C2 DECIMAL(10,2), C3 BIGINT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_primary_key_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT, C1 VARCHAR(40), C2 DECIMAL(10,2), C3 BIGINT PRIMARY KEY)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_precision_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(11,2), C3 BIGINT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_column_name_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,2), OTHER BIGINT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, different_column_type_is_not_a_match) {
    execute_statement("CREATE TABLE C1_DUMMY_T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,2), C3 INT)");
    check_identity("C1_DUMMY_T", false);
}

TEST_F(distributed_query_test_metadata_test, prepared_create_can_be_reused_after_drop) {
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("CREATE TABLE C1_DUMMY_T " + std::string{columns}, prepared));
    execute_statement(prepared);
    check_identity("C1_DUMMY_T", true);
    execute_statement("DROP TABLE C1_DUMMY_T");
    execute_statement(prepared);
    check_identity("C1_DUMMY_T", true);
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
}

TEST_F(distributed_query_test_metadata_test, definition_ids_and_rows_survive_restart) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP() << "requires persistent storage";
    execute_statement("CREATE TABLE C1_DUMMY_T " + std::string{columns});
    execute_statement("INSERT INTO C1_DUMMY_T VALUES (900, 'persisted', 12.34, 0)");
    check_identity("C1_DUMMY_T", true);
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    check_identity("C1_DUMMY_T", true);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM C1_DUMMY_T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(900, result[0].get_value<std::int64_t>(0));
}

} // namespace jogasaki::testing
