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
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/sequence/metadata_store.h>
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

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class generated_identity_test :
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
    std::size_t find_next_available_seq_def_id();
};

using namespace std::string_view_literals;

TEST_F(generated_identity_test, simple) {
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int generated always as identity)");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    std::vector<mock::basic_record> result{};
    execute_query("select c1 from t0", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(generated_identity_test, pk_generated) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity)");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    std::vector<mock::basic_record> result{};
    execute_query("select c1 from t0", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(generated_identity_test, use_generated_identity_as_pk) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (minvalue 0 maxvalue 1) primary key)");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    test_stmt_err("INSERT INTO t0 (c0) VALUES (2)", error_code::unique_constraint_violation_exception);
    std::vector<mock::basic_record> result{};
    execute_query("select c1 from t0", result);
    ASSERT_EQ(2, result.size());
}

TEST_F(generated_identity_test, updating_readonly_key_column) {
    execute_statement("CREATE TABLE t0 (c0 int primary key generated always as identity, c1 int)");
    // by insert statement
    test_stmt_err("INSERT INTO t0 (c0, c1) VALUES (0, 0)", error_code::restricted_operation_exception);

    // by update statement
    execute_statement("INSERT INTO t0 (c1) VALUES (10)");
    test_stmt_err("UPDATE t0 SET c0=1 WHERE c1=10", error_code::restricted_operation_exception);

    // by insert-select statement
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (100, 100)");
    test_stmt_err("INSERT INTO t0 (c0, c1) SELECT * FROM t1", error_code::restricted_operation_exception);
}

TEST_F(generated_identity_test, updating_readonly_value_column) {
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int generated always as identity)");
    // by insert statement
    test_stmt_err("INSERT INTO t0 (c0, c1) VALUES (0, 0)", error_code::restricted_operation_exception);

    // by update statement
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    test_stmt_err("UPDATE t0 SET c1=10 WHERE c0=1", error_code::restricted_operation_exception);

    // by insert-select statement
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (100, 100)");
    test_stmt_err("INSERT INTO t0 (c0, c1) SELECT * FROM t1", error_code::restricted_operation_exception);
}

TEST_F(generated_identity_test, updating_updatable_identity) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated by default as identity)");
    execute_statement("INSERT INTO t0 (c0, c1) VALUES (1, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
    }
    execute_statement("UPDATE t0 SET c1=100 WHERE c0=1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
    }
}

TEST_F(generated_identity_test, invalid_types) {
    test_stmt_err("CREATE TABLE t0 (c0 varchar generated always as identity)", error_code::type_analyze_exception);
    test_stmt_err("CREATE TABLE t0 (c0 char generated always as identity)", error_code::type_analyze_exception);
    test_stmt_err("CREATE TABLE t0 (c0 decimal generated always as identity)", error_code::type_analyze_exception);
    test_stmt_err("CREATE TABLE t0 (c0 real generated always as identity)", error_code::type_analyze_exception);
    test_stmt_err("CREATE TABLE t0 (c0 double generated always as identity)", error_code::type_analyze_exception);
}

TEST_F(generated_identity_test, default_option_for_generated_sequence) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity)");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(generated_identity_test, initial_value) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start 100))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(101)), result[0]);
    }
}

std::size_t generated_identity_test::find_next_available_seq_def_id() {
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);
    executor::sequence::metadata_store ms{*tctx->object()};
    std::size_t def_id{};
    ms.find_next_empty_def_id(def_id);
    return def_id;
}

TEST_F(generated_identity_test, drop_table_deletes_sequence) {
    auto before = find_next_available_seq_def_id();
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int generated always as identity)");
    auto after = find_next_available_seq_def_id();
    EXPECT_NE(before, after);
    execute_statement("DROP TABLE t0");
    auto after_drop = find_next_available_seq_def_id();
    EXPECT_EQ(before, after_drop);
}

TEST_F(generated_identity_test, drop_table_deletes_sequence_for_pk) {
    // same as above, but with primary key
    auto before = find_next_available_seq_def_id();
    execute_statement("CREATE TABLE t0 (c0 int)");
    auto after = find_next_available_seq_def_id();
    EXPECT_NE(before, after);
    execute_statement("DROP TABLE t0");
    auto after_drop = find_next_available_seq_def_id();
    EXPECT_EQ(before, after_drop);
}

TEST_F(generated_identity_test, drop_table_deletes_sequence_multi) {
    auto before = find_next_available_seq_def_id();
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity, c2 int generated always as "
                      "identity, c3 int generated always as identity)");
    auto after = find_next_available_seq_def_id();
    EXPECT_NE(before, after);
    execute_statement("DROP TABLE t0");
    auto after_drop = find_next_available_seq_def_id();
    EXPECT_EQ(before, after_drop);
}

TEST_F(generated_identity_test, various_options) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start 3 increment 2 minvalue 3 maxvalue 5 CYCLE))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 3", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[0]);
    }
}

TEST_F(generated_identity_test, various_options_negative_increment) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start 3 increment -2 minvalue 1 maxvalue 3 CYCLE))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 3", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(generated_identity_test, no_cycle_reach_max) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (minvalue 0 maxvalue 1 NO CYCLE))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    test_stmt_err("INSERT INTO t0 (c0) VALUES (2)", error_code::value_evaluation_exception);
}

TEST_F(generated_identity_test, no_cycle_reach_min) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start 1 increment -1 minvalue 0 maxvalue 1 NO CYCLE))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    test_stmt_err("INSERT INTO t0 (c0) VALUES (2)", error_code::value_evaluation_exception);
}

TEST_F(generated_identity_test, cycle_int4_max) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start 2147483646))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2147483646)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2147483647)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(generated_identity_test, cycle_int4_min) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int generated always as identity (start -2147483647 increment -1))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-2147483647)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-2147483648)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-1)), result[0]);
    }
}

TEST_F(generated_identity_test, cycle_int8_max) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 bigint generated always as identity (start 9223372036854775806))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(9223372036854775806)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(9223372036854775807)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[0]);
    }
}

TEST_F(generated_identity_test, cycle_int8_min) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 bigint generated always as identity (start -9223372036854775807 increment -1))");
    execute_statement("INSERT INTO t0 (c0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(-9223372036854775807)), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(std::numeric_limits<std::int64_t>::min())), result[0]);
    }
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c1 from t0 where c0 = 2", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(-1)), result[0]);
    }
}

}

