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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/api/error_info.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
using jogasaki::api::impl::get_impl;

class api_test :
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
        auto& impl = jogasaki::api::impl::get_impl(*db_);
        jogasaki::utils::add_test_tables(*impl.tables());
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
    }

    void TearDown() override {
        db_teardown();
    }

    std::shared_ptr<error::error_info> execute(api::transaction_handle tx, api::executable_statement& stmt);
};

using namespace std::string_view_literals;

TEST_F(api_test, syntax_error) {
    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_parse_error, get_impl(*db_).create_executable("AAA", stmt, info));
    EXPECT_EQ(error_code::syntax_exception, info->code());
    std::cerr << info->message() << std::endl;
}

TEST_F(api_test, missing_table) {
    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_compiler_error, get_impl(*db_).create_executable("select * from dummy", stmt, info));
    EXPECT_EQ(error_code::symbol_analyze_exception, info->code());
    std::cerr << info->message() << std::endl;
}

TEST_F(api_test, invalid_column_name) {
    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_compiler_error, get_impl(*db_).create_executable("INSERT INTO T0(dummy) VALUES(1)", stmt, info));
    EXPECT_EQ(error_code::symbol_analyze_exception, info->code());
    std::cerr << info->message() << std::endl;
}

TEST_F(api_test, inconsistent_type_in_write) {
    // old compiler made this error, while new compiler can pass to jogasaki in order to let jogasaki try conversion
    // analyzer option cast_literals_in_context = false can be used to keep the old behavior
    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::ok, get_impl(*db_).create_executable("INSERT INTO T0(C0) VALUES('X')", stmt, info));
    auto tx = utils::create_transaction(*db_);
    auto err = execute(*tx, *stmt);
    ASSERT_EQ(error_code::value_evaluation_exception, err->code());
    ASSERT_EQ(status::ok, tx->abort());
}

TEST_F(api_test, inconsistent_type_in_query) {
    std::unique_ptr<api::executable_statement> stmt{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_compiler_error, get_impl(*db_).create_executable("select C1 from T0 where C1='X'", stmt, info));
    EXPECT_EQ(error_code::type_analyze_exception, info->code());
    std::cerr << info->message() << std::endl;
}

std::shared_ptr<error::error_info> api_test::execute(api::transaction_handle tx, api::executable_statement& stmt) {
    std::shared_ptr<error::error_info> err{};
    std::unique_ptr<api::result_set> result{};
    std::shared_ptr<request_statistics> stats{};
    executor::execute(get_impl(*db_), get_transaction_context(tx), stmt, result, err, stats);
    if(err) {
        std::cerr << *err << std::endl;
    }
    return err;
}

TEST_F(api_test, primary_key_violation) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = utils::create_transaction(*db_);
    auto err = execute(*tx, *stmt);
    ASSERT_EQ(error_code::unique_constraint_violation_exception, err->code());
    ASSERT_EQ(status::ok, tx->abort());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_DOUBLE_EQ(10.0, rec.ref().get_value<double>(rec.record_meta()->value_offset(1)));
}

TEST_F(api_test, primary_key_violation_in_same_tx) {
    std::unique_ptr<api::executable_statement> stmt0{};
    std::unique_ptr<api::executable_statement> stmt1{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt0));
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt1));
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->execute(*stmt0));
    auto err = execute(*tx, *stmt1);
    ASSERT_EQ(error_code::unique_constraint_violation_exception, err->code());
    ASSERT_EQ(status::ok, tx->abort());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    if (jogasaki::kvs::implementation_id() == "memory") {
        // sharksfin memory doesn't support rollback
        ASSERT_EQ(1, result.size());
        return;
    }
    ASSERT_EQ(0, result.size());
}

TEST_F(api_test, violate_not_null_constraint_by_insert) {
    {
        // insert null to non-primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO NON_NULLABLES (K0, C1, C2, C3, C4) VALUES (1, 100, 1000.0, 10000.0, '111')", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        // insert null to primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO NON_NULLABLES (C0, C1, C2, C3, C4) VALUES (10, 100, 1000.0, 10000.0, '111')", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM NON_NULLABLES", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(api_test, violate_not_null_constraint_by_update) {
    // update non-pk key and set null
    execute_statement("INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, 10, 100, 1000.0, 10000.0, '111')");
    {
        // update to null for non-primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE NON_NULLABLES SET C0=NULL WHERE K0=1", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }

    if (jogasaki::kvs::implementation_id() != "memory") {
        // sharksfin-memory doesn't support rollback on abort, so the result records are undefined
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM NON_NULLABLES", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(api_test, violate_not_null_pk_constraint_by_update) {
    // update pk key and set null - separated from testcase above since sharksfin-memory abort cannot rollback everything
    execute_statement( "INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, 10, 100, 1000.0, 10000.0, '111')");
    {
        // update to null for primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE NON_NULLABLES SET K0=NULL WHERE K0=1", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
    if (jogasaki::kvs::implementation_id() != "memory") {
        // sharksfin-memory doesn't support rollback on abort, so the result records are undefined
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM NON_NULLABLES", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(api_test, violate_not_null_constraint_by_insert_host_variable) {
    {
        // insert null to non-primary key column
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
        };
        ASSERT_EQ(status::ok, db_->prepare("INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, :p0, 100, 1000.0, 10000.0, '111')", variables, prepared));

        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }
    {
        // insert null to primary key column
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
        };
        ASSERT_EQ(status::ok, db_->prepare("INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (:p0, 10, 100, 1000.0, 10000.0, '111')", variables, prepared));

        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM NON_NULLABLES", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(api_test, violate_not_null_constraint_by_update_host_variable_non_pkey) {
    execute_statement( "INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, 10, 100, 1000.0, 10000.0, '111')");
    {
        // update to null for non-primary key column
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
        };
        ASSERT_EQ(status::ok, db_->prepare("UPDATE NON_NULLABLES SET C0=:p0 WHERE K0=1", variables, prepared));

        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }
}

TEST_F(api_test, violate_not_null_constraint_by_update_host_variable_pkey) {
    execute_statement( "INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, 10, 100, 1000.0, 10000.0, '111')");
    {
        // update to null for primary key column
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
        };
        ASSERT_EQ(status::ok, db_->prepare("UPDATE NON_NULLABLES SET K0=:p0 WHERE K0=1", variables, prepared));

        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(error_code::not_null_constraint_violation_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }
}

TEST_F(api_test, resolve_place_holder_with_null) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p1", api::field_type_kind::int8},
        {"p2", api::field_type_kind::float8},
    };
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:p1, :p2)", variables, prepared));
    {
        auto tx = utils::create_transaction(*db_);
        auto ps = api::create_parameter_set();
        ps->set_int8("p1", 1);
        ps->set_null("p2");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_TRUE(rec.ref().is_null(rec.record_meta()->nullity_offset(1)));
}

TEST_F(api_test, dump_load) {
    execute_statement( "DELETE FROM T0");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
    std::stringstream ss{};
    ASSERT_EQ(status::ok, db_->dump(ss, "T0", 0));
    execute_statement( "DELETE FROM T0");
    wait_epochs();
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(0, result.size());
    ASSERT_EQ(status::ok, db_->load(ss, "T0", 0));
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(10.0, result[0].ref().get_value<double>(meta->value_offset(1)));
    EXPECT_EQ(2, result[1].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(20.0, result[1].ref().get_value<double>(meta->value_offset(1)));
}

TEST_F(api_test, select_update_delete_for_missing_record) {
    // verify no error even if target records are missing
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(0, result.size());
    execute_statement("DELETE FROM T0 WHERE C0=1");
    execute_statement("UPDATE T0 SET C1=1.0 WHERE C0=1");
}

TEST_F(api_test, resolve_host_variable) {
    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("p0", api::field_type_kind::int8);

    execute_statement("DELETE FROM T0");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 1);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 = :p0", variables, *ps, result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 4);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 = :p0", variables, *ps, result);
        ASSERT_EQ(0, result.size());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 = :p0", variables, *ps, result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(api_test, scan_with_host_variable) {
    // test scan op, range keys are host vars TODO move to scan op UT rather than using SQL
    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("p0", api::field_type_kind::int8);
    variables.emplace("p1", api::field_type_kind::int8);

    execute_statement("DELETE FROM T0");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (20,20.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (10,10.0)");
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 15);
        ps->set_int8("p1", 25);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 > :p0 AND C0 < :p1", variables, *ps, result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_EQ(20, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 15);
        ps->set_null("p1");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 > :p0 AND C0 < :p1", variables, *ps, result);
        ASSERT_EQ(0, result.size());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        ps->set_int8("p1", 15);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 > :p0 AND C0 < :p1", variables, *ps, result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(api_test, scan_with_host_variable_with_nulls) {
    // verify comparison with null
    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("p1", api::field_type_kind::int4);

    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("create index i on t(c1)");
    execute_statement("INSERT INTO t VALUES (0, null),(1, 1)");
    {
        auto ps = api::create_parameter_set();
        ps->set_null("p1");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t WHERE c1 <= :p1", variables, *ps, result);
        ASSERT_EQ(0, result.size());
    }
}


TEST_F(api_test, join_find_with_key_null) {
    // test join_find op, key contains null TODO move to join_find op UT rather than using SQL
    execute_statement("DELETE FROM T0");
    execute_statement("DELETE FROM T1");
    execute_statement("INSERT INTO T1 (C0) VALUES (1)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (20,20.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (10,10.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 JOIN T1 ON T0.C0 = T1.C1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(api_test, host_variable_same_name_different_type) {
    std::unordered_map<std::string, api::field_type_kind> variables1{{"p0", api::field_type_kind::int8}};
    std::unordered_map<std::string, api::field_type_kind> variables2{{"p0", api::field_type_kind::float8}};

    execute_statement("DELETE FROM T0");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 1);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C0 = :p0", variables1, *ps, result);
        ASSERT_EQ(1, result.size());
        auto& rec= result[0];
        EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_float8("p0", 20.0);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 WHERE C1 = :p0", variables2, *ps, result);
        ASSERT_EQ(1, result.size());
        auto& rec= result[0];
        EXPECT_EQ(2, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    }
}

TEST_F(api_test, extra_parameter_not_used_by_stmt) {
    // WARNING should be shown
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(0, 0)", prepared));
    {
        auto tx = utils::create_transaction(*db_);
        auto ps = api::create_parameter_set();
        ps->set_int8("unused1", 1);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }

    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("unused1", api::field_type_kind::int8);

    api::statement_handle query{};
    ASSERT_EQ(status::ok, db_->prepare("SELECT C0, C1 FROM T0", query));
    {
        auto tx = utils::create_transaction(*db_);
        auto ps = api::create_parameter_set();
        ps->set_int8("unused1", 1);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(query, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
        ASSERT_EQ(status::ok,db_->destroy_statement(query));
    }
}

TEST_F(api_test, undefined_host_variables) {
    std::unordered_map<std::string, api::field_type_kind> variables{};
    api::statement_handle prepared{};

    std::unique_ptr<api::executable_statement> stmt{};

    {
        std::shared_ptr<error::error_info> info{};
        ASSERT_EQ(status::err_compiler_error, get_impl(*db_).prepare("INSERT INTO T0 (C0, C1) VALUES(:undefined0, 0)", variables, prepared, info));
        EXPECT_EQ(error_code::symbol_analyze_exception, info->code());
        std::cerr << info->message() << std::endl;
    }
    {
        std::shared_ptr<error::error_info> info{};
        api::statement_handle query{};
        ASSERT_EQ(status::err_compiler_error, get_impl(*db_).prepare("SELECT C0, C1 FROM T0 WHERE C0=:undefined0", variables, query, info));
        EXPECT_EQ(error_code::symbol_analyze_exception, info->code());
        std::cerr << info->message() << std::endl;
    }
}

TEST_F(api_test, unresolved_parameters) {
    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("unresolved0", api::field_type_kind::int8);
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:unresolved0, 0)", variables, prepared));
    {
        std::shared_ptr<error::error_info> info{};
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::err_unresolved_host_variable, get_impl(*db_).resolve(prepared, std::shared_ptr{std::move(ps)}, exec, info));
        EXPECT_EQ(error_code::unresolved_placeholder_exception, info->code());
        std::cerr << info->message() << std::endl;
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }
    api::statement_handle query{};
    ASSERT_EQ(status::ok, db_->prepare("SELECT C0, C1 FROM T0 WHERE C0=:unresolved0", variables, query));
    {
        std::shared_ptr<error::error_info> info{};
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::err_unresolved_host_variable, get_impl(*db_).resolve(query, std::shared_ptr{std::move(ps)}, exec, info));
        EXPECT_EQ(error_code::unresolved_placeholder_exception, info->code());
        std::cerr << info->message() << std::endl;
        ASSERT_EQ(status::ok,db_->destroy_statement(query));
    }
}

TEST_F(api_test, char_data_too_long_insert) {
    execute_statement("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (0,'00000', '11111')");
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (1,'00000X', '11111')", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::value_too_long_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (2,'00000', '111111')", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_EQ(error_code::value_too_long_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
}

// char_data_too_long_update is separated to two testcases because sharksfin-memory rollback fails and it affects running second testcase
TEST_F(api_test, char_data_too_long_update_vc) {
    execute_statement("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (0,'00000', '11111')");
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE CHAR_TAB SET VC='00000X' WHERE C0=0", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_TRUE(err);
        ASSERT_EQ(error_code::value_too_long_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
}

TEST_F(api_test, char_data_too_long_update_ch) {
    execute_statement("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (0,'00000', '11111')");
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE CHAR_TAB SET CH='111111' WHERE C0=0", stmt));
        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *stmt);
        ASSERT_TRUE(err);
        ASSERT_EQ(error_code::value_too_long_exception, err->code());
        ASSERT_EQ(status::ok, tx->abort());
    }
}

TEST_F(api_test, bad_wp_storage_name) {
    api::transaction_handle tx{};
    std::shared_ptr<api::error_info> info{};
    auto res = get_impl(*db_).do_create_transaction(tx, api::transaction_option{false, true, {"DUMMY_STORAGE"}}, info);
    ASSERT_TRUE(info);
    std::cerr << *info;
    ASSERT_EQ(error_code::target_not_found_exception, info->code());
}

TEST_F(api_test, bad_ra_storage_name) {
    api::transaction_handle tx{};
    std::shared_ptr<api::error_info> info{};
    auto res = get_impl(*db_).do_create_transaction(tx, api::transaction_option{false, true, {""}, "", {"DUMMY_STORAGE"}}, info);
    ASSERT_TRUE(info);
    std::cerr << *info;
    ASSERT_EQ(error_code::target_not_found_exception, info->code());
}

TEST_F(api_test, empty_result) {
    // we don't use not_found error even when query result is empty
    {
        // scan op
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::ok, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->commit());
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(0, result.size());
    }
    {
        // find op
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 WHERE C0=0", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::ok, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->commit());
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(api_test, column_name) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("select C0, C1 from T0", stmt));
    EXPECT_EQ("C0", stmt->meta()->field_name(0));
    EXPECT_EQ("C1", stmt->meta()->field_name(1));

    api::statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare("select C0, C1 from T0", handle));
    EXPECT_EQ("C0", handle.meta()->field_name(0));
    EXPECT_EQ("C1", handle.meta()->field_name(1));
}

TEST_F(api_test, empty_column_name) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("select max(C0) from T0", stmt));
    EXPECT_FALSE(stmt->meta()->field_name(0));

    api::statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare("select min(C1) from T0", handle));
    EXPECT_FALSE(handle.meta()->field_name(0));
}

TEST_F(api_test, err_inactive_tx) {
    std::unique_ptr<api::executable_statement> stmt0{};
    std::unique_ptr<api::executable_statement> stmt1{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt0));
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt1));
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->execute(*stmt0));
    {
        auto err = execute(*tx, *stmt1);
        ASSERT_EQ(error_code::unique_constraint_violation_exception, err->code());
    }
    {
        auto err = execute(*tx, *stmt0);
        ASSERT_EQ(error_code::inactive_transaction_exception, err->code());
    }
    ASSERT_EQ(status::ok, tx->abort());
}



TEST_F(api_test, err_querying_generated_rowid) {
    // generated rowid is invisible even if it's renamed
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    api::statement_handle handle{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_parse_error, get_impl(*db_).prepare("SELECT __generated_rowid___T as rowid, C0 FROM T ORDER BY C0", handle, info));
    EXPECT_EQ(error_code::syntax_exception, info->code());
    std::cerr << info->message() << std::endl;
}

TEST_F(api_test, err_insert_lack_of_values) {
    api::statement_handle handle{};
    std::shared_ptr<error::error_info> info{};
    ASSERT_EQ(status::err_compiler_error, get_impl(*db_).prepare("INSERT INTO T0(C0, C1) VALUES (1)", handle, info));
    EXPECT_EQ(error_code::analyze_exception, info->code());
}

bool contains(std::vector<std::string> const& v, std::string_view s) {
    auto it = v.begin();
    while(it != v.end()) {
        if(*it == s) {
            return true;
        }
        ++it;
    }
    return false;
}

TEST_F(api_test, list_tables) {
    execute_statement("create table TT0 (C0 int)");
    execute_statement("create table TT1 (C0 int)");
    execute_statement("create index I0 on TT0 (C0)");
    execute_statement("create index I1 on TT1 (C0)");
    std::vector<std::string> simple_names{};
    ASSERT_EQ(status::ok, db_->list_tables(simple_names));
    ASSERT_TRUE(contains(simple_names, "TT0"));
    ASSERT_TRUE(contains(simple_names, "TT1"));
    ASSERT_FALSE(contains(simple_names, "I0"));
    ASSERT_FALSE(contains(simple_names, "I1"));
}

// TODO auto generate index name when omitted
TEST_F(api_test, create_index_wo_name) {
    execute_statement("create table TT0 (C0 int)");
    test_stmt_err("create index on TT0 (C0)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(api_test, create_table_if_not_exsits) {
    execute_statement("create table T (C0 int)");
    execute_statement("create table if not exists T (C0 int)");
    execute_statement("drop table T");
}

TEST_F(api_test, drop_table_if_exists) {
    execute_statement("drop table if exists T");
    execute_statement("create table T (C0 int)");
    execute_statement("drop table if exists T");
    execute_statement("create table T (C0 int)");
}

TEST_F(api_test, create_index_if_not_exsits) {
    execute_statement("create table T (C0 int)");
    execute_statement("create index I0 on T (C0)");
    execute_statement("create index if not exists I0 on T (C0)");
    execute_statement("drop index I0");
}

TEST_F(api_test, drop_index_if_exists) {
    execute_statement("drop index if exists I0");
    execute_statement("create table T (C0 int)");
    execute_statement("create index I0 on T (C0)");
    execute_statement("drop index if exists I0");
    execute_statement("create index I0 on T (C0)");
}

// TODO enable after fix for 702
TEST_F(api_test, DISABLED_use_insert_prepared_stmt_after_table_dropped) {
    execute_statement("create table t (c0 int primary key)");

    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
    };
    ASSERT_EQ(status::ok, db_->prepare("insert into t values (:p0)", variables, prepared));

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 10);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(status::ok, tx->commit());
    }

    execute_statement("drop table t");

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 20);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(status::ok, tx->commit());
    }
    ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
}

// TODO enable after fix for 702
TEST_F(api_test, DISABLED_use_select_prepared_stmt_after_table_dropped) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (10)");

    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
    };
    ASSERT_EQ(status::ok, db_->prepare("select * from t where c0 = :p0", variables, prepared));

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 10);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(status::ok, tx->commit());
    }

    execute_statement("drop table t");

    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 10);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));

        auto tx = utils::create_transaction(*db_);
        auto err = execute(*tx, *exec);
        ASSERT_EQ(status::ok, tx->commit());
    }
    ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
}



}
