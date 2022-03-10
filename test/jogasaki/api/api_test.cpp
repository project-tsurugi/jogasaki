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
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "../test_utils/temporary_folder.h"
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/kvs/id.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

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

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }

};

using namespace std::string_view_literals;

TEST_F(api_test, syntax_error) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_parse_error, db_->create_executable("AAA", stmt));
}

TEST_F(api_test, missing_table) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_translator_error, db_->create_executable("select * from dummy", stmt));
}

TEST_F(api_test, invalid_column_name) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_translator_error, db_->create_executable("INSERT INTO T0(dummy) VALUES(1)", stmt));
}

TEST_F(api_test, inconsistent_type_in_write) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_compiler_error, db_->create_executable("INSERT INTO T0(C0) VALUES('X')", stmt));
}

TEST_F(api_test, inconsistent_type_in_query) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_compiler_error, db_->create_executable("select C1 from T0 where C1='X'", stmt));
}

TEST_F(api_test, primary_key_violation) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::err_already_exists, tx->execute(*stmt));
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
    ASSERT_EQ(status::err_already_exists, tx->execute(*stmt1));
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
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        // insert null to primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO NON_NULLABLES (C0, C1, C2, C3, C4) VALUES (10, 100, 1000.0, 10000.0, '111')", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM NON_NULLABLES", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(api_test, DISABLED_violate_not_null_constraint_by_update) {
    execute_statement( "INSERT INTO NON_NULLABLES (K0, C0, C1, C2, C3, C4) VALUES (1, 10, 100, 1000.0, 10000.0, '111')");
    {
        // update to null for non-primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE NON_NULLABLES SET C0=NULL WHERE K0=1", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        // update to null for primary key column
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("UPDATE NON_NULLABLES SET K0=NULL WHERE K0=1", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM NON_NULLABLES", result);
    ASSERT_EQ(0, result.size());
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
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*exec));
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
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*exec));
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
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*exec));
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
        ASSERT_EQ(status::err_integrity_constraint_violation, tx->execute(*exec));
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

    execute_statement( "DELETE FROM T0");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
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

    execute_statement( "DELETE FROM T0");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (20,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (10,10.0)");
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

TEST_F(api_test, join_find_with_key_null) {
    // test join_find op, key contains null TODO move to join_find op UT rather than using SQL
    execute_statement( "DELETE FROM T0");
    execute_statement( "DELETE FROM T1");
    execute_statement( "INSERT INTO T1 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (20,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (10,10.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 JOIN T1 ON T0.C0 = T1.C1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(api_test, host_variable_same_name_different_type) {
    std::unordered_map<std::string, api::field_type_kind> variables1{{"p0", api::field_type_kind::int8}};
    std::unordered_map<std::string, api::field_type_kind> variables2{{"p0", api::field_type_kind::float8}};

    execute_statement( "DELETE FROM T0");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
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
    ASSERT_EQ(status::err_translator_error, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:undefined0, 0)", variables, prepared));
    api::statement_handle query{};
    ASSERT_EQ(status::err_translator_error, db_->prepare("SELECT C0, C1 FROM T0 WHERE C0=:undefined0", variables, query));
}

TEST_F(api_test, unresolved_parameters) {
    std::unordered_map<std::string, api::field_type_kind> variables{};
    variables.emplace("unresolved0", api::field_type_kind::int8);
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:unresolved0, 0)", variables, prepared));
    {
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::err_unresolved_host_variable,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    }
    api::statement_handle query{};
    ASSERT_EQ(status::ok, db_->prepare("SELECT C0, C1 FROM T0 WHERE C0=:unresolved0", variables, query));
    {
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::err_unresolved_host_variable,db_->resolve(query, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,db_->destroy_statement(query));
    }
}

TEST_F(api_test, char_data_too_long) {
    execute_statement( "INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (0,'00000', '11111')");
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (1,'00000X', '11111')", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_type_mismatch, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }
    {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO CHAR_TAB (C0, VC, CH) VALUES (2,'00000', '111111')", stmt));
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_type_mismatch, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->abort());
    }
}

TEST_F(api_test, bad_wp_storage_name) {
    api::transaction_handle tx{};
    ASSERT_NE(status::ok, db_->create_transaction(tx, api::transaction_option{false, true, {"DUMMY_STORAGE"}}));
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

}
