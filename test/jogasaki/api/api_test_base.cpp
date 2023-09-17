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
#include "api_test_base.h"

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/test_base.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/utils/create_tx.h>

#include "runner.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

void api_test_base::set_dbpath(configuration& cfg) {
    temporary_.prepare();
    cfg.db_location(path());
}

void api_test_base::db_create(std::shared_ptr<configuration> cfg) {
    set_dbpath(*cfg);
    db_ = std::shared_ptr{api::create_database(cfg)};
}

void api_test_base::db_setup(std::shared_ptr<configuration> cfg) {
    db_create(cfg);
    db_->start();
}

void api_test_base::db_teardown() {
    db_->stop();
    temporary_.clean();
}

api::impl::database* api_test_base::db_impl() {
    return unsafe_downcast<api::impl::database>(db_.get());
}

void api_test_base::explain(api::executable_statement& stmt) {
    if (to_explain()) {
        db_->explain(stmt, std::cout);
        std::cout << std::endl;
    }
}

runner api_test_base::builder() {
    return runner{}
        .db(*db_)
        .show_plan(to_explain())
        .show_recs(true);
}

void api_test_base::execute_query(
    std::string_view query,
    api::parameter_set const& params,
    api::transaction_handle& tx,
    std::vector<mock::basic_record>& out
) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .params(params)
            .tx(tx)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(
    std::string_view query,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    api::transaction_handle& tx,
    std::vector<mock::basic_record>& out
) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .params(params)
            .vars(variables)
            .tx(tx)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(
    api::statement_handle& prepared,
    api::parameter_set const& params,
    api::transaction_handle& tx,
    std::vector<mock::basic_record>& out
) {
    ASSERT_EQ("",
        builder()
            .prepared(prepared)
            .params(params)
            .tx(tx)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(
    std::string_view query,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    std::vector<mock::basic_record>& out
) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .params(params)
            .vars(variables)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(
    std::string_view query,
    api::parameter_set const& params,
    std::vector<mock::basic_record>& out
) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .params(params)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(std::string_view query, std::vector<mock::basic_record>& out) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_query(std::string_view query, api::transaction_handle& tx, std::vector<mock::basic_record>& out) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .tx(tx)
            .output_records(out)
            .run()
            .report()
    );
}

void api_test_base::execute_statement(
    std::string_view query,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    api::transaction_handle& tx,
    status expected
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .text(query)
            .tx(tx)
            .params(params)
            .vars(variables)
            .expect_error(expected != status::ok)
            .st(result)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::execute_statement(
    std::string_view query,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    status expected,
    bool no_abort
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .text(query)
            .params(params)
            .vars(variables)
            .st(result)
            .expect_error(expected != status::ok)
            .no_abort(no_abort)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::execute_statement(
    std::string_view query,
    api::transaction_handle& tx,
    status expected
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .text(query)
            .tx(tx)
            .st(result)
            .expect_error(expected != status::ok)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::execute_statement(
    std::string_view query,
    status expected,
    bool no_abort
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .text(query)
            .st(result)
            .expect_error(expected != status::ok)
            .no_abort(no_abort)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::test_stmt_err(
    std::string_view stmt,
    api::transaction_handle& tx,
    error_code expected
) {
    std::shared_ptr<error::error_info> result{};
    ASSERT_EQ("",
        builder()
            .text(stmt)
            .tx(tx)
            .error(result)
            .expect_error(true)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result->code());
    std::cerr << *result << std::endl;
}

void api_test_base::test_stmt_err(
    api::statement_handle stmt,
    error_code expected
) {
    std::shared_ptr<error::error_info> result{};
    ASSERT_EQ("",
        builder()
            .prepared(stmt)
            .expect_error(true)
            .error(result)
            .run()
            .report()
    );
    ASSERT_TRUE(result);
    ASSERT_EQ(expected, result->code());
    std::cerr << *result << std::endl;
}

void api_test_base::explain_statement(
    std::string_view query,
    std::string& out,
    std::unordered_map<std::string, api::field_type_kind> const& variables
) {
    ASSERT_EQ("",
        builder()
            .db(*db_)
            .text(query)
            .explain(out)
            .vars(variables)
            .run()
            .report()
    );
}

void api_test_base::explain_statement(
    std::string_view query,
    std::string& out,
    api::parameter_set const& params,
    std::unordered_map<std::string, api::field_type_kind> const& variables
) {
    ASSERT_EQ("",
        builder()
            .text(query)
            .explain(out)
            .params(params)
            .vars(variables)
            .run()
            .report()
    );
}

void api_test_base::execute_statement(
    api::statement_handle prepared,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    api::transaction_handle& tx,
    status expected
) {
    (void) variables; //TODO delete from input args
    status out{};
    ASSERT_EQ("",
        builder()
            .prepared(prepared)
            .params(params)
            .expect_error(expected != status::ok)
            .tx(tx)
            .st(out)
            .run()
            .report()
    );
    ASSERT_EQ(expected, out);
}

void api_test_base::execute_statement(
    api::statement_handle prepared,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    api::parameter_set const& params,
    status expected,
    bool no_abort
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .prepared(prepared)
            .params(params)
            .vars(variables)
            .expect_error(expected != status::ok)
            .no_abort(no_abort)
            .st(result)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::execute_statement(
    api::statement_handle prepared,
    api::transaction_handle& tx,
    status expected
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .prepared(prepared)
            .expect_error(expected != status::ok)
            .tx(tx)
            .st(result)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::execute_statement(
    api::statement_handle prepared,
    status expected,
    bool no_abort
) {
    status result{};
    ASSERT_EQ("",
        builder()
            .prepared(prepared)
            .expect_error(expected != status::ok)
            .no_abort(no_abort)
            .st(result)
            .run()
            .report()
    );
    ASSERT_EQ(expected, result);
}

void api_test_base::resolve(std::string& query, std::string_view place_holder, std::string value) {
    query = std::regex_replace(query, std::regex(std::string(place_holder)), value);
}

std::string api_test_base::path() const {
    return temporary_.path();
}
}
