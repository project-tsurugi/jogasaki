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
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/test_base.h>
#include <jogasaki/test_utils/temporary_folder.h>

namespace jogasaki::utils {
class runner;
}
namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
using takatori::util::fail;

class api_test_base : public test_base {

protected:
    virtual bool to_explain() = 0;

public:

    void set_dbpath(configuration& cfg);
    void db_setup(std::shared_ptr<configuration> cfg = std::make_shared<configuration>());
    void db_create(std::shared_ptr<configuration> cfg = std::make_shared<configuration>());
    void db_teardown();

    api::impl::database* db_impl();

    void explain(api::executable_statement& stmt);

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        api::transaction_handle& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        api::transaction_handle& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        api::parameter_set const& params,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        api::transaction_handle& tx,
        std::vector<mock::basic_record>& out
    );

    void execute_query(
        std::string_view query,
        std::vector<mock::basic_record>& out
    );

    void execute_statement(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        api::transaction_handle& tx,
        status expected = status::ok
    );
    void execute_statement(
        std::string_view query,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        status expected = status::ok,
        bool no_abort = false
    );
    void execute_statement(
        std::string_view query,
        api::transaction_handle& tx,
        status expected = status::ok
    );

    void execute_statement(
        std::string_view query,
        status expected = status::ok,
        bool no_abort = false
    );

    void execute_statement(
        api::statement_handle prepared,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        api::transaction_handle& tx,
        status expected = status::ok
    );
    void execute_statement(
        api::statement_handle prepared,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        status expected = status::ok,
        bool no_abort = false
    );
    void execute_statement(
        api::statement_handle prepared,
        api::transaction_handle& tx,
        status expected = status::ok
    );

    void execute_statement(
        api::statement_handle prepared,
        status expected = status::ok,
        bool no_abort = false
    );

    void execute_statement(
        std::string_view stmt,
        request_info info
    );

    void test_stmt_err(
        std::string_view stmt,
        api::transaction_handle& tx,
        error_code expected,
        std::string_view msg = {}
    );
    void test_stmt_err(
        api::statement_handle stmt,
        error_code expected
    );

    void test_stmt_err(
        std::string_view stmt,
        error_code expected,
        std::string_view msg = ""
    );

    void test_stmt_err(
        std::string_view stmt,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::parameter_set const& params,
        error_code expected = error_code::none,
        std::string_view msg = "");

    void resolve(std::string& query, std::string_view place_holder, std::string value);

    void explain_statement(
        std::string_view query,
        std::string& out,
        api::parameter_set const& params,
        std::unordered_map<std::string, api::field_type_kind> const& variables = {}
    );

    void explain_statement(
        std::string_view query,
        std::string& out,
        std::unordered_map<std::string, api::field_type_kind> const& variables = {}
    );

    template <class T>
    void set(api::parameter_set& ps, std::string_view place_holder, api::field_type_kind kind, T value) {
        host_variables_.emplace(place_holder, kind);
        switch(kind) {
            case api::field_type_kind::int4:
                if constexpr (std::is_convertible_v<T, std::int32_t>) {
                    ps.set_int4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::int8:
                if constexpr (std::is_convertible_v<T, std::int64_t>) {
                    ps.set_int8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float4:
                if constexpr (std::is_convertible_v<T, float>) {
                    ps.set_float4(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::float8:
                if constexpr (std::is_convertible_v<T, double>) {
                    ps.set_float8(place_holder, value);
                } else {
                    fail();
                }
                break;
            case api::field_type_kind::character:
                if constexpr (std::is_convertible_v<T, std::string_view>) {
                    ps.set_character(place_holder, value);
                } else {
                    fail();
                }
                break;
            default:
                fail();
        }
    }

    utils::runner builder();

    [[nodiscard]] std::string path() const;

    test::temporary_folder temporary_{};  //NOLINT
    takatori::util::maybe_shared_ptr<jogasaki::api::database> db_;  //NOLINT
    std::unordered_map<std::string, api::field_type_kind> host_variables_{};  //NOLINT

private:
    void execute_query(
        api::statement_handle& prepared,
        api::parameter_set const& params,
        api::transaction_handle& tx,
        std::vector<mock::basic_record>& out
    );
};

}
