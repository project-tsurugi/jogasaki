/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "extract_sql_and_tx_id.h"

#include <ostream>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <optional>
#include <type_traits>
#include <glog/logging.h>

#include <takatori/util/string_builder.h>

#include <jogasaki/request_context.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/statement_handle_internal.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/datastore/get_lob_data.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/create_statement_handle_error.h>
#include <jogasaki/utils/decimal.h>
#include <jogasaki/utils/proto_field_types.h>

namespace jogasaki::utils {

using takatori::util::string_builder;

template<class Request>
static std::string extract_transaction(
    Request const& msg,
    std::shared_ptr<error::error_info>& err_info,
    std::optional<std::size_t> session_id
) {
    if(! msg.has_transaction_handle()) {
        err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing transaction_handle",
            status::err_invalid_argument
        );
        return {};
    }
    api::transaction_handle tx{
        msg.transaction_handle().handle(),
        session_id
    };
    auto t = get_transaction_context(tx);
    if(! t) {
        // failed to get transaction_context
        // this is not an error because depending on the timing transaction may be disposed
        // return empty string as transaction id
        return {};
    }
    return std::string{t->transaction_id()};
}

template<class Request>
static std::shared_ptr<api::impl::prepared_statement> extract_statement(
    Request const& msg,
    std::shared_ptr<error::error_info>& out,
    std::optional<std::size_t> session_id
) {
    if(! msg.has_prepared_statement_handle()) {
        out = create_error_info(
            error_code::statement_not_found_exception,
            "Invalid request format - missing prepared_statement_handle",
            status::err_invalid_argument
        );
        return {};
    }
    api::statement_handle handle{
        reinterpret_cast<void*>(msg.prepared_statement_handle().handle()), session_id}; //NOLINT
    auto stmt = get_statement(handle);
    if (stmt == nullptr) {
        out = create_statement_handle_error(handle);
        return {};
    }
    return stmt;
}

bool extract_sql_and_tx_id(
    sql::request::Request const& req,
    std::shared_ptr<std::string>& sql_text,
    std::string& tx_id,
    std::shared_ptr<error::error_info>& err_info,
    std::optional<std::size_t> session_id
) {
    switch (req.request_case()) {
        case sql::request::Request::RequestCase::kExecuteStatement: {
            auto& msg = req.execute_statement();
            sql_text = std::make_shared<std::string>(msg.sql());
            tx_id = extract_transaction(msg, err_info, session_id);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecuteQuery: {
            auto& msg = req.execute_query();
            sql_text = std::make_shared<std::string>(msg.sql());
            tx_id = extract_transaction(msg, err_info, session_id);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedStatement: {
            auto& msg = req.execute_prepared_statement();
            auto stmt = extract_statement(msg, err_info, session_id);
            if(! stmt) {
                return false;
            }
            sql_text = stmt->body()->sql_text_shared();
            tx_id = extract_transaction(msg, err_info, session_id);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedQuery: {
            auto& msg = req.execute_prepared_query();
            auto stmt = extract_statement(msg, err_info, session_id);
            if(! stmt) {
                return false;
            }
            sql_text = stmt->body()->sql_text_shared();
            tx_id = extract_transaction(msg, err_info, session_id);
            if(err_info) {
                return false;
            }
            break;
        }
        default: {
            auto msg = string_builder{} << "extracting sql from request payload " << req.request_case()
                                        << " is unsupported" << string_builder::to_string;
            err_info = create_error_info(
                error_code::request_failure_exception,
                msg,
                status::err_unsupported
            );
            return false;
        }
    }
    return true;
}

} // namespace jogasaki::utils
