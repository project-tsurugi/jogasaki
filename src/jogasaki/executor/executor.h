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
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/io/dump_config.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/latch.h>

namespace jogasaki::api::impl {
class database;
}

/**
 * @brief interface for SQL job execution
 */
namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

/**
 * @brief the callback type used for async execution
 */
using error_info_callback = std::function<void(status, std::shared_ptr<error::error_info>)>;

/**
 * @brief the callback type exchanging statistics information
 */
using error_info_stats_callback = std::function<
    void(status, std::shared_ptr<error::error_info>, std::shared_ptr<request_statistics>)
>;

/**
 * @brief commit the transaction
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param option commit options
 * @return status::ok when successful
 * @return error code otherwise
 * @note this function is synchronous and committing transaction may require indefinite length of wait for other tx.
 * @deprecated Use `commit_async`. This function is left for testing.
 */
status commit(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::commit_option option = api::commit_option{}
);

/**
 * @brief commit the transaction asynchronously
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param on_completion callback on completion of commit
 * @param option commit options
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return id of the job to execute commit
 * @note normal error such as SQL runtime processing failure will be reported by callback
 * @deprecated Use `commit_async` with commit_response_callback arg. This function is left for testing.
 */
scheduler::job_context::job_id_type commit_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    error_info_callback on_completion,
    api::commit_option option,
    request_info const& req_info
);

/**
 * @brief commit the transaction asynchronously
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param on_response callback invoked when commit successfully makes progress to the point
 * where `response_kinds` indicates
 * @param response_kinds the commit response points to invoke the `on_response` callback
 * @note currently, only `commit_response_kind::accepted` and `commit_response_kind::stored` are supported
 * @param on_error callback invoked when commit fails
 * @param option commit options
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return id of the job to execute commit
 */
scheduler::job_context::job_id_type commit_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    commit_response_callback on_response,
    commit_response_kind_set response_kinds,
    commit_error_callback on_error,
    api::commit_option option,
    request_info const& req_info
);

/**
 * @brief abort the transaction
 * @param tx the transaction to abort
 * @param req_info the request info. used to external logging
 * @note this function is synchronous
 */
void abort_transaction(
    std::shared_ptr<transaction_context> tx,
    request_info const& req_info = {}
);

/**
 * @brief execute statement expecting result set
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param statement statement to execute
 * @param result [out] the result set to be filled on completion
 * @param error [out] the info object to be filled on error
 * @param stats [out] the stats object to be filled
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 * @deprecated This is kept for testing. Use execute_async for production.
 */
status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result,
    std::shared_ptr<error::error_info>& error,
    std::shared_ptr<request_statistics>& stats,
    request_info const& req_info = {}
);

/**
 * @brief execute statement expecting result set
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param prepared prepared statement to execute
 * @param parameters parameters to fill the place holders
 * @param result [out] the result set to be filled on completion
 * @param error [out] the info object to be filled on error
 * @param stats [out] the stats object to be filled
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 * @deprecated This is kept for testing. Use execute_async for production.
 */
status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result,
    std::shared_ptr<error::error_info>& error,
    std::shared_ptr<request_statistics>& stats,
    request_info const& req_info = {}
);

/**
 * @brief execute statement (or query) asynchronously
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param statement statement to execute
 * @param channel channel to receive statement result records, pass nullptr if no records are to be received
 * @param on_completion callback on completion of statement execution
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @param sync specify true if execution waits for it completion before function finishes. Testing purpose only.
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    error_info_stats_callback on_completion,
    request_info const& req_info = {},
    bool sync = false
);

/**
 * @brief execute statement (or query) asynchronously
 * @param database the database to request execution
 * @param tx the transaction used to execute the request
 * @param prepared prepared statement to execute
 * @param parameters parameters to fill place holders
 * @param channel channel to receive statement result records, pass nullptr if no records are to be received
 * @param on_completion callback on completion of statement execution
 * @param sync specify true if execution waits for it completion before function finishes. Testing purpose only.
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    error_info_stats_callback on_completion,
    bool sync = false,
    request_info const& req_info = {}
);

/**
 * @brief execute statement (or query) asynchronously on the given request context
 * @param database the database to request execution
 * @param rctx the request context to execute statement on
 * @param statement statement to execute
 * @param on_completion callback on completion of statement execution
 * @param sync specify true if execution waits for it completion before function finishes. Testing purpose only.
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async_on_context(
    api::impl::database& database,
    std::shared_ptr<request_context> rctx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    error_info_stats_callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync,
    request_info const& req_info = {}
);

//@brief indicates undefined for the execute dump arg
constexpr static std::size_t undefined = static_cast<std::size_t>(-1);

/**
 * @brief execute dump
 * @param database the database to request dump execution
 * @param tx the transaction used to execute the request
 * @param statement statement to execute
 * @param channel the channel to dump out result files list
 * @param directory the directory where the dump result files will be created
 * @param on_completion callback on completion of statement execution
 * @param cfg dump setting options
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_dump(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    error_info_callback on_completion,
    executor::io::dump_config const& cfg = {},
    request_info const& req_info = {}
);

/**
 * @brief execute (transactional) load
 * @param database the database to request dump execution
 * @param tx the transaction used to execute the request
 * @param prepared statement used to execute load
 * @param parameters the parameters prototype that will be filled for each loaded records
 * @param files the list of file path to be loaded
 * @param on_completion callback on completion of load
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_load(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    error_info_callback on_completion,
    request_info const& req_info = {}
);

/**
 * @brief create and start new transaction
 * @param db the parent database that the transaction runs on
 * @param out [OUT] filled with newly created transaction object
 * @param options transaction options
 * @return status::ok when successful
 * @return error otherwise
 */
[[nodiscard]] status create_transaction(
    api::impl::database &db,
    std::shared_ptr<transaction_context>& out,
    std::shared_ptr<kvs::transaction_option const> options
);

}  // namespace jogasaki::executor
