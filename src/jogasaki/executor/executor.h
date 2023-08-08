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
#pragma once

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/utils/latch.h>

#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {
class database;
}

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

using callback = api::transaction_handle::callback;

namespace details {

bool execute_internal(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion,  //NOLINT(performance-unnecessary-value-param)
    bool sync
);

std::shared_ptr<request_context> create_request_context(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    std::shared_ptr<scheduler::request_detail> request_detail
);

status init(
    api::impl::database& database,
    kvs::transaction_option const& options,
    std::shared_ptr<transaction_context>& out
);

}  //namespace details

/**
 * @brief check if transaction is already assigned to epoch and ready for request
 * @return true when transaction is ready
 * @return false otherwise
 */
[[nodiscard]] bool is_ready(
    std::shared_ptr<transaction_context> const& tx
);

/**
 * @brief commit the transaction
 * @return status::ok when successful
 * @return error code otherwise
 * @note this function is synchronous and committing transaction may require indefinite length of wait for other tx.
 * @deprecated Use `commit_async`. This function is left for testing.
 */
status commit(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx
);

/**
 * @brief commit the transaction asynchronously
 * @return id of the job to execute commit
 * @note normal error such as SQL runtime processing failure will be reported by callback
 */
scheduler::job_context::job_id_type commit_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    callback on_completion
);

/**
 * @brief abort the transaction
 * @return status::ok when successful
 * @return error code otherwise
 * @note this function is synchronous
 */
status abort(
    std::shared_ptr<transaction_context> const& tx
);

/**
 * @brief execute statement expecting result set
 * @param statement
 * @param result
 * @return status::ok when successful
 * @return error otherwise
 * @deprecated This is kept for testing. Use execute_async for production.
 */
status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
);

/**
 * @brief execute statement expecting result set
 * @param prepared prepared statement to execute
 * @param parameters parameters to fill the place holders
 * @param result
 * @return status::ok when successful
 * @return error otherwise
 * @deprecated This is kept for testing. Use execute_async for production.
 */
status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result
);

/**
 * @brief execute statement (or query) asynchronously
 * @param statement statement to execute
 * @param channel channel to receive statement result records, pass nullptr if no records are to be received
 * @param on_completion callback on completion of statement execution
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    callback on_completion
);

/**
 * @brief execute statement (or query) asynchronously
 * @param prepared prepared statement to execute
 * @param parameters parameters to fill place holders
 * @param channel channel to receive statement result records, pass nullptr if no records are to be received
 * @param on_completion callback on completion of statement execution
 * @param sync specify true if execution waits for it completion before function finishes. Testing purpose only.
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion,
    bool sync = false
);

/**
 * @brief execute statement (or query) asynchronously on the given request context
 * @param rctx the request context to execute statement on
 * @param statement statement to execute
 * @param on_completion callback on completion of statement execution
 * @param sync specify true if execution waits for it completion before function finishes. Testing purpose only.
 * @return status::ok when successful
 * @return error otherwise
 */
bool execute_async_on_context(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    std::shared_ptr<request_context> rctx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
);

constexpr static std::size_t undefined = static_cast<std::size_t>(-1);
bool execute_dump(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    callback on_completion,
    std::size_t max_records_per_file = undefined,
    bool keep_files_on_error = false
);

bool execute_load(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    callback on_completion
);

/**
 * @brief return the transaction id
 * @return transaction id string
 * @return empty string when it's not available
 */
[[nodiscard]] std::string_view transaction_id(
    std::shared_ptr<transaction_context> const& tx
) noexcept;

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
    kvs::transaction_option const& options
);

/**
 * @brief commit function for internal use
 * @details this is for internal use (esps, commit operation for loading), not intended for external caller.
 * @return status::ok when successful
 * @return error code otherwise
 * @note this function is synchronous and committing transaction may require indefinite length of wait for other tx.
 */
status commit_internal(
    std::shared_ptr<transaction_context> const& tx
);

}
