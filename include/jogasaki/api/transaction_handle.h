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
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <variant>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/error_info.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/hash_combine.h>

namespace jogasaki::api {

using takatori::util::maybe_shared_ptr;

/**
 * @brief transaction handle
 * @details the handle is the trivially copyable object that references transaction object stored in the database.
 * Using the handle, caller can create, execute, and destroy the transaction keeping the ownership managed
 * by the database. This makes api more flexible than handling the ownership object such as unique_ptr
 * for transaction object.
 */
class transaction_handle {
public:
    /**
     * @brief the callback type used for async execution
     * @see `execute_async` or `commit_async`
     */
    using callback = std::function<void(status, std::string_view)>;

    /**
     * @brief the callback type used for async execution
     * @see `execute_async` or `commit_async`
     */
    using error_info_callback = std::function<
        void(status, std::shared_ptr<api::error_info>)
    >;

    /**
     * @brief the callback type used for async execution
     * @see `execute_async` or `commit_async`
     */
    using error_info_stats_callback = std::function<
        void(status, std::shared_ptr<api::error_info>, std::shared_ptr<request_statistics>)
    >;

    /**
     * @brief create empty handle - null reference
     */
    transaction_handle() = default;

    /**
     * @brief destruct the object
     */
    ~transaction_handle() = default;

    transaction_handle(transaction_handle const& other) = default;
    transaction_handle& operator=(transaction_handle const& other) = default;
    transaction_handle(transaction_handle&& other) noexcept = default;
    transaction_handle& operator=(transaction_handle&& other) noexcept = default;

    /**
     * @brief create new object from id
     * @param surrogate_id the surrogate id of the transaction
     */
    explicit transaction_handle(
        std::size_t surrogate_id
    ) noexcept;

    /**
     * @brief create new object from id
     * @param surrogate_id the surrogate id of the transaction
     * @param session_id the session id of the transaction
     */
    transaction_handle(
        std::size_t surrogate_id,
        std::optional<std::size_t> session_id
    ) noexcept;

    /**
     * @brief conversion operator to std::size_t
     * @return the hash value that can be used for equality comparison
     * @note this function is kept for compatibility. The old intel tbb (pre-oneTBB) used this function
     * and build failed on Alma and Rockey linux 9.5
     * TODO remove when it's not needed
     */
    explicit operator std::size_t() const noexcept;

    /**
     * @brief conversion operator to bool
     * @return whether the handle has body (i.e. valid transaction object reference) or not
     */
    explicit operator bool() const noexcept;

    /**
     * @brief commit the transaction
     * @param option commit options
     * @return status::ok when successful
     * @return error code otherwise
     * @note this function is synchronous and committing transaction may require indefinite length of wait for other tx.
     * @deprecated Use `commit_async`. This function is left for testing.
     */
    status commit(commit_option option = commit_option{});

    /**
     * @brief commit the transaction asynchronously
     * @param on_completion callback to be called on completion
     * @note normal error such as SQL runtime processing failure will be reported by callback
     * @deprecated Use `commit_async(error_info_callback)`. This function is left for testing.
     */
    void commit_async(callback on_completion);

    /**
     * @brief commit the transaction asynchronously
     * @param on_completion callback to be called on completion
     * @param opt options for the commit operation
     * @note normal error such as SQL runtime processing failure will be reported by callback
     */
    void commit_async(
        error_info_callback on_completion,
        commit_option opt = commit_option{},
        request_info const& req_info = request_info{}
    );

    /**
     * @brief abort the transaction and have transaction engine rollback the on-going processing (if it supports rollback)
     * @return status::ok when successful
     * @return error code otherwise
     * @deprecated use `abort_transaction` instead
     */
    status abort(request_info const& req_info = {});

    /**
     * @brief abort the transaction and have transaction engine rollback the on-going processing (if it supports rollback)
     * @return status::ok when successful
     * @return error code otherwise
     */
    status abort_transaction(request_info const& req_info = {});

    /**
     * @brief execute the statement in the transaction. No result records are expected
     * from the statement (e.g. insert/update/delete).
     * @param statement the statement to be executed
     * @return status::ok when successful
     * @return error code otherwise
     */
    status execute(
        executable_statement& statement,
        request_info const& req_info = {}
    );

    /**
     * @brief execute the statement in the transaction. The result records are expected.
     * from the statement (e.g. query to tables/views).
     * @param statement the statement to be executed
     * @param result [out] the unique ptr to be filled with result set, which must be closed when caller
     * completes using the result records.
     * @return status::ok when successful
     * @return error code otherwise
     * @deprecated kept for testing purpose. record_meta from result doesn't provide column names
     */
    status execute(
        executable_statement& statement,
        std::unique_ptr<result_set>& result,
        request_info const& req_info = {}
    );

    /**
     * @brief resolve and execute the statement in the transaction. The result records are expected.
     * from the statement (e.g. query to tables/views).
     * @param prepared the statement to be executed
     * @param parameters the parameters to assign value for each placeholder
     * @param result [out] the unique ptr to be filled with result set, which must be closed when caller
     * completes using the result records.
     * @return status::ok when successful
     * @return error code otherwise
     * @deprecated kept for testing purpose. record_meta from result doesn't provide column names
     */
    status execute(
        api::statement_handle prepared,
        std::shared_ptr<api::parameter_set> parameters,
        std::unique_ptr<result_set>& result,
        request_info const& req_info = {}
    );

    /**
     * @brief asynchronously execute the statement in the transaction. No result records are expected
     * from the statement (e.g. insert/update/delete).
     * @param statement the statement to be executed. If raw pointer is passed, caller is responsible to ensure it live
     * long by the end of callback.
     * @param on_completion the callback invoked when async call is completed
     * @return true when successful
     * @return false on error in preparing async execution (normally this should not happen)
     * @note normal error such as SQL runtime processing failure will be reported by callback
     */
    bool execute_async(
        maybe_shared_ptr<executable_statement> const& statement,
        callback on_completion,
        request_info const& req_info = {}
    );

    /**
     * @brief asynchronously execute the statement in the transaction. No result records are expected
     * from the statement (e.g. insert/update/delete).
     * @param statement the statement to be executed. If raw pointer is passed, caller is responsible to ensure it live
     * long by the end of callback.
     * @param on_completion the callback invoked when async call is completed
     * @return true when successful
     * @return false on error in preparing async execution (normally this should not happen)
     * @note normal error such as SQL runtime processing failure will be reported by callback
     */
    bool execute_async(
        maybe_shared_ptr<executable_statement> const& statement,
        error_info_stats_callback on_completion,
        request_info const& req_info = {}
    );

    /**
     * @brief asynchronously execute the statement in the transaction. The result records are expected
     * to be written to the writers derived from `channel`
     * @param statement the statement to be executed.
     * If raw pointer is passed, caller is responsible to ensure it live long by the end of callback.
     * @param channel the data channel to acquire/release writer to write output data
     * If raw pointer is passed, caller is responsible to ensure it live long by the end of callback.
     * @param on_completion the callback invoked when async call is completed
     * @return true when successful
     * @return false on error in preparing async execution (normally this should not happen)
     * @note normal error such as SQL runtime processing failure will be reported by callback
     */
    bool execute_async(
        maybe_shared_ptr<executable_statement> const& statement,
        maybe_shared_ptr<data_channel> const& channel,
        callback on_completion,
        request_info const& req_info = {}
    );

    /**
     * @brief asynchronously execute the statement in the transaction. The result records are expected
     * to be written to the writers derived from `channel`
     * @param statement the statement to be executed.
     * If raw pointer is passed, caller is responsible to ensure it live long by the end of callback.
     * @param channel the data channel to acquire/release writer to write output data
     * If raw pointer is passed, caller is responsible to ensure it live long by the end of callback.
     * @param on_completion the callback invoked when async call is completed
     * @return true when successful
     * @return false on error in preparing async execution (normally this should not happen)
     * @note normal error such as SQL runtime processing failure will be reported by callback
     */
    bool execute_async(
        maybe_shared_ptr<executable_statement> const& statement,
        maybe_shared_ptr<data_channel> const& channel,
        error_info_stats_callback on_completion,
        request_info const& req_info = {}
    );

    /**
     * @brief return the transaction id
     * @return transaction id string
     * @return empty string when it's not available, or transaction handle is invalid
     */
    [[nodiscard]] std::string_view transaction_id() const noexcept;

    /**
     * @brief return the transaction error information
     * @param out transaction error info (nullptr if the transaction error is not available)
     * @return status::ok if successful
     * @return status::err_invalid_argument if the transaction handle is invalid
     */
    [[nodiscard]] status error_info(std::shared_ptr<api::error_info>& out) const noexcept;

    /**
     * @brief return the surrogate id of the transaction
     * @return surrogate transaction id
     */
    [[nodiscard]] std::size_t surrogate_id() const noexcept;

    /**
     * @brief return the session id of the transaction
     * @return session id
     */
    [[nodiscard]] std::optional<std::size_t> session_id() const noexcept;

private:
    std::size_t surrogate_id_{};
    std::optional<std::size_t> session_id_{};
};

static_assert(std::is_trivially_copyable_v<transaction_handle>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(transaction_handle const& a, transaction_handle const& b) noexcept {
    if (a.surrogate_id() != b.surrogate_id()) {
        return false;
    }
    if (a.session_id().has_value() != b.session_id().has_value()) {
        return false;
    }
    if (! a.session_id().has_value()) {
        return true;
    }
    return *a.session_id() == *b.session_id();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(transaction_handle const& a, transaction_handle const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, transaction_handle value) {
    // surrogate_id is printed in decimal to avoid using utils::hex in public header
    out << "transaction_handle[surrogate_id:" << value.surrogate_id();
    if (value.session_id().has_value()) {
        // print session_id in decimal to conform to tateyama log message
        out << ",session_id:" << value.session_id().value();
    }
    out << "]";
    return out;
}

}  // namespace jogasaki::api

/**
 * @brief std::hash specialization
 */
template<>
struct std::hash<jogasaki::api::transaction_handle> {
    /**
     * @brief compute hash of the given object.
     * @param value the target object
     * @return computed hash code
     */
    std::size_t operator()(jogasaki::api::transaction_handle const& value) const noexcept {
        if (! value.session_id().has_value()) {
            // normally we don't mix entries with/without session_id, so this doesn't make many collisions
            return value.surrogate_id();
        }
        return jogasaki::utils::hash_combine(value.surrogate_id(), *value.session_id());
    }
};

