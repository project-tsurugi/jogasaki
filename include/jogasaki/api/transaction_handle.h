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

#include <cstdint>
#include <functional>
#include <ostream>
#include <type_traits>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/status.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/parameter_set.h>

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
     * @brief create new object from integer
     * @param arg integer representing target object pointer
     */
    explicit transaction_handle(std::uintptr_t arg) noexcept;

    /**
     * @brief create new object from integer
     * @param arg integer representing target object pointer
     */
    explicit transaction_handle(void* arg) noexcept;

    /**
     * @brief accessor to the content of the handle
     * @return the handle value
     */
    [[nodiscard]] std::uintptr_t get() const noexcept;

    /**
     * @brief conversion operator to std::size_t
     * @return the hash value that can be used for equality comparison
     */
    explicit operator std::size_t() const noexcept;

    /**
     * @brief conversion operator to bool
     * @return whether the handle has body (i.e. valid transaction object reference) or not
     */
    explicit operator bool() const noexcept;

    /**
     * @brief commit the transaction
     * @return status::ok when successful
     * @return error code otherwise
     */
    status commit();

    /**
     * @brief abort the transaction and have transaction engine rollback the on-going processing (if it supports rollback)
     * @return status::ok when successful
     * @return error code otherwise
     */
    status abort();

    /**
     * @brief execute the statement in the transaction. No result records are expected
     * from the statement (e.g. insert/update/delete).
     * @param statement the statement to be executed
     * @return status::ok when successful
     * @return error code otherwise
     */
    status execute(executable_statement& statement);

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
    status execute(executable_statement& statement, std::unique_ptr<result_set>& result);

    /**
     * @brief execute the statement in the transaction. The result records are expected.
     * from the statement (e.g. query to tables/views).
     * @param prepared the statement to be executed
     * @param result [out] the unique ptr to be filled with result set, which must be closed when caller
     * completes using the result records.
     * @return status::ok when successful
     * @return error code otherwise
     * @deprecated kept for testing purpose. record_meta from result doesn't provide column names
     */
    status execute(
        api::statement_handle prepared,
        std::shared_ptr<api::parameter_set> parameters,
        std::unique_ptr<result_set>& result);

    /**
     * @brief the callback type used for async execution
     * @see `execute_async`
     */
    using callback = std::function<void(status, std::string_view)>;

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
    bool execute_async(maybe_shared_ptr<executable_statement> const& statement, callback on_completion);

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
        callback on_completion
    );

private:
    std::uintptr_t body_{};
};

static_assert(std::is_trivially_copyable_v<transaction_handle>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(transaction_handle const& a, transaction_handle const& b) noexcept {
    return a.get() == b.get();
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
    return out << "transaction_handle[" << value.get() << "]";
}

}
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
        return static_cast<std::size_t>(value);
    }
};

