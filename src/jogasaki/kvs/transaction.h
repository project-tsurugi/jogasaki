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

#include <mutex>

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <sharksfin/api.h>
#include <jogasaki/status.h>

#include <jogasaki/kvs/transaction_option.h>

namespace jogasaki::kvs {

using ::takatori::util::fail;

class database;
/**
 * @brief transaction object
 * @details The object is thread unsafe, and should not be called from different threads simultaneously.
 */
class transaction {
public:
    using commit_callback_type = ::sharksfin::commit_callback_type;
    /**
     * @brief create empty object
     */
    transaction() = default;

    /**
     * @brief create new object
     * @param db the parent database that the transaction runs on
     */
    explicit transaction(kvs::database& db);

    /**
     * @brief destruct object
     */
    ~transaction() noexcept;

    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status commit(bool async = false);

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return true if callback is already invoked when control is returned to caller
     * @return false otherwise
     */
    [[nodiscard]] bool commit(commit_callback_type cb);

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();

    /**
     * @brief return the native transaction control handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionControlHandle control_handle() const noexcept;

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionHandle handle() noexcept;

    /**
     * @brief return the parent database object
     * @return the parent database
     */
    [[nodiscard]] kvs::database* database() const noexcept;

    /**
     * @brief return the tx state
     * @return the tx state of this object
     */
    [[nodiscard]] sharksfin::TransactionState check_state() noexcept;

    /**
     * @brief return the detailed info for the recent kvs api call
     * @return recent api call result
     * @return nullptr if result is not available
     */
    [[nodiscard]] std::shared_ptr<sharksfin::CallResult> recent_call_result() noexcept;

    /**
     * @brief return the transaction id
     * @return transaction id string
     * @return empty string when it's not available
     */
    [[nodiscard]] std::string_view transaction_id() const noexcept;

    /**
     * @brief create and start new transaction
     * @param db the parent database that the transaction runs on
     * @param out [OUT] filled with newly created transaction object
     * @param options transaction options
     * @return status::ok when successful
     * @return status::err_resource_limit_reached if transaction count exceeds its limit
     * @return status::err_invalid_argument if option value is invalid
     * @return error otherwise
     */
    [[nodiscard]] static status create_transaction(
        kvs::database &db,
        std::unique_ptr<transaction>& out,
        kvs::transaction_option const& options = {}
    );

private:
    sharksfin::TransactionControlHandle tx_{};
    sharksfin::TransactionHandle handle_{};
    kvs::database* database_{};
    bool active_{false};
    std::shared_ptr<sharksfin::TransactionInfo> info_{};

    status init(kvs::transaction_option const& options);
};

/**
 * @brief compare contents of two objects
 * @param a first arg to compare
 * @param b second arg to compare
 * @return true if a == b
 * @return false otherwise
 */
inline bool operator==(transaction const& a, transaction const& b) noexcept {
    return a.control_handle() == b.control_handle();
}

inline bool operator!=(transaction const& a, transaction const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, transaction const& value) {
    out << "transaction(handle:" << std::hex << value.control_handle() << ")";
    return out;
}

}

