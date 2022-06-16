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

#include <mutex>

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <sharksfin/api.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

using ::takatori::util::fail;

class database;
/**
 * @brief transaction object
 * @details The object is thread unsafe, and should not be called from different threads simultaneously.
 */
class transaction {
public:
    /**
     * @brief create empty object
     */
    transaction() = default;

    /**
     * @brief create new object
     * @param db the parent database that the transaction runs on
     * @param readonly whether the transaction is read-only
     * @param is_long whether the transaction is long batch
     * @param write_preserves write preserve storage names
     */
    explicit transaction(kvs::database& db,
        bool readonly = false,
        bool is_long= false,
        std::vector<std::string> const& write_preserves = {}
    );

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
     * @brief wait for commit
     * @details wait for async commit
     * @return status::ok if the operation is successful
     * @return status::err_time_out if waiting timed out
     * @return other status code when error occurs
     */
    [[nodiscard]] status wait_for_commit(std::size_t timeout_ns = 0UL);

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
     * @brief return the mutex object owned by this transaction
     * @return the mutex object that can be used to serialize the laned operations for the shared transactin
     */
    [[nodiscard]] std::mutex& mutex() noexcept;

    /**
     * @brief return the native handle in the transaction layer
     * @note this is expected to be package private (i.e. callable from code in kvs namespace)
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionState check_state() noexcept;

private:
    sharksfin::TransactionControlHandle tx_{};
    sharksfin::TransactionHandle handle_{};
    kvs::database* database_{};
    bool active_{true};
    std::mutex mutex_{};
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

