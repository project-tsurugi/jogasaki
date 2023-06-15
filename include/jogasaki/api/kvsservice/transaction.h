/*
 * Copyright 2018-2023 tsurugi project.
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

#include <future>
#include <mutex>

#include <sharksfin/api.h>
#include <tateyama/proto/kvs/data.pb.h>

#include "index.h"
#include "commit_option.h"
#include "put_option.h"
#include "status.h"
#include "transaction_state.h"
#include "transaction_result.h"

namespace jogasaki::api::kvsservice {

/**
 * @brief a transaction of KVS database
 */
class transaction {
public:

    /**
     * @brief create new object
     */
    transaction();

    /**
     * @brief create new object
     * @param handle transaction control handle of sharksfin
     */
    explicit transaction(sharksfin::TransactionControlHandle handle);

    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    /**
     * @brief destructor the object
     */
    ~transaction() = default;

    /**
     * @brief retrieves the native transaction control handle in the transaction layer
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionControlHandle control_handle() const noexcept;

    /**
     * @brief retrieves the native handle in the transaction layer
     * @return the handle held by this object
     */
    [[nodiscard]] sharksfin::TransactionHandle handle() const noexcept;

    /**
     * @brief retrieves the system_id of this transaction
     * @return the system_id of the transaction
     */
    [[nodiscard]] std::uint64_t system_id() const noexcept;

    /**
     * @brief retrieves the state of this transaction
     * @return the state of the transaction
     */
    [[nodiscard]] transaction_state state() const noexcept;

    /**
     * @brief acquire the lock of this transaction.
     * @details You should acquire the lock not to run operations of this transaction parallel.
     * You don't have to acquire the lock only if you use this transaction from single thread.
     * @post unlock()
     */
    void lock();

    /**
     * @brief try to acquire the lock of this transaction
     * @return true the lock has been acquired by the method caller, otherwise false
     * @post unlock() if you acquired the lock
     */
    [[nodiscard]] bool try_lock();

    /**
     * @brief release the lock of this transaction
     * @pre lock()
     */
    void unlock();

    /**
     * @brief commit the transaction synchronously
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * This method blocks until the transaction is committed.
     * Use commit_async() if you don't wait for a long time.
     * @param opt commit operation behavior
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] status commit(commit_option opt = commit_option::commit_type_unspecified);

    /**
     * @brief commit the transaction asynchronously
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * This method returns instantly.
     * @param opt commit operation behavior
     * @return the future object of the transaction_result with operation status
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] std::future<transaction_result> commit_async(
            commit_option opt = commit_option::commit_type_unspecified);

    /**
     * @brief abort the transaction synchronously
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * This method blocks until the transaction is aborted.
     * Use abort_async() if you don't wait for a long time.
     * @param rollback indicate whether rollback is requested on abort.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] status abort(bool rollback = true);

    /**
     * @brief abort the transaction asynchronously
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * This method returns instantly.
     * @param rollback indicate whether rollback is requested on abort.
     * @return the future object of the transaction_result with operation status
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] std::future<transaction_result> abort_async(bool rollback = true);

    /**
     * @brief put the value for the given key
     * @param table the name of the table
     * @param key the key for the entry
     * @param value the value for the entry
     * @param option option to set put mode
     * @return status::ok if the target content was successfully put
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return otherwise if error was occurred
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] status put(std::string_view table, tateyama::proto::kvs::data::Record const &key,
                             tateyama::proto::kvs::data::Record const &value,
                             put_option opt = put_option::create_or_update);

    /**
     * @brief get the value for the given key
     * @param index the name of the index
     * @param key key for searching
     * @param value[out] the value of the entry matching the key
     * @return status::ok if the target content was obtained successfully
     * @return status::not_found if the target content does not exist
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return otherwise if error was occurred
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] status get(index &index, tateyama::proto::kvs::data::Record const &key,
                             tateyama::proto::kvs::data::Record &value);

    /**
     * @brief remove the entry for the given key
     * @param table the name of the table
     * @param key the key for searching
     * @return status::ok if the target content was successfully deleted (or not found)
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::not_found if the target content was not found (optional behavior)
     * @return otherwise if error was occurred
     * @pre lock() in multi-thread environment
     * @post unlock() in multi-thread environment
     */
    [[nodiscard]] status remove(std::string_view table, tateyama::proto::kvs::data::Record const &key);

private:
    sharksfin::TransactionControlHandle ctrl_handle_{};
    sharksfin::TransactionHandle tx_handle_{};
    std::uint64_t system_id_ {};
    std::mutex mtx_tx_{}; // for lock(), try_lock(), unlock()
};
}
