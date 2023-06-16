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
#include "remove_option.h"
#include "status.h"
#include "transaction_state.h"

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
     * @brief retrieves the reference of the mutex
     * The returned mutex can be used to run transactional operations
     * only in one thread.
     */
    std::mutex &transaction_mutex();

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * After calling this method, the state of this transaction will be
     * transaction_state::state_kind::waiting_cc_commit, waiting_durable, or
     * durable.
     * This method doesn't wait until the state is what you wanted.
     * You can use state() method to check the state of this transaction.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     * @see state()
     */
    [[nodiscard]] status commit();

    /**
     * @brief abort the transaction synchronously
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();

    /**
     * @brief put the value for the given primary key
     * @param table the name of the table
     * @param primary_key the primary key for the entry
     * the primary_key should contain all columns of the primary key.
     * the primary_key should only contain the primary key data.
     * @param value the value for the entry
     * the value shouldn't contain the primary key data.
     * @param opt option to set put mode
     * @return status::ok if the target content was successfully put.
     * If opt is create_or_update, returns status::ok after successful operation
     * whether the content of the primary_key exists or not.
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::already_exists if opt is create and found the content of the primary_key.
     * @return status::not_found if opt is update and couldn't find the content of the primary_key.
     * @return status::err_invalid_argument if the specified key isn't a primary key or not enough,
     * the specified value contains primary key data.
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status put(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                             tateyama::proto::kvs::data::Record const &value,
                             put_option opt = put_option::create_or_update);

    /**
     * @brief get the value for the given primary key
     * @param index the name of the table
     * @param primary_key primary key for searching
     * the primary_key should contain all columns of the primary key.
     * the primary_key should only contain the primary key data.
     * @param value[out] the value of the entry matching the primary key
     * the value doesn't contain the primary key data.
     * @return status::ok if the target content was obtained successfully
     * @return status::not_found if the target content does not exist
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::err_invalid_argument if the specified key isn't a primary key or not enough
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status get(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                             tateyama::proto::kvs::data::Record &value);

    /**
     * @brief remove the value for the given primary key
     * @param table the name of the table
     * @param primary_key the key for searching
     * @param opt option to set remove mode
     * @return status::ok if the target content was successfully deleted.
     * If opt is instant, returns status::ok after successful operation
     * whether the content of the primary_key exists or not.
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::not_found if opt is counting and the target content was not found
     * @return status::err_invalid_argument if the specified key isn't a primary key or not enough
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status remove(std::string_view table,
                                tateyama::proto::kvs::data::Record const &primary_key,
                                remove_option opt = remove_option::counting);

private:
    sharksfin::TransactionControlHandle ctrl_handle_{};
    sharksfin::TransactionHandle tx_handle_{};
    std::uint64_t system_id_ {};
    std::mutex mtx_tx_{};
};
}
