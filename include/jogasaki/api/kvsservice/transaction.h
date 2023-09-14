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
#include <map>
#include <mutex>

#include <sharksfin/api.h>
#include <tateyama/proto/kvs/data.pb.h>
#include <tateyama/proto/kvs/response.pb.h>
#include <jogasaki/api/database.h>

#include "index.h"
#include "commit_option.h"
#include "put_option.h"
#include "remove_option.h"
#include "status.h"
#include "transaction_state.h"

namespace jogasaki::api::impl {
class database;
}

namespace jogasaki::api::kvsservice {

/**
 * @brief a transaction of KVS database
 */
class alignas(64) transaction {
public:

    /**
     * @brief create new object
     * @param db database of jogasaki
     * @param handle transaction control handle of sharksfin
     */
    explicit transaction(jogasaki::api::database *db, sharksfin::TransactionControlHandle handle);

    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    /**
     * @brief destructor the object
     */
    ~transaction() = default;

    /**
     * @brief retrieves the system_id of this transaction
     * @return the system_id of the transaction
     */
    [[nodiscard]] std::uint64_t system_id() const noexcept;

    /**
     * @brief retrieves the state of this transaction
     * @return the state of the transaction
     */
    [[nodiscard]] transaction_state state() const;

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
     * You should call dispose() after commit().
     * This method doesn't wait until the state is what you wanted.
     * You can use state() method to check the state of this transaction.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     * @see state()
     */
    [[nodiscard]] status commit();

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * You should call dispose() after abort().
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();

    /**
     * @brief put the record to the table
     * @param table_name the full qualified name of the table
     * @param record the record to be put.
     * the record should always contain all columns of the table.
     * the order of each column is free, any order will be accepted.
     * Even if you want to update some columns and not to update others,
     * you should specify all columns of the table.
     * You can use get() to get all columns of the record.
     * @param opt option to set put mode
     * @return status::ok if the target content was successfully put.
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::already_exists if opt=put_option::create and found the record of same primary key
     * @return status::not_found if opt=put_option::update and couldn't find the target record to be updated
     * @return status::err_invalid_argument if the specified record doesn't contain all columns of the table
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status put(std::string_view table_name, tateyama::proto::kvs::data::Record const &record,
                             put_option opt = put_option::create_or_update);

    /**
     * @brief get the record for the given primary key
     * @param table_name the full qualified name of the table
     * @param primary_key primary key for searching.
     * the primary_key should contain all columns of the primary key.
     * the primary_key should only contain the primary key data.
     * the order of each column is free, any order will be accepted.
     * @param record[out] the record matching the primary key.
     * the record contains all columns of the table.
     * the order of each column is undefined.
     * @return status::ok if the target content was obtained successfully
     * @return status::not_found if the target content does not exist
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::err_invalid_argument if the specified key isn't a primary key or not enough
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status get(std::string_view table_name, tateyama::proto::kvs::data::Record const &primary_key,
                             tateyama::proto::kvs::data::Record &record);

    /**
     * @brief remove the record for the given primary key
     * @param table_name the full qualified name of the table
     * @param primary_key the key for searching
     * the primary_key should contain all columns of the primary key.
     * the primary_key should only contain the primary key data.
     * the order of each column is free, any order will be accepted.
     * @param opt option to set remove mode
     * @return status::ok successfully deleted the existing record
     * @return status::ok if opt=remove_option::instant and the target record is absent
     * @return status::err_inactive_transaction if the transaction is inactive and the request is rejected
     * @return status::not_found only if opt=remove_option::counting and the target record was not found
     * @return status::err_invalid_argument if the specified key isn't a primary key or not enough
     * @return otherwise if error was occurred
     */
    [[nodiscard]] status remove(std::string_view table_name,
                                tateyama::proto::kvs::data::Record const &primary_key,
                                remove_option opt = remove_option::counting);

    /**
     * @brief set commit/abort error information
     * @see get_error_info()
     */
    void set_error_info(tateyama::proto::kvs::response::Error const &error) noexcept;

    /**
     * @brief retrieves commit/abort error information
     * @return commit/abort error information after commit/abort called
     * @return cleared error object before commit/abort called
     * @see set_error_info()
     */
    [[nodiscard]] tateyama::proto::kvs::response::Error const &get_error_info() const noexcept;

    /**
     * @brief dispose the transaction
     * @details dispose the transaction.
     * You should call this after commit/abort.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status dispose();

private:
    jogasaki::api::impl::database *db_{};
    sharksfin::DatabaseHandle db_handle_{};
    sharksfin::TransactionControlHandle ctrl_handle_{};
    sharksfin::TransactionHandle tx_handle_{};
    std::uint64_t system_id_ {};
    std::mutex mtx_tx_{};

    // save commit/abort error information for GetErrorInfo
    tateyama::proto::kvs::response::Error error_{};
    // commit/abort called flag, locked by mtx_tx_
    bool commit_abort_called_{};

    status is_inactive() const noexcept;
    status get_storage(std::string_view name, sharksfin::StorageHandle &storage);
};
}
