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

#include <jogasaki/api/kvsservice/details/storage.h>
#include <jogasaki/api/kvsservice/details/transaction_option.h>

namespace jogasaki::api::kvsservice {

using transaction = details::transaction;
using transaction_option = details::transaction_option;
using storage = details::storage;

/**
* @brief kvs database core for remote kvs service.
*/
class database {
public:
    /**
     * @brief create new object
     */
    database() = default;

    database(database const& other) = delete;
    database& operator=(database const& other) = delete;
    database(database&& other) noexcept = delete;
    database& operator=(database&& other) noexcept = delete;

    /**
     * @brief start servicing database
     * @details database initialization is done by this function.
     * No request should be made to database prior to this call.
     * @attention this function is not thread-safe. stop()/start() should be called from single thread at a time.
     * @return status::ok when successful
     * @return other code when error
     */
    status start();

    /**
     * @brief stop servicing database
     * @details stopping database and closing internal resources.
     * No request should be made to the database after this call.
     * @attention this function is not thread-safe. stop()/start() should be called from single thread at a time.
     * @return status::ok when successful
     * @return other code when error
     */
    status stop();

    /**
     * @brief begin the new transaction
     * @param option transaction option
     * @param tx [out] the transaction filled when successful
     * @return status::ok when successful
     * @return any other error otherwise
     */
    status begin_transaction(transaction_option& option, std::unique_ptr<transaction>& tx);

    /**
     * @brief close the transaction transaction
     * @param tx the transaction
     * @return status::ok when successful
     * @return any other error otherwise
     */
    status close_transaction(transaction& tx);

    /**
     * @brief retrieve the storage on the database or create if not found
     * @param name name of the storage
     * @return storage object for the given name
     * @return nullptr if the any error occurs
     * @attention Multiple threads can call this function simultaneously to get the storages and each thread can use one
     * retrieved to update storage content. That can be done concurrently.
     * But concurrent operations for adding/removing storage entries are not strictly controlled for safety.
     * For the time being, storages are expected to be created sequentially before any transactions are started.
     * Accessing the storage object which is deleted by storage::delete_storage() causes undefined behavior.
     */
    std::unique_ptr<storage> get_or_create_storage(std::string_view name);

};

}
