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

#include <mutex>

#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/kvsservice/transaction.h>
#include <jogasaki/api/kvsservice/details/transaction_option.h>

namespace jogasaki::kvs {
class database;
}

namespace jogasaki::api::kvsservice {

using transaction_option = details::transaction_option;

/**
* @brief kvs database core for remote kvs service.
*/
class database {
public:
    /**
     * @brief create new object
     */
    database() = default;

    /**
     * @brief create new object
     * @param bridge the resource of Jogasaki
     */
    database(std::shared_ptr<jogasaki::api::resource::bridge> bridge);

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
    [[nodiscard]] status start();

    /**
     * @brief stop servicing database
     * @details stopping database and closing internal resources.
     * No request should be made to the database after this call.
     * @attention this function is not thread-safe. stop()/start() should be called from single thread at a time.
     * @return status::ok when successful
     * @return other code when error
     */
    [[nodiscard]] status stop();

    /**
     * @brief begin the new transaction
     * @param option transaction option
     * @param tx [out] the transaction filled when successful
     * @return status::ok when successful
     * @return any other error otherwise
     */
    [[nodiscard]] status begin_transaction(const transaction_option& option, std::shared_ptr<transaction>& tx);

    /**
     * @brief close the transaction
     * @param tx the transaction
     * @return status::ok when successful
     * @return any other error otherwise
     */
    [[nodiscard]] status close_transaction(std::shared_ptr<transaction> tx);

    /**
     * @brief find a transaction by system_id
     * @param system_id id of the transaction
     * @return the transaction
     * @return nullptr if not found
     */
    [[nodiscard]] std::shared_ptr<transaction> find_transaction(std::uint64_t system_id);

private:
    std::shared_ptr<kvs::database> kvs_db_{};
    std::map<std::uint64_t, std::shared_ptr<transaction>> id2tx_map_ {};
    std::mutex mtx_id2tx_map_{};
};

}
