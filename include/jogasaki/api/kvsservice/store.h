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
#include <optional>
#include <tbb/concurrent_hash_map.h>

#include <jogasaki/api/resource/bridge.h>
#include <sharksfin/api.h>

#include "transaction.h"
#include "transaction_option.h"
#include "transaction_result.h"

namespace jogasaki::api::kvsservice {

class store {
public:
    /**
     * @brief create new object
     */
    store() = default;

    /**
     * @brief create new object
     * @param bridge the resource of Jogasaki
     */
    explicit store(std::shared_ptr<jogasaki::api::resource::bridge> const& bridge);

    /**
     * @brief create new object
     * @param db database handle of sharksfin
     */
    explicit store(sharksfin::DatabaseHandle db);

    store(store const &other) = delete;
    store &operator=(store const &other) = delete;
    store(store &&other) noexcept = delete;
    store &operator=(store &&other) noexcept = delete;

    /**
     * @brief destructor the object
     */
    ~store() = default;

    /**
     * @brief begin the new transaction synchronously.
     * This method blocks until the transaction object is created.
     * Use transaction_begin_async() if you don't wait for a long time.
     * @param option transaction option
     * @param tx [out] the transaction filled when successful
     * @return status::ok when successful
     * @return any other error otherwise
     */
    [[nodiscard]] status transaction_begin(transaction_option const& option,
                                           std::shared_ptr<transaction>& tx);

    /**
     * @brief begin the new transaction asynchronously.
     * This method returns instantly, you can get the operation status always and
     * the transaction if succeeded from the returned future object.
     * @param option transaction option
     * @return the future object of transaction_result including operation status
     * and std::shared_ptr<transaction> if succeeded.
     */
    [[nodiscard]] std::future<transaction_result> transaction_begin_async(
            transaction_option const& option);

    /**
     * @brief find the transaction with the system_id.
     * system_id should be the return value of transaction::system_id().
     * This method is thread-safe.
     * @param system_id system_id of the transaction
     * @return the optional value with the transaction
     * @return std::nullopt if the transaction is not found
     * @see transaction::system_id()
     */
    [[nodiscard]] std::optional<std::shared_ptr<transaction>> transaction_find(std::uint64_t system_id);

    /**
     * @brief dispose the transaction
     * If the transaction is still running (e.g. commit/abort has not been requested and no abort condition has been met
     * with APIs) the transaction will be aborted (by calling transaction_abort(rollback=true)) and then disposed.
     * @param tx the transaction to be disposed
     * @return status::ok when successful
     * @return any other error otherwise
     */
    [[nodiscard]] status transaction_dispose(std::shared_ptr<transaction> tx);
private:
    sharksfin::DatabaseHandle db_;
    tbb::concurrent_hash_map<std::uint64_t, std::shared_ptr<transaction>> transactions_{};
};

}
