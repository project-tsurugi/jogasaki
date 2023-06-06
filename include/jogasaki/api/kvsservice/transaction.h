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

#include <jogasaki/api/kvsservice/details/index.h>
#include <jogasaki/api/kvsservice/details/commit_option.h>
#include <jogasaki/api/kvsservice/details/put_option.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record.h>
#include <jogasaki/status.h>

namespace jogasaki::api::kvsservice {

using index = details::index;
using commit_option = details::commit_option;
using put_option = details::put_option;

/**
 * @brief a transaction of KVS database
 */
class transaction {
public:
    /**
     * @brief the callback type used for async execution
     * @see commit_async
     */
    using callback = std::function<void(status, std::string_view)>;

    /**
     * @brief create new object
     */
    transaction();

    transaction(transaction const& other) = delete;
    transaction& operator=(transaction const& other) = delete;
    transaction(transaction&& other) noexcept = delete;
    transaction& operator=(transaction&& other) noexcept = delete;

    /**
     * @brief retrieves the system_id of this transaction
     * @return the system_id of rhe transaction
     */
    std::uint64_t system_id() const noexcept;

    /**
     * @brief acquire the lock of this transaction
     */
    void lock();

    /**
     * @brief try to acquire the lock of this transaction
     * @return true the lock has been acquired
     * @return false the lock couldn't be acquired
     */
    bool try_lock();

    /**
     * @brief release the lock of this transaction
     */
    void unlock();

    /**
     * @brief commit the transaction synchronously
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @param opt commit operation behavior
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status commit(commit_option opt = commit_option::commit_type_unspecified);

    /**
     * @brief commit the transaction asynchronously
     * @param on_completion callback to be called on completion
     * @param opt commit operation behavior
     */
    void commit_async(callback on_completion,
                      commit_option opt = commit_option::commit_type_unspecified);

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();

    /**
     * @brief put the value for the given key
     * @param table the name of the table
     * @param key the key for the entry
     * @param value the value for the entry
     * @param option option to set put mode
     * @return status::ok if the operation is successful
     * @return status::err_already_exists if the option is `create` and record already exists for the key
     * @return status::err_not_found if the option is `update` and the record doesn't exist for the key
     * @return status::abort_retryable on occ error
     * @return otherwise, other status code
     * @note status::not_found is not returned even if the record doesn't exist for the key
     * @see jogasaki::api::create_parameter_set()
     */
    [[nodiscard]] status put(std::string_view table, jogasaki::api::parameter_set const &key,
                             jogasaki::api::parameter_set const &value,
                             put_option opt = put_option::create_or_update);

    /**
     * @brief get the value for the given key
     * @param index the name of the index
     * @param key key for searching
     * @param value[out] the value of the entry matching the key
     * The data pointed by the returned value gets invalidated after any other api call.
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::abort_retryable on occ error
     * @return otherwise, other status code
     * @see jogasaki::api::create_parameter_set()
     */
    [[nodiscard]] status get(index const index, jogasaki::api::parameter_set const &key,
                             jogasaki::api::record &value);

    /**
     * @brief remove the entry for the given key
     * @param table the name of the table
     * @param key the key for searching
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::abort_retryable on occ error
     * @return otherwise, other status code
     * @see jogasaki::api::create_parameter_set()
     */
    [[nodiscard]] status remove(std::string_view table, jogasaki::api::parameter_set const &key);

private:
    std::uint64_t system_id_ {};
    std::mutex mtx_tx_{}; // for lock(), try_lock(), unlock()
};
}
