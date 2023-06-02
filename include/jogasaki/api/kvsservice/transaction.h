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

#include <jogasaki/api/kvsservice/details/index.h>
#include <jogasaki/api/kvsservice/details/put_option.h>
#include <jogasaki/api/kvsservice/transaction_info.h>
#include <jogasaki/status.h>

namespace jogasaki::api::kvsservice {

using index = details::index;

/**
 * @brief a transaction of KVS database
 */
class transaction {
public:
    transaction() = default;

    /**
     * @brief retrieves the transaction_info of this transaction
     * @return the transaction_info of this transaction
     */
    [[nodiscard]] const transaction_info &info() const noexcept;

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @param tx the transaction
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
     // FIXME support commit type (and async commit?)
    [[nodiscard]] status commit();

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @param tx the transaction
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();

    /**
     * @brief put the value for the given key
     * @param tx transaction used
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
     */
    [[nodiscard]] status put(std::string_view table, std::string_view key, std::string_view value,
               enum details::put_option opt = details::put_option::create_or_update);

    /**
     * @brief get the value for the given key
     * @param tx transaction used for the point query
     * @param index the name of the index
     * @param key key for searching
     * @param value[out] the value of the entry matching the key
     * The data pointed by the returned value gets invalidated after any other api call.
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::abort_retryable on occ error
     * @return otherwise, other status code
     */
    [[nodiscard]] status get(index index, std::string_view key, std::string_view& value);

    /**
     * @brief remove the entry for the given key
     * @param tx transaction used for the delete operation
     * @param table the name of the table
     * @param key the key for searching
     * @return status::ok if the operation is successful
     * @return status::not_found if the entry for the key is not found
     * @return status::abort_retryable on occ error
     * @return otherwise, other status code
     */
    [[nodiscard]] status remove(std::string_view table, std::string_view key);

private:
    transaction_info info_{};
};
}
