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

#include <jogasaki/status.h>

namespace jogasaki::api::kvsservice {
class database;
}

namespace jogasaki::api::kvsservice::details {

class transaction {
public:
    /**
     * @brief create empty object
     */
    transaction() = default;

    /**
     * @brief create new object
     * @param db the parent database that the transaction runs on
     */
    explicit transaction(jogasaki::api::kvsservice::database *db) noexcept;

    /**
     * @brief destruct the object
     */
    ~transaction() = default;

    transaction(transaction const& other) = default;
    transaction& operator=(transaction const& other) = default;
    transaction(transaction&& other) noexcept = default;
    transaction& operator=(transaction&& other) noexcept = default;

    jogasaki::api::kvsservice::database *database() const noexcept {
        return database_;
    }

    /**
     * @brief commit the transaction
     * @details commit the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status commit(bool async = false);

    /**
     * @brief abort the transaction
     * @details abort the current transaction. When successful,
     * the object gets invalidated and should not be used any more.
     * @return status::ok if the operation is successful
     * @return other status code when error occurs
     */
    [[nodiscard]] status abort();
private:
    jogasaki::api::kvsservice::database *database_ {};
};

}
