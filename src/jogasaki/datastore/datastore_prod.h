/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include "datastore.h"

#include <jogasaki/kvs/database.h>

namespace jogasaki::datastore {

class datastore_prod : public datastore {

public:
    /**
     * @brief create empty object
     */
    datastore_prod() noexcept = default;

    /**
     * @brief destruct the object
     */
    ~datastore_prod() noexcept override = default;

    datastore_prod(datastore_prod const& other) = default;
    datastore_prod& operator=(datastore_prod const& other) = default;
    datastore_prod(datastore_prod&& other) noexcept = default;
    datastore_prod& operator=(datastore_prod&& other) noexcept = default;

    /**
     * @brief create object
     */
    explicit datastore_prod(limestone::api::datastore* ds);

    /**
     * @brief return the kind of this object
     */
    datastore_kind kind() noexcept override {
        return datastore_kind::production;
    };

    /**
     * @brief acquires a new empty BLOB pool.
     * @details This pool is used for temporary registration of BLOBs,
     *      and all BLOBs that are not fully registered will become unavailable when the pool is destroyed.
     * @return the created BLOB pool
     * @see blob_pool::release()
     * @attention the returned BLOB pool must be released by the blob_pool::release() after the usage, or it may cause leaks of BLOB data.
     * @attention Undefined behavior if using pool after destroying this datastore.
     */
    [[nodiscard]] std::unique_ptr<limestone::api::blob_pool> acquire_blob_pool() override;

    /**
     * @brief returns BLOB file for the BLOB reference.
     * @param reference the target BLOB reference
     * @return the corresponding BLOB file
     * @return unavailable BLOB file if there is no BLOB file for the reference,
     *   that is, the BLOB file has not been registered or has already been
     * removed.
     * @attention the returned BLOB file is only effective
     *    during the transaction that has provided the corresponding BLOB
     * reference.
     */
    [[nodiscard]] limestone::api::blob_file get_blob_file(limestone::api::blob_id_type reference) override;

private:
    limestone::api::datastore* ds_{};
};

}  // namespace jogasaki::datastore
