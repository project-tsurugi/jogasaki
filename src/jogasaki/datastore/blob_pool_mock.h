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

#include <limestone/api/blob_pool.h>

namespace jogasaki::datastore {

class datastore_mock;

class blob_pool_mock : public limestone::api::blob_pool {

public:
    /**
     * @brief create empty object
     */
    blob_pool_mock() noexcept = default;

    /**
     * @brief destruct the object
     */
    ~blob_pool_mock() noexcept override = default;

    blob_pool_mock(blob_pool_mock const &other) = delete;
    blob_pool_mock &operator=(blob_pool_mock const &other) = delete;
    blob_pool_mock(blob_pool_mock &&other) noexcept = delete;
    blob_pool_mock &operator=(blob_pool_mock &&other) noexcept = delete;

    /**
     * @brief create object
     */
    explicit blob_pool_mock(datastore_mock *parent);

    void release() override;

    [[nodiscard]] limestone::api::blob_id_type register_file(boost::filesystem::path const &file, bool is_temporary_file) override;

    [[nodiscard]] limestone::api::blob_id_type register_data(std::string_view data) override;

    [[nodiscard]] limestone::api::blob_id_type duplicate_data(limestone::api::blob_id_type reference) override;

    [[nodiscard]] bool released() const noexcept {
        return released_;
    }
private:

    datastore_mock *parent_{};
    bool released_{};

};

}  // namespace jogasaki::datastore
