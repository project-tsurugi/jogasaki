/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <memory>
#include <string>
#include <string_view>

#include <jogasaki/storage/storage_list.h>

namespace jogasaki::storage {

class storage_manager;

class shared_lock {
public:

    /**
     * @brief create empty object
     */
    shared_lock() = default;

    /**
     * @brief destruct the object
     */
    ~shared_lock() noexcept;

    shared_lock(shared_lock const& other) = delete;
    shared_lock& operator=(shared_lock const& other) = delete;
    shared_lock(shared_lock&& other) noexcept = delete;
    shared_lock& operator=(shared_lock&& other) noexcept = delete;

    shared_lock(storage_manager& manager, storage_list storages);

    storage_list_view storage();

private:
    storage_manager* manager_{};
    storage_list storages_{};
};

};
