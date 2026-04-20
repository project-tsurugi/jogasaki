/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <unordered_set>

#include <jogasaki/storage/storage_list.h>

namespace jogasaki::storage {

class storage_manager;

/**
 * @brief RAII scope that keeps storage entries referenced to prevent their deletion.
 * @details Maintains a set of referenced storage entries. Each entry's
 * ref_transaction_count is incremented when the entry is added and decremented
 * when this object is destroyed. Idempotent: adding the same entry more than
 * once has no additional effect. Thread-safe for concurrent add_storage() calls.
 */
class reference_scope {
public:
    /**
     * @brief create empty object
     */
    reference_scope() = default;

    /**
     * @brief destruct the object, decrementing ref_transaction_count for all referenced entries
     */
    ~reference_scope() noexcept;

    reference_scope(reference_scope const& other) = delete;
    reference_scope& operator=(reference_scope const& other) = delete;
    reference_scope(reference_scope&& other) noexcept = delete;
    reference_scope& operator=(reference_scope&& other) noexcept = delete;

    /**
     * @brief create a reference_scope associated with the given storage manager
     * @param manager the storage manager that owns the storage entries
     */
    explicit reference_scope(storage_manager& manager);

    /**
     * @brief add a storage entry to the reference set
     * @param entry the storage entry to reference
     * @details If the entry is already referenced, this is a no-op.
     * Thread-safe.
     */
    void add_storage(storage_entry entry);

private:
    storage_manager* manager_{};
    std::unordered_set<storage_entry> storages_{};
    std::mutex mutex_{};
};

} // namespace jogasaki::storage
