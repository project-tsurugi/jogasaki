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
#include <tbb/concurrent_hash_map.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/request_context.h>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/storage/unique_lock.h>
#include <jogasaki/storage/shared_lock.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/hash_combine.h>
#include "jogasaki/utils/assert.h"

namespace jogasaki::storage {

// the source of table ids assigned for each table
// table id is not durable and assigned arbitrarily from this source on restart
inline std::atomic_size_t table_id_src = 100;  //NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

namespace impl {

struct lock_state {
    bool write_locked:1;
    std::size_t read_lock_count:63;
};

static_assert(sizeof(lock_state) == 8);

class cache_align storage_control {
public:

    /**
     * @brief create empty object
     */
    storage_control() = default;

    /**
     * @brief destruct the object
     */
    ~storage_control() = default;

    storage_control(storage_control const& other) = delete;
    storage_control& operator=(storage_control const& other) = delete;
    storage_control(storage_control&& other) noexcept = delete;
    storage_control& operator=(storage_control&& other) noexcept = delete;

    /**
     * @brief create named object
     */
    explicit storage_control(std::string name) :
        name_(std::move(name))
    {}

    [[nodiscard]] std::string_view name() const noexcept {
        return name_;
    }

    bool lock() {
        auto cur = state_.load();
        if(cur.read_lock_count != 0 || cur.write_locked) {
            return false;
        }
        auto desired = cur;
        desired.write_locked = true;
        return state_.compare_exchange_strong(cur, desired);
    }

    bool can_lock() {
        auto cur = state_.load();
        return cur.read_lock_count == 0 && ! cur.write_locked;
    }

    bool lock_shared() {
        auto cur = state_.load();
        lock_state desired{};
        do {
            if(cur.write_locked) {
                return false;
            }
            desired = cur;
            ++desired.read_lock_count;
        } while(! state_.compare_exchange_weak(cur, desired));
        return true;
    }

    bool can_lock_shared() {
        return ! state_.load().write_locked;
    }

    void release() {
        auto cur = state_.load();
        lock_state desired{};
        do {
            assert_with_exception(cur.write_locked, static_cast<bool>(cur.write_locked));
            assert_with_exception(cur.read_lock_count == 0, static_cast<std::size_t>(cur.read_lock_count));
            desired = cur;
            desired.write_locked = false;
        } while(! state_.compare_exchange_weak(cur, desired));
    }

    void release_shared() {
        auto cur = state_.load();
        lock_state desired{};
        do {
            assert_with_exception(! cur.write_locked, static_cast<bool>(cur.write_locked));
            assert_with_exception(cur.read_lock_count > 0, static_cast<std::size_t>(cur.read_lock_count));
            desired = cur;
            --desired.read_lock_count;
        } while(! state_.compare_exchange_weak(cur, desired));
    }

private:
    std::atomic<lock_state> state_{};
    std::string name_{};
};

} // namespace impl

class storage_manager {
public:
    /**
     * @brief create empty object
     */
    storage_manager() = default;

    /**
     * @brief destruct the object
     */
    ~storage_manager() = default;

    storage_manager(storage_manager const& other) = delete;
    storage_manager& operator=(storage_manager const& other) = delete;
    storage_manager(storage_manager&& other) noexcept = delete;
    storage_manager& operator=(storage_manager&& other) noexcept = delete;

    /**
     * @brief return the storage entry count
     * @return the number of storage entries in the manager
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief add new storage entry with name
     * @param entry new storage entry to add
     * @param name name of the storage (must be unique among same kind of storages)
     * @return true if the entry was added successfully
     * @return false is the entry already exists
     */
    bool add_entry(storage_entry entry, std::string_view name);

    /**
     * @brief remove storage entry
     * @param entry storage entry to remove
     * @return true if the entry was removed successfully
     * @return false is the entry doesn't exist
     */
    bool remove_entry(storage_entry entry);

    /**
     * @brief find storage entry by its identifier
     * @param entry
     * @return the storage control object for the entry
     * @return nullptr if the entry is not found
     */
    std::shared_ptr<impl::storage_control> find_entry(storage_entry entry);

    /**
     * @brief find storage key by the name
     * @param name the name of the storage to find
     * @return the storage key if found
     * @return nullopt if the entry is not found
     */
    std::optional<storage_entry> find_by_name(std::string_view name);

    /**
     * @brief create new unique lock object
     * @return new unique lock object with empty storage list (i.e. no storages locked yet)
     */
    std::unique_ptr<unique_lock> create_unique_lock();

    /**
     * @brief acquire unique locks for given storages and add them to the existing unique_lock object
     * @param storages list of storages to lock
     * @param lock unique lock RAII object to add more storages.
     * @details The unique locks are acquired for the storage entries and the storage list held by `lock` is updated with the new ones.
     * @return true if all storages were successfully locked and added to the lock
     * @return false if one of the storages failed to lock
     */
    bool add_locked_storages(storage_list_view storages, unique_lock& lock);

    /**
     * @brief release unique locks for given storages and remove them from the existing unique_lock object
     * @param storages list of storages to release
     * @param lock unique lock RAII object to remove storages.
     * @pre `lock` must hold locks for all storages in `storages`
     * @details The unique locks are release for the storage entries and the storage list held by `lock` is updated with the removed ones.
     * The behavior is undefined if `lock` does not hold locks for all storages in `storages`.
     */
    void remove_locked_storages(storage_list_view storages, unique_lock& lock);

    /**
     * @brief acquire locks for given storages and return RAII object to release them
     * @param storages list of storages to lock
     * @param parent optional unique lock to inherit from. If provided, shared locks are acquired for the
     * storage entries except ones included in the parent lock, assuming caller already own locks from parent.
     * @return shared_lock object that will release the locks
     * @return nullptr if acquiring one of the storages failed
     */
    std::unique_ptr<shared_lock> create_shared_lock(storage_list_view storages, unique_lock* parent = nullptr);

private:
    tbb::concurrent_hash_map<storage_entry, std::shared_ptr<impl::storage_control>> storages_{};
    tbb::concurrent_hash_map<std::string, storage_entry> storage_names_{};

    std::pair<bool, storage_list> lock_internal(bool shared, storage_list_view storages, unique_lock* lock);
};

};
