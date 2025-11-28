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

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <tbb/concurrent_hash_map.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/auth/action_set.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/request_context.h>
#include <jogasaki/storage/shared_lock.h>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/storage/unique_lock.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/hash_combine.h>

namespace jogasaki::storage {

// the source of index ids assigned for each index
// index id is not durable and assigned arbitrarily from this source on restart
inline std::atomic_size_t index_id_src = 100;  //NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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
     * @param name the name of the index (storage)
     * @param is_primary whether this is a primary index (table) or secondary index
     */
    explicit storage_control(std::string name, bool is_primary = true) :
        name_(std::move(name)),
        is_primary_(is_primary)
    {}

    [[nodiscard]] std::string_view name() const noexcept {
        return name_;
    }

    /**
     * @brief check whether this storage represents a primary index
     * @return true if this is a primary index (table)
     * @return false if this is a secondary index
     */
    [[nodiscard]] bool is_primary() const noexcept {
        return is_primary_;
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

    /**
     * @brief getter for reference to authorized actions
     * @return reference to authorized users' actions
     */
    auth::authorized_users_action_set& authorized_actions() {
        return authorized_actions_;
    }

    /**
     * @brief getter for reference to public actions
     * @return reference to actions that are allowed for all users
     */
    auth::action_set& public_actions() {
        return public_actions_;
    }

    /**
     * @brief check whether the given user is allowed to perform all the given actions on this storage
     * @param user the user to check
     * @param actions the actions to check
     * @details check both authorized_actions and public_actions and
     * return whether the given user is allowed to perform all the actions given in `actions`.
     */
    [[nodiscard]] bool allows_user_actions(std::string_view user, auth::action_set const& actions) const {
        auto& user_actions = authorized_actions_.find_user_actions(user);
        return std::all_of(actions.begin(), actions.end(), [&](auto&& a) {
            return public_actions_.action_allowed(a) || user_actions.action_allowed(a);
        });
    }

    /**
     * @brief getter for storage key (return name if storage key is not assigned)
     */
    [[nodiscard]] std::string_view derived_storage_key() const noexcept {
        if (storage_key_) {
            return *storage_key_;
        }
        return name_;
    }

    /**
     * @brief getter for storage key
     */
    [[nodiscard]] std::optional<std::string_view> storage_key() const noexcept {
        return storage_key_;
    }
    /**
     * @brief setter for storage key
     * @param key the storage key
     */
    void storage_key(std::optional<std::string_view> key) noexcept {
        storage_key_ = key;
    }

private:
    std::atomic<lock_state> state_{};
    std::string name_{};
    std::optional<std::string> storage_key_{};
    bool is_primary_{true};
    auth::authorized_users_action_set authorized_actions_{};
    auth::action_set public_actions_{};
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
     * @param name name of the storage (must be unique)
     * @param storage_key optional storage key for the index. if not provided, the name is used as the storage key
     * @param is_primary whether this is a primary index (table) or secondary index. default is true
     * @return true if the entry was added successfully
     * @return false is the entry already exists
     */
    bool add_entry(storage_entry entry, std::string_view name, std::optional<std::string_view> storage_key = std::nullopt, bool is_primary = true);

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

    /**
     * @brief clear all storage entries
     */
    void clear();

    /**
     * @brief generate a new unique surrogate ID for an index
     * @return the newly generated surrogate ID
     * @details this function is thread-safe
     */
    [[nodiscard]] std::uint64_t generate_surrogate_id();

    /**
     * @brief initialize the next surrogate ID counter
     * @param value the value to set for the next surrogate ID
     * @details this function should only be called during database initialization/recovery.
     * it is not thread-safe and should not be called concurrently with generate_surrogate_id().
     */
    void init_next_surrogate_id(std::uint64_t value);

    /**
     * @brief get the storage key for the given index name
     * @param name the name of the index
     * @return the storage key if found
     * @return nullopt if the entry is not found
     */
    [[nodiscard]] std::optional<std::string> get_storage_key(std::string_view name) const;

    /**
     * @brief get the index name for the given storage key
     * @param storage_key the storage key
     * @return the index name if found
     * @return nullopt if the entry is not found
     */
    [[nodiscard]] std::optional<std::string> get_index_name(std::string_view storage_key) const;

private:
    tbb::concurrent_hash_map<storage_entry, std::shared_ptr<impl::storage_control>> storages_{};
    tbb::concurrent_hash_map<std::string, storage_entry> storage_names_{};
    tbb::concurrent_hash_map<std::string, storage_entry> storage_keys_{};
    std::atomic<std::uint64_t> next_surrogate_id_{1000};

    std::pair<bool, storage_list> lock_internal(bool shared, storage_list_view storages, unique_lock* lock);
};

};
