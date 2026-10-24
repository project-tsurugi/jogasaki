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
#include "storage_manager.h"

#include <deque>
#include <memory>
#include <string>
#include <string_view>

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/subtract_vectors.h>

namespace jogasaki::storage {

std::size_t storage_manager::size() const noexcept {
    return storages_.size();
}

bool storage_manager::add_entry(
    storage_entry entry, std::optional<std::string_view> name, std::optional<std::string_view> storage_key,
    bool is_primary, std::optional<storage_entry> primary_entry, std::optional<std::string_view> original_name,
    unique_lock* lock
) {
    return add_entry_internal(entry, name, storage_key, is_primary, primary_entry, original_name, lock, std::nullopt);
}

bool storage_manager::replace_entry(
    storage_entry entry, storage_entry replaced, std::string_view name, std::optional<std::string_view> storage_key,
    bool is_primary, std::optional<storage_entry> primary_entry, unique_lock& lock
) {
    if (! can_replace(name, replaced, lock)) {
        return false;
    }
    // secondary index entries are not locked by themselves, but protected by the lock on their primary entry
    return add_entry_internal(
        entry, name, storage_key, is_primary, primary_entry, std::nullopt, is_primary ? std::addressof(lock) : nullptr,
        replaced
    );
}

bool storage_manager::can_replace(std::string_view name, storage_entry replaced, unique_lock& lock) const {
    {
        decltype(storage_names_)::const_accessor acc{};
        if (! storage_names_.find(acc, std::string{name}) || acc->second != replaced) {
            return false;
        }
    }
    // the entry to be replaced must be locked by `lock` (for secondary index, the lock is held by its primary entry)
    std::optional<storage_entry> protector{};
    {
        decltype(storages_)::const_accessor acc{};
        if (! storages_.find(acc, replaced)) {
            return false;
        }
        protector = acc->second->is_primary() ? std::optional<storage_entry>{replaced} : acc->second->primary_entry();
    }
    return protector && lock.storage().contains(*protector);
}

bool storage_manager::add_entry_internal(
    storage_entry entry, std::optional<std::string_view> name, std::optional<std::string_view> storage_key,
    bool is_primary, std::optional<storage_entry> primary_entry, std::optional<std::string_view> original_name,
    unique_lock* lock, std::optional<storage_entry> replaced
) {
    std::shared_ptr<impl::storage_control> control{};
    {
        decltype(storages_)::accessor acc;
        if (! storages_.insert(acc, entry)) {
            return false;
        }
        std::optional<std::string> name_str = name ? std::optional<std::string>{std::string{*name}} : std::nullopt;
        std::optional<std::string> original_name_str = original_name ? std::optional<std::string>{std::string{*original_name}} : std::nullopt;
        acc->second = std::make_shared<impl::storage_control>(std::move(name_str), is_primary, primary_entry, std::move(original_name_str));
        control = acc->second;
        control->storage_key(storage_key);
        if (! name) {
            control->delete_reserved(true);
        }
    }
    if (lock != nullptr) {
        // lock the entry before the name is registered below, otherwise other operation can find the entry by name
        // and lock it first. The new entry is not visible to others yet, so locking should always succeed.
        auto locked = control->lock();
        assert_with_exception(locked, entry);
        std::vector<storage_entry> list{lock->storage().entity().begin(), lock->storage().entity().end()};
        list.emplace_back(entry);
        lock->set_storage_list(storage_list{std::move(list)});
    }
    if (name) {
        // add name after storages_ is updated successfully
        if (! replaced) {
            // mark current before the name becomes visible, and revert if the name is used by another entry
            control->current(true);
            if (! storage_names_.emplace(std::string{*name}, entry)) {
                control->current(false);
            }
        } else {
            // let the name refer to this entry directly so that the name always refers to either the replaced or
            // this entry
            control->current(true);
            decltype(storage_names_)::accessor name_acc;
            if (! storage_names_.insert(name_acc, std::string{*name}) && name_acc->second != *replaced) {
                // the name has been changed by other operation after it was checked - undo adding the entry
                name_acc.release();
                if (lock != nullptr) {
                    lock->release_storage(entry);
                }
                storages_.erase(entry);
                return false;
            }
            name_acc->second = entry;
            name_acc.release();
            // the replaced entry is exclusively locked by the caller (by its primary if it's a secondary index)
            if (auto r = find_entry(*replaced)) {
                r->current(false);
            }
        }
    }
    candidate_entries_.push(entry);
    // add storage key mapping
    storage_keys_.emplace(std::string{control->derived_storage_key()}, entry);
    return true;
}

bool storage_manager::remove_entry(storage_entry entry) {
    decltype(storages_)::accessor acc;
    if (storages_.find(acc, entry)) {
        acc->second->current(false);
        auto name_opt = acc->second->name();
        // Only erase name/key maps if they still point to THIS entry.
        // They may have been remapped by a new add_entry() after reserve_delete_entry().
        if (name_opt) {
            decltype(storage_names_)::const_accessor name_acc;
            if (storage_names_.find(name_acc, std::string{name_opt.value()}) && name_acc->second == entry) {
                storage_names_.erase(name_acc);
            }
        }
        auto storage_key = std::string{acc->second->derived_storage_key()};
        {
            decltype(storage_keys_)::const_accessor key_acc;
            if (storage_keys_.find(key_acc, storage_key) && key_acc->second == entry) {
                storage_keys_.erase(key_acc);
            }
        }
        storages_.erase(acc);
        return true;
    }
    return false;
}

bool storage_manager::reserve_delete_entry(storage_entry entry) {
    decltype(storages_)::accessor acc;
    if (! storages_.find(acc, entry)) {
        return false;
    }
    auto& ctrl = *acc->second;
    ctrl.current(false);
    // name() is still valid here - erase from name map before clearing.
    // Only erase it if it still refers to this entry. It may have been re-pointed to a new entry by replace_entry()
    // (e.g. TRUNCATE TABLE replacing the entry).
    if (auto name_opt = ctrl.name()) {
        decltype(storage_names_)::const_accessor name_acc;
        if (storage_names_.find(name_acc, std::string{*name_opt}) && name_acc->second == entry) {
            storage_names_.erase(name_acc);
        }
    }
    // for pre-1.8 indices (no storage_key), promote name to storage_key before clearing name
    // so that derived_storage_key() remains usable by the maintenance thread
    if (! ctrl.storage_key()) {
        ctrl.storage_key(ctrl.name());
    }
    // clear name so get_index_name() returns nullopt for this delete-reserved entry
    ctrl.name(std::nullopt);
    ctrl.delete_reserved(true);
    return true;
}

std::vector<storage_entry>
storage_manager::get_delete_reserved_entries() {
    std::vector<storage_entry> ret{};
    std::deque<storage_entry> temp{};
    storage_entry e{};
    while (candidate_entries_.try_pop(e)) {
        decltype(storages_)::const_accessor acc{};
        if (! storages_.find(acc, e)) {
            // entry no longer in storages_: skip entirely (do not re-queue)
            continue;
        }
        if (acc->second->delete_reserved()) {
            ret.emplace_back(e);
        }
        temp.emplace_back(e);
    }
    for (auto&& t : temp) {
        candidate_entries_.push(t);
    }
    return ret;
}

std::unique_ptr<unique_lock> storage_manager::create_unique_lock() {
    return std::make_unique<unique_lock>(*this, storage_list{});
}

template<class T>
struct lock_holder {
    lock_holder() = default;

    lock_holder(T owner, bool shared) :
        owner_(std::move(owner)),
        shared_(shared)
    {}

    lock_holder(lock_holder const& other) = default;
    lock_holder& operator=(lock_holder const& other) = default;
    lock_holder(lock_holder&& other) noexcept = default;
    lock_holder& operator=(lock_holder&& other) noexcept = default;

    ~lock_holder() noexcept {
        try {
            if (holding_) {
                if (shared_) {
                    owner_->release_shared();
                    return;
                }
                owner_->release();
            }
        } catch (...) {
            LOG_LP(ERROR) << "unexpected exception occurred while releasing storage";
        }
    }
    void unhold() {
        holding_ = false;
    }
    T owner_{};  //NOLINT
    bool holding_ = true;  //NOLINT
    bool shared_ = false;  //NOLINT
};

bool storage_manager::is_current_entry(storage_entry entry) const {
    decltype(storages_)::const_accessor acc{};
    return storages_.find(acc, entry) && acc->second->current();
}

std::pair<bool, storage_list> storage_manager::lock_internal(bool shared, storage_list_view storages, unique_lock* lock) {
    // holder holds in-flight locks until the end of this function.
    std::vector<storage_entry> locked_storages{};
    std::vector<lock_holder<std::shared_ptr<impl::storage_control>>> holders{};
    auto max_held = storages.entity().size() + (lock ? lock->storage().entity().size() : 0);
    for(auto&& e : storages.entity()) {
        if(lock != nullptr && lock->storage().contains(e)) {
            // lock already held by the given unique lock, but the entry may have been dropped or replaced by DDL
            // in the same transaction after the caller resolved it. Reject it for the same reason as below.
            if(! is_current_entry(e)) {
                return {false, {}};
            }
            continue;
        }
        auto s = find_entry(e);
        if(! s || (shared && ! s->lock_shared()) || (! shared && ! s->lock())) {
            // failed to lock. In-flight lock will be released by holder.
            return {false, {}};
        }
        holders.reserve(max_held);
        auto current = s->current();
        holders.emplace_back(std::move(s), shared);
        if(! current) {
            // The entry has been dropped or replaced (e.g. by TRUNCATE) after the caller resolved it by name.
            // Its name may be gone or now refers to another entry, so locking this entry doesn't protect
            // the storage the caller is going to access by name. DDL changes the entry the name refers to only while
            // holding the exclusive lock on the entry, so checking after acquiring the lock is enough.
            // In-flight lock will be released by holder.
            return {false, {}};
        }
        locked_storages.reserve(max_held);
        locked_storages.emplace_back(e);
    }
    // all locks are acquired, so stop holder to release them
    for(auto&& l : holders) {
        l.unhold();
    }
    if (! shared && lock) {
        // for unique lock, existing locked storages and requested storages are merged and returned
        locked_storages.reserve(max_held);
        for(auto&& e : lock->storage().entity()) {
            locked_storages.emplace_back(e);
        }
        std::sort(locked_storages.begin(), locked_storages.end());
        locked_storages.erase(std::unique(locked_storages.begin(), locked_storages.end()), locked_storages.end());
    }
    return {true, storage_list{std::move(locked_storages)}};
}

bool storage_manager::add_locked_storages(storage_list_view storages, unique_lock& lock) {
    auto [success, list] = lock_internal(false, storages, std::addressof(lock));
    if (! success) {
        return false;
    }
    lock.set_storage_list(std::move(list));
    return true;
}

std::unique_ptr<shared_lock> storage_manager::create_shared_lock(storage_list_view storages, unique_lock* parent) {
    auto [success, list] = lock_internal(true, storages, parent);
    if (! success) {
        return {};
    }
    return std::make_unique<shared_lock>(*this, std::move(list));
}

std::shared_ptr<impl::storage_control> storage_manager::find_entry(storage_entry entry) {
    decltype(storages_)::const_accessor acc{};
    if (storages_.find(acc, entry)) {
        return acc->second;
    }
    return {};
}

std::optional<storage_entry> storage_manager::find_by_name(std::string_view name) {
    decltype(storage_names_)::const_accessor acc{};
    if (storage_names_.find(acc, std::string{name})) {
        return acc->second;
    }
    return {};
}

void storage_manager::remove_locked_storages(storage_list_view storages, unique_lock& lock) {
    for(auto&& e : storages.entity()) {
        assert_with_exception(lock.storage().contains(e), lock.storage());
        auto s = find_entry(e);
        if (s) {
            s->release();
        }
    }
    // TODO implement subtract method for storage_list
    std::vector<storage_entry> list{lock.storage().entity().begin(), lock.storage().entity().end()};
    std::vector<storage_entry> to_remove{storages.entity().begin(), storages.entity().end()};
    lock.set_storage_list(storage_list{utils::subtract_vectors(list, to_remove)});
}

void storage_manager::clear() {
    storages_.clear();
    storage_names_.clear();
    storage_keys_.clear();
    storage_entry e{};
    while (candidate_entries_.try_pop(e)) {}
}

std::uint64_t storage_manager::generate_surrogate_id() {
    return next_surrogate_id_.fetch_add(1);
}

void storage_manager::init_next_surrogate_id(std::uint64_t value) {
    next_surrogate_id_.store(value);
}

std::optional<std::string> storage_manager::get_storage_key(std::string_view name) const {
    storage_entry entry{};
    {
        // release the accessor before accessing storages_. DDL erases the name while holding the accessor of
        // storages_, so holding both in the reverse order causes deadlock.
        decltype(storage_names_)::const_accessor acc{};
        if (! storage_names_.find(acc, std::string{name})) {
            return std::nullopt;
        }
        entry = acc->second;
    }
    return get_storage_key(entry);
}

std::optional<std::string> storage_manager::get_storage_key(storage_entry entry) const {
    decltype(storages_)::const_accessor acc{};
    if (! storages_.find(acc, entry)) {
        return std::nullopt;
    }
    return std::string{acc->second->derived_storage_key()};
}

std::optional<std::string> storage_manager::get_index_name(std::string_view storage_key) const {
    storage_entry entry{};
    {
        // release the accessor before accessing storages_ for the same reason as get_storage_key()
        decltype(storage_keys_)::const_accessor acc{};
        if (! storage_keys_.find(acc, std::string{storage_key})) {
            return std::nullopt;
        }
        entry = acc->second;
    }

    decltype(storages_)::const_accessor storages_acc{};
    if (! storages_.find(storages_acc, entry)) {
        return std::nullopt;
    }
    auto n = storages_acc->second->name();
    if (! n) {
        return std::nullopt;
    }
    return std::string{*n};
}

}
