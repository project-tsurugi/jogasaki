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
#include "storage_manager.h"

#include <memory>
#include <string>
#include <string_view>

#include <tbb/concurrent_hash_map.h>

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

bool storage_manager::add_entry(storage_entry entry, std::string_view name) {
    bool ret = false;
    {
        decltype(storages_)::accessor acc;
        if (storages_.insert(acc, entry)) {
            acc->second = std::make_shared<impl::storage_control>(std::string{name});
            ret = true;
        }
    }
    if (ret) {
        // add name after storages_ is updated successfully
        storage_names_.emplace(std::string{name}, entry);
    }
    return ret;
}

bool storage_manager::remove_entry(storage_entry entry) {
    decltype(storages_)::accessor acc;
    if (storages_.find(acc, entry)) {
        auto name = acc->second->name();
        storage_names_.erase(std::string{name});
        storages_.erase(acc);
        return true;
    }
    return false;
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

std::pair<bool, storage_list> storage_manager::lock_internal(bool shared, storage_list_view storages, unique_lock* lock) {
    // holder holds in-flight locks until the end of this function.
    std::vector<storage_entry> locked_storages{};
    std::vector<lock_holder<std::shared_ptr<impl::storage_control>>> holders{};
    auto max_held = storages.entity().size() + (lock ? lock->storage().entity().size() : 0);
    for(auto&& e : storages.entity()) {
        if(lock != nullptr && lock->storage().contains(e)) {
            // lock already held by the given unique lock
            continue;
        }
        auto s = find_entry(e);
        if(! s || (shared && ! s->lock_shared()) || (! shared && ! s->lock())) {
            // failed to lock. In-flight lock will be released by holder.
            return {false, {}};
        }
        holders.reserve(max_held);
        holders.emplace_back(std::move(s), shared);
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

}
