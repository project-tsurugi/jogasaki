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
#include "remote_function_id_registry.h"

#include <limits>
#include <utility>

namespace jogasaki::dist {

remote_function_id_lease::remote_function_id_lease(std::weak_ptr<remote_function_id_registry> owner,
                                                   std::vector<definition_id_type> functions,
                                                   std::vector<definition_id_type> aggregate_functions) noexcept
    : owner_(std::move(owner)), functions_(std::move(functions)), aggregate_functions_(std::move(aggregate_functions)) {
}

remote_function_id_lease::~remote_function_id_lease() {
    // the registry may already be gone together with its database; then nothing remains to release
    if (auto owner = owner_.lock()) {
        owner->release(functions_, aggregate_functions_);
    }
}

std::vector<remote_function_id_lease::definition_id_type> const& remote_function_id_lease::functions() const noexcept {
    return functions_;
}

std::vector<remote_function_id_lease::definition_id_type> const&
remote_function_id_lease::aggregate_functions() const noexcept {
    return aggregate_functions_;
}

remote_function_id_registry::remote_function_id_registry(definition_id_type minimum_id) noexcept
    : minimum_id_(minimum_id == 0 ? 1 : minimum_id) {}

void remote_function_id_registry::exclude_function(definition_id_type id) {
    std::scoped_lock lock{mutex_};
    functions_.excluded.emplace(id);
}

void remote_function_id_registry::exclude_aggregate_function(definition_id_type id) {
    std::scoped_lock lock{mutex_};
    aggregate_functions_.excluded.emplace(id);
}

bool remote_function_id_registry::allocate(id_space& space, std::size_t count,
                                           std::vector<definition_id_type>& out) const {
    out.clear();
    out.reserve(count);
    for (auto candidate = minimum_id_; out.size() < count; ++candidate) {
        if (space.excluded.count(candidate) == 0 && space.reserved.count(candidate) == 0) {
            out.emplace_back(candidate);
        }
        if (candidate == std::numeric_limits<definition_id_type>::max()) {
            return out.size() == count;
        }
    }
    return true;
}

status remote_function_id_registry::reserve(std::size_t functions, std::size_t aggregate_functions,
                                            std::shared_ptr<remote_function_id_lease>& out) {
    auto owner = weak_from_this();
    if (owner.expired()) {
        // a lease could not release its IDs without a shared owner
        return status::err_invalid_state;
    }
    std::shared_ptr<remote_function_id_lease> lease{};
    {
        std::vector<definition_id_type> function_ids{};
        std::vector<definition_id_type> aggregate_ids{};
        std::scoped_lock lock{mutex_};
        if (!allocate(functions_, functions, function_ids) ||
            !allocate(aggregate_functions_, aggregate_functions, aggregate_ids)) {
            return status::err_resource_limit_reached;
        }
        try {
            functions_.reserved.insert(function_ids.begin(), function_ids.end());
            aggregate_functions_.reserved.insert(aggregate_ids.begin(), aggregate_ids.end());
            lease = std::make_shared<remote_function_id_lease>(owner, function_ids, aggregate_ids);
        } catch (...) {
            // Roll back before unlocking, so another reservation cannot acquire these IDs first.
            for (auto id : function_ids) {
                functions_.reserved.erase(id);
            }
            for (auto id : aggregate_ids) {
                aggregate_functions_.reserved.erase(id);
            }
            throw;
        }
    }
    // assigned outside the lock: a lease previously held by out may release into this registry
    out = std::move(lease);
    return status::ok;
}

bool remote_function_id_registry::function_reserved(definition_id_type id) const {
    std::scoped_lock lock{mutex_};
    return functions_.reserved.count(id) != 0;
}

bool remote_function_id_registry::aggregate_function_reserved(definition_id_type id) const {
    std::scoped_lock lock{mutex_};
    return aggregate_functions_.reserved.count(id) != 0;
}

std::size_t remote_function_id_registry::reserved_count() const {
    std::scoped_lock lock{mutex_};
    return functions_.reserved.size() + aggregate_functions_.reserved.size();
}

void remote_function_id_registry::release(std::vector<definition_id_type> const& functions,
                                          std::vector<definition_id_type> const& aggregate_functions) noexcept {
    std::scoped_lock lock{mutex_};
    for (auto id : functions) {
        functions_.reserved.erase(id);
    }
    for (auto id : aggregate_functions) {
        aggregate_functions_.reserved.erase(id);
    }
}

} // namespace jogasaki::dist
