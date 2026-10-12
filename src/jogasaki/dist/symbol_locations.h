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


#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <yugawara/aggregate/provider.h>
#include <yugawara/function/provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/provider.h>

#include <jogasaki/dist/remote_catalog_provider.h>

namespace jogasaki::dist {
struct location_info {
    using location_id_type = std::uint64_t;
    static constexpr location_id_type local_location_id = 0;
    location_id_type id{};
    std::string name{};
};
/** @brief Location IDs for one database runtime, never reused until restart. */
class location_registry {
public:
    using id_type = location_info::location_id_type;
    location_registry() { locations_.emplace(0, location_info{0, "local"}); }
    id_type register_server(std::string name) {
        if (name.empty() || name == "local") {
            throw std::invalid_argument("invalid remote location name");
        }
        std::scoped_lock lock{mutex_};
        if (auto it = names_.find(name); it != names_.end()) {
            return it->second;
        }
        if (next_ == std::numeric_limits<id_type>::max()) {
            throw std::overflow_error("location IDs exhausted");
        }
        auto id = next_;
        locations_.emplace(id, location_info{id, name});
        try {
            names_.emplace(std::move(name), id);
        } catch (...) {
            locations_.erase(id);
            throw;
        }
        ++next_;
        return id;
    }
    location_info of(id_type id) const {
        std::scoped_lock lock{mutex_};
        return locations_.at(id);
    }

private:
    mutable std::mutex mutex_{};
    id_type next_{1};
    std::unordered_map<std::string, id_type> names_{};
    std::unordered_map<id_type, location_info> locations_{};
};
/**
 * @brief Engine implementation boundary for L3-03 symbol placement.
 * @details Formal Yugawara base class is not available yet. Catalogs and placement are captured
 * together from the same immutable snapshot. Unknown declarations return no location; they are
 * never guessed to be local by name. Local tables are resolved by provider identity.
 */
class symbol_locations {
public:
    using id_type = location_info::location_id_type;
    using id_set_type = std::set<id_type>;
    symbol_locations(std::shared_ptr<remote_catalog_snapshot const> snapshot,
                     std::shared_ptr<location_registry> const& registry,
                     std::shared_ptr<yugawara::storage::provider const> local = {},
                     std::shared_ptr<yugawara::function::provider const> functions = {},
                     std::shared_ptr<yugawara::aggregate::provider const> aggregates = {},
                     std::shared_ptr<yugawara::variable::provider const> variables = {})
        : snapshot_(std::move(snapshot)), local_(std::move(local)), functions_(std::move(functions)),
          aggregates_(std::move(aggregates)), variables_(std::move(variables)) {
        if (!snapshot_ || !registry) {
            throw std::invalid_argument("catalog snapshot and registry are required");
        }
        infos_.emplace(0, registry->of(0));
        available_.insert(0);
        for (auto const& catalog : snapshot_->catalogs()) {
            catalog->schema_provider().each([&](auto const& schema) {
                auto const& provider = schema->storage_provider();
                provider.each_relation([&](std::string_view name, auto const&) {
                    auto table = provider.find_table(name);
                    if (!table) {
                        throw std::invalid_argument("unsupported remote relation");
                    }
                    auto server = snapshot_->remote_server(*table);
                    if (!server) {
                        throw std::invalid_argument("remote table has no placement");
                    }
                    auto id = registry->register_server(*server);
                    tables_.emplace(table.get(), id);
                    infos_.emplace(id, registry->of(id));
                    available_.insert(id);
                });
            });
        }
    }
    remote_catalog_snapshot::catalogs_type const& external_catalogs() const noexcept { return snapshot_->catalogs(); }
    location_info const& of(id_type id) const { return infos_.at(id); }
    id_set_type const& available_location_ids() const noexcept { return available_; }
    id_set_type find(yugawara::storage::table const& table) const {
        if (auto it = tables_.find(&table); it != tables_.end()) {
            return {it->second};
        }
        if (local_ && local_->find_table(table.simple_name()).get() == &table) {
            return {0};
        }
        return {};
    }
    id_set_type find(yugawara::function::declaration const& symbol) const {
        return contains(functions_, symbol) ? id_set_type{0} : id_set_type{};
    }
    id_set_type find(yugawara::aggregate::declaration const& symbol) const {
        return contains(aggregates_, symbol) ? id_set_type{0} : id_set_type{};
    }
    id_set_type find(yugawara::variable::declaration const& symbol) const {
        return contains(variables_, symbol) ? id_set_type{0} : id_set_type{};
    }

private:
    template <class Provider, class Symbol>
    static bool contains(std::shared_ptr<Provider const> const& provider, Symbol const& symbol) {
        if (!provider) {
            return false;
        }
        bool found{};
        provider->each([&](auto const& declaration) { found = found || declaration.get() == &symbol; });
        return found;
    }
    std::shared_ptr<remote_catalog_snapshot const> snapshot_{};
    std::shared_ptr<yugawara::storage::provider const> local_{};
    std::shared_ptr<yugawara::function::provider const> functions_{};
    std::shared_ptr<yugawara::aggregate::provider const> aggregates_{};
    std::shared_ptr<yugawara::variable::provider const> variables_{};
    std::unordered_map<yugawara::storage::table const*, id_type> tables_{};
    std::unordered_map<id_type, location_info> infos_{};
    id_set_type available_{};
};
} // namespace jogasaki::dist
