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
#include "snapshot_remote_catalog_fetcher.h"

#include <optional>
#include <string_view>
#include <utility>

#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>

namespace jogasaki::dist {

namespace {

// the single server of all tables in the catalog, or empty if none or mixed
} // namespace

[[nodiscard]] static std::optional<std::string> placement_of(remote_catalog_snapshot const& snapshot,
                                                             yugawara::schema::catalog const& catalog) {
    std::optional<std::string> result{};
    bool mixed = false;
    catalog.schema_provider().each([&](std::shared_ptr<yugawara::schema::declaration const> const& schema) {
        if (!schema) {
            return;
        }
        auto const& storage = schema->storage_provider();
        storage.each_relation([&](std::string_view id, auto const&) {
            auto table = storage.find_table(id);
            auto const* server = table ? snapshot.remote_server(*table) : nullptr;
            if (!server || (result && *result != *server)) {
                mixed = true;
                return;
            }
            result = *server;
        });
    });
    if (mixed) {
        return std::nullopt;
    }
    return result;
}

snapshot_remote_catalog_fetcher::snapshot_remote_catalog_fetcher(
    std::shared_ptr<remote_catalog_snapshot const> source) noexcept
    : source_(std::move(source)) {}

status snapshot_remote_catalog_fetcher::fetch(remote_catalog_reference const& reference,
                                              std::shared_ptr<grpc::Channel> const&,
                                              std::chrono::system_clock::time_point, remote_catalog_information& result,
                                              std::string& diagnostic) {
    if (source_) {
        for (auto const& catalog : source_->catalogs()) {
            if (!catalog || catalog->name() != reference.catalog_name) {
                continue;
            }
            if (placement_of(*source_, *catalog) != reference.remote_server_name) {
                break;
            }
            return describe_remote_catalog(*catalog, result, diagnostic);
        }
    }
    diagnostic =
        "development snapshot has no catalog " + reference.catalog_name + " on server " + reference.remote_server_name;
    return status::err_not_found;
}

std::vector<remote_catalog_reference> snapshot_remote_catalog_fetcher::references() const {
    std::vector<remote_catalog_reference> result{};
    if (!source_) {
        return result;
    }
    for (auto const& catalog : source_->catalogs()) {
        if (!catalog) {
            continue;
        }
        if (auto server = placement_of(*source_, *catalog)) {
            result.push_back({std::string{catalog->name()}, std::move(*server)});
        }
    }
    return result;
}

} // namespace jogasaki::dist
