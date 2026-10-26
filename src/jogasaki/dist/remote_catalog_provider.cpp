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
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>

#include <jogasaki/dist/remote_catalog_provider.h>

#include "fixture_remote_catalog_provider.h"

namespace jogasaki::dist {

remote_catalog_snapshot::remote_catalog_snapshot(catalogs_type catalogs, locations_type locations,
                                                 definitions_type definitions)
    : catalogs_(std::move(catalogs)), locations_(std::move(locations)), definitions_(std::move(definitions)) {}

remote_catalog_snapshot::catalogs_type const& remote_catalog_snapshot::catalogs() const noexcept { return catalogs_; }

remote_catalog_snapshot::locations_type const& remote_catalog_snapshot::locations() const noexcept {
    return locations_;
}

remote_catalog_snapshot::definitions_type const& remote_catalog_snapshot::definitions() const noexcept {
    return definitions_;
}

std::string const* remote_catalog_snapshot::remote_server(yugawara::storage::table const& table) const noexcept {
    auto it = locations_.find(&table);
    return it == locations_.end() ? nullptr : &it->second;
}

remote_declaration_identity const*
remote_catalog_snapshot::remote_definition(yugawara::storage::table const& table) const noexcept {
    auto it = definitions_.find(&table);
    return it == definitions_.end() || it->second.kind != remote_declaration_kind::table ? nullptr : &it->second;
}

remote_declaration_identity const*
remote_catalog_snapshot::remote_definition(yugawara::storage::index const& index) const noexcept {
    auto it = definitions_.find(&index);
    return it == definitions_.end() || it->second.kind != remote_declaration_kind::index ? nullptr : &it->second;
}

remote_declaration_identity const*
remote_catalog_snapshot::remote_definition(yugawara::function::declaration const& function) const noexcept {
    auto it = definitions_.find(&function);
    return it == definitions_.end() || it->second.kind != remote_declaration_kind::function ? nullptr : &it->second;
}

remote_declaration_identity const*
remote_catalog_snapshot::remote_definition(yugawara::aggregate::declaration const& function) const noexcept {
    auto it = definitions_.find(&function);
    return it == definitions_.end() || it->second.kind != remote_declaration_kind::aggregate_function ? nullptr
                                                                                                      : &it->second;
}

std::string remote_catalog_snapshot::validation_error(std::vector<remote_server_configuration> const& servers) const {
    std::unordered_set<std::string> configured{};
    for (auto const& server : servers) {
        if (server.name.empty() || !configured.emplace(server.name).second) {
            return "invalid or duplicate remote server name";
        }
    }
    std::unordered_set<yugawara::storage::table const*> owned{};
    std::unordered_set<std::string> names{};
    std::string error{};
    for (auto const& catalog : catalogs_) {
        if (!catalog || catalog->name().empty() || !names.emplace(catalog->name()).second) {
            return "invalid or duplicate remote catalog";
        }
        std::unordered_set<std::string> schemas{};
        catalog->schema_provider().each([&](auto const& schema) {
            if (!error.empty()) return;
            if (!schema || schema->name().empty() || !schemas.emplace(schema->name()).second) {
                error = "invalid or duplicate remote schema";
                return;
            }
            schema->storage_provider().each_relation([&](auto, auto const& relation) {
                if (!error.empty()) return;
                // Use the callback relation; a provider lookup would reacquire its lock.
                auto table = std::dynamic_pointer_cast<yugawara::storage::table const>(relation);
                if (!table) {
                    error = "unsupported remote catalog relation";
                    return;
                }
                owned.emplace(table.get());
                auto* server = remote_server(*table);
                if (!server || configured.count(*server) == 0) {
                    error = "missing or unknown remote table placement";
                }
            });
        });
        if (!error.empty()) return error;
    }
    for (auto const& entry : locations_) {
        if (owned.count(entry.first) == 0) {
            return "remote placement refers to an unowned table";
        }
    }
    return {};
}

status load_remote_catalogs(remote_catalog_options const& options,
                            std::vector<remote_server_configuration> const& servers,
                            std::shared_ptr<remote_catalog_snapshot const>& result, std::string& diagnostic,
                            remote_catalog_provider* rpc_provider) {
    result.reset();
    diagnostic.clear();
    if (options.source != remote_catalog_source::fixture && !options.fixture_path.empty()) {
        diagnostic = "fixture_path requires catalog_source=fixture";
        return status::err_invalid_argument;
    }
    std::shared_ptr<remote_catalog_snapshot const> snapshot{};
    status st{};
    switch (options.source) {
        case remote_catalog_source::disabled:
            snapshot = std::make_shared<remote_catalog_snapshot>();
            st = status::ok;
            break;
        case remote_catalog_source::rpc:
            if (!rpc_provider) {
                diagnostic = "remote catalog RPC provider is not implemented";
                return status::err_not_implemented;
            }
            try {
                st = rpc_provider->load(servers, snapshot, diagnostic);
            } catch (std::exception const& e) {
                diagnostic = std::string{"remote catalog provider failed: "} + e.what();
                return status::err_io_error;
            }
            break;
        case remote_catalog_source::fixture:
            if (options.fixture_path.empty()) {
                diagnostic = "catalog_source=fixture requires fixture_path";
                return status::err_invalid_argument;
            }
            st = load_fixture_catalogs(options.fixture_path, servers, snapshot, diagnostic);
            break;
        default:
            diagnostic = "invalid remote catalog source";
            return status::err_invalid_argument;
    }
    if (st != status::ok) {
        return st;
    }
    if (!snapshot) {
        diagnostic = "remote catalog provider returned no snapshot";
        return status::err_invalid_state;
    }
    if (auto error = snapshot->validation_error(servers); !error.empty()) {
        diagnostic = std::move(error);
        return status::err_invalid_argument;
    }
    result = std::move(snapshot);
    return status::ok;
}

} // namespace jogasaki::dist
