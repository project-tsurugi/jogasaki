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
#include <memory>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include <yugawara/schema/provider.h>
#include <yugawara/storage/provider.h>

#include <jogasaki/dist/remote_catalog_provider.h>


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

std::shared_ptr<yugawara::schema::catalog const>
remote_catalog_snapshot::catalog_for(yugawara::storage::table const& table) const {
    for (auto const& catalog : catalogs_) {
        bool owns_table = false;
        catalog->schema_provider().each([&](auto const& schema) {
            if (table.owner().get() == std::addressof(schema->storage_provider())) {
                owns_table = true;
            }
        });
        if (owns_table) {
            return catalog;
        }
    }
    return {};
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

} // namespace jogasaki::dist
