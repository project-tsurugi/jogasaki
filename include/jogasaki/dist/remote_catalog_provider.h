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
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <yugawara/aggregate/declaration.h>
#include <yugawara/function/declaration.h>
#include <yugawara/schema/catalog.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <jogasaki/dist/remote_server_configuration.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {


enum class remote_declaration_kind { table, index, function, aggregate_function };

/**
 * @brief Remote-side identity of a coordinator-side declaration.
 * @details remote_definition_id is the remote declaration's definition_id as received.
 * It is neither a transfer index nor a coordinator-side definition_id.
 */
struct remote_declaration_identity {
    std::string remote_server_name{};
    remote_declaration_kind kind{remote_declaration_kind::table};
    std::optional<std::uint64_t> remote_definition_id{};
};

/**
 * @brief Metadata owned for the lifetime of a compilation.
 * @details Remote tables are separate from local storage. Location lookup uses
 * the same table identity as the catalog, never an unqualified table name.
 * This is the engine-side boundary for the compiler's declaration/location contract.
 */
class remote_catalog_snapshot {
public:
    using catalogs_type = std::vector<std::shared_ptr<yugawara::schema::catalog const>>;
    using locations_type = std::unordered_map<yugawara::storage::table const*, std::string>;
    // Keyed by the address of a table, index, or function declaration owned by catalogs().
    using definitions_type = std::unordered_map<void const*, remote_declaration_identity>;

    explicit remote_catalog_snapshot(catalogs_type catalogs = {}, locations_type locations = {},
                                     definitions_type definitions = {});
    [[nodiscard]] catalogs_type const& catalogs() const noexcept;
    [[nodiscard]] locations_type const& locations() const noexcept;
    [[nodiscard]] definitions_type const& definitions() const noexcept;
    [[nodiscard]] std::string const* remote_server(yugawara::storage::table const& table) const noexcept;
    /** @brief Finds the catalog owning a table declaration without looking up its name again. */
    [[nodiscard]] std::shared_ptr<yugawara::schema::catalog const>
    catalog_for(yugawara::storage::table const& table) const;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::storage::table const& table) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::storage::index const& index) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::function::declaration const& function) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::aggregate::declaration const& function) const noexcept;


private:
    catalogs_type catalogs_{};
    locations_type locations_{};
    definitions_type definitions_{};
};


} // namespace jogasaki::dist
