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

enum class remote_catalog_source { disabled, rpc, fixture };

struct remote_catalog_options {
    remote_catalog_source source{remote_catalog_source::disabled};
    std::string fixture_path{};
};

enum class remote_declaration_kind { table, index, function, aggregate_function };

/**
 * @brief Remote-side identity of a coordinator-side declaration.
 * @details remote_definition_id is the remote declaration's definition_id as received.
 * It is neither a transfer index nor a coordinator-side definition_id.
 */
struct remote_declaration_identity {
    std::string remote_server_name{};
    remote_declaration_kind kind{remote_declaration_kind::table};
    std::uint64_t remote_definition_id{};
};

/**
 * @brief Metadata owned for the lifetime of a compilation.
 * @details Remote tables are separate from local storage. Location lookup uses
 * the same table identity as the catalog, never an unqualified table name.
 * This is the engine-side boundary for the compiler's future location query.
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
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::storage::table const& table) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::storage::index const& index) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::function::declaration const& function) const noexcept;
    [[nodiscard]] remote_declaration_identity const*
    remote_definition(yugawara::aggregate::declaration const& function) const noexcept;

    // Check ownership and placement before publishing an RPC snapshot.
    [[nodiscard]] std::string validation_error(std::vector<remote_server_configuration> const& servers) const;

private:
    catalogs_type catalogs_{};
    locations_type locations_{};
    definitions_type definitions_{};
};

/** @brief A real transport and development fixtures return the same metadata. */
class remote_catalog_provider {
public:
    remote_catalog_provider() = default;
    remote_catalog_provider(remote_catalog_provider const&) = default;
    remote_catalog_provider& operator=(remote_catalog_provider const&) = default;
    remote_catalog_provider(remote_catalog_provider&&) = default;
    remote_catalog_provider& operator=(remote_catalog_provider&&) = default;
    virtual ~remote_catalog_provider() = default;
    virtual status load(std::vector<remote_server_configuration> const& servers,
                        std::shared_ptr<remote_catalog_snapshot const>& result, std::string& diagnostic) = 0;
};

/**
 * @brief Loads one snapshot, publishing it only on success.
 * @details This startup loader is retained for development fixtures and an
 * explicitly supplied provider. Normal CREATE FOREIGN CATALOG uses
 * remote_catalog_manager and its GetRemoteCatalog RPC fetcher instead.
 * Transport failure never enables fixtures. Fixture code is excluded unless
 * ENABLE_DISTRIBUTED_QUERY_FIXTURES was enabled at build time.
 */
[[nodiscard]] status load_remote_catalogs(remote_catalog_options const& options,
                                          std::vector<remote_server_configuration> const& servers,
                                          std::shared_ptr<remote_catalog_snapshot const>& result,
                                          std::string& diagnostic, remote_catalog_provider* rpc_provider = nullptr);

[[nodiscard]] bool distributed_query_fixtures_available() noexcept;

} // namespace jogasaki::dist
