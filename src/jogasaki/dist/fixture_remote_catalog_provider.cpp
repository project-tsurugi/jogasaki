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
#include "fixture_remote_catalog_provider.h"

#ifdef ENABLE_DISTRIBUTED_QUERY_FIXTURES
#include <fstream>
#include <stdexcept>
#include <unordered_set>
#include <nlohmann/json.hpp>

#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/float.h>
#include <takatori/type/int.h>
#include <takatori/util/reference_vector.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#endif

namespace jogasaki::dist {

bool distributed_query_fixtures_available() noexcept {
#ifdef ENABLE_DISTRIBUTED_QUERY_FIXTURES
    return true;
#else
    return false;
#endif
}

#ifdef ENABLE_DISTRIBUTED_QUERY_FIXTURES
using json = nlohmann::json;

static std::string name_of(json const& value, char const* key) {
    auto name = value.at(key).get<std::string>();
    if (name.empty()) {
        throw std::invalid_argument("empty fixture name");
    }
    return name;
}

static std::shared_ptr<takatori::type::data const> column_type(json const& column) {
    auto type = name_of(column, "type");
    if (type == "bigint") {
        return std::make_shared<takatori::type::int8>();
    }
    if (type == "integer") {
        return std::make_shared<takatori::type::int4>();
    }
    if (type == "real") {
        return std::make_shared<takatori::type::float4>();
    }
    if (type == "double") {
        return std::make_shared<takatori::type::float8>();
    }
    if (type == "decimal") {
        auto precision = column.at("precision").get<std::int64_t>();
        auto scale = column.at("scale").get<std::int64_t>();
        if (precision <= 0 || precision > 38 || scale < 0 || scale > precision) {
            throw std::invalid_argument("invalid decimal precision/scale");
        }
        return std::make_shared<takatori::type::decimal>(static_cast<std::size_t>(precision),
                                                         static_cast<std::size_t>(scale));
    }
    if (type == "varchar") {
        auto length = column.at("length").get<std::int64_t>();
        if (length <= 0) {
            throw std::invalid_argument("invalid varchar length");
        }
        return std::make_shared<takatori::type::character>(takatori::type::varying, static_cast<std::size_t>(length));
    }
    throw std::invalid_argument("unsupported fixture column type: " + type);
}

static void unique(std::unordered_set<std::string>& names, std::string const& name) {
    if (!names.emplace(name).second) {
        throw std::invalid_argument("duplicate fixture name: " + name);
    }
}
static std::unordered_set<std::string> collect_primary_key(json const& table,
                                                           yugawara::storage::table const& definition,
                                                           std::vector<yugawara::storage::index::key>& keys) {
    std::unordered_set<std::string> primary_key{};
    if (!table.at("primary_key").is_array() || table.at("primary_key").empty()) {
        throw std::invalid_argument("primary_key must be a nonempty array");
    }
    for (auto const& key : table.at("primary_key")) {
        auto key_name = key.get<std::string>();
        unique(primary_key, key_name);
        bool found = false;
        for (auto const& column : definition.columns()) {
            if (column.simple_name() == key_name) {
                if (column.criteria().nullity().nullable()) {
                    throw std::invalid_argument("nullable primary key");
                }
                keys.emplace_back(column);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::invalid_argument("unknown primary key column: " + key_name);
        }
    }
    return primary_key;
}

static std::shared_ptr<yugawara::storage::table> add_fixture_table(json const& table,
                                                                   yugawara::storage::configurable_provider& storage) {
    auto table_name = name_of(table, "name");
    auto definition = std::make_shared<yugawara::storage::table>(
        table_name, takatori::util::reference_vector<yugawara::storage::column>{});
    std::unordered_set<std::string> column_names{};
    if (!table.at("columns").is_array() || table.at("columns").empty()) {
        throw std::invalid_argument("columns must be a nonempty array");
    }
    for (auto const& column : table.at("columns")) {
        auto column_name = name_of(column, "name");
        unique(column_names, column_name);
        definition->columns().emplace_back(
            column_name, column_type(column),
            yugawara::variable::criteria{yugawara::variable::nullity{column.value("nullable", true)}},
            yugawara::storage::column_value{}, yugawara::storage::column_feature_set{});
    }
    std::vector<yugawara::storage::index::key> keys{};
    std::vector<yugawara::storage::index::column_ref> values{};
    auto primary_key = collect_primary_key(table, *definition, keys);
    for (auto const& column : definition->columns()) {
        if (primary_key.count(std::string{column.simple_name()}) == 0) {
            values.emplace_back(column);
        }
    }
    storage.add_table(definition);
    storage.add_index(
        yugawara::storage::index{definition,
                                 table_name,
                                 std::move(keys),
                                 std::move(values),
                                 {yugawara::storage::index_feature::primary, yugawara::storage::index_feature::find,
                                  yugawara::storage::index_feature::scan, yugawara::storage::index_feature::unique}});
    return definition;
}

static yugawara::schema::declaration
fixture_schema(json const& schema, remote_catalog_snapshot::locations_type& locations, std::string const& server) {
    auto schema_name = name_of(schema, "name");
    auto storage = std::make_shared<yugawara::storage::configurable_provider>();
    std::unordered_set<std::string> table_names{};
    if (!schema.at("tables").is_array()) {
        throw std::invalid_argument("tables must be an array");
    }
    for (auto const& table : schema.at("tables")) {
        unique(table_names, name_of(table, "name"));
        auto definition = add_fixture_table(table, *storage);
        locations.emplace(definition.get(), server);
    }
    return yugawara::schema::declaration{std::nullopt, schema_name, storage};
}

static std::shared_ptr<yugawara::schema::catalog> fixture_catalog(json const& entry,
                                                                  std::unordered_set<std::string> const& server_names,
                                                                  std::unordered_set<std::string>& catalog_names,
                                                                  remote_catalog_snapshot::locations_type& locations) {
    auto name = name_of(entry, "name");
    unique(catalog_names, name);
    auto server = name_of(entry, "server");
    if (server_names.count(server) == 0) {
        throw std::invalid_argument("unknown fixture server: " + server);
    }
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    std::unordered_set<std::string> schema_names{};
    if (!entry.at("schemas").is_array()) {
        throw std::invalid_argument("schemas must be an array");
    }
    for (auto const& schema : entry.at("schemas")) {
        unique(schema_names, name_of(schema, "name"));
        schemas->add(fixture_schema(schema, locations, server));
    }
    return std::make_shared<yugawara::schema::catalog>(name, std::nullopt, schemas);
}
#endif

status load_fixture_catalogs(std::string const& path, std::vector<remote_server_configuration> const& servers,
                             std::shared_ptr<remote_catalog_snapshot const>& result, std::string& diagnostic) {
    result.reset();
#ifndef ENABLE_DISTRIBUTED_QUERY_FIXTURES
    (void)path;
    (void)servers;
    diagnostic = "fixture catalogs were excluded at build time; enable ENABLE_DISTRIBUTED_QUERY_FIXTURES";
    return status::err_unsupported;
#else
    std::ifstream input{path};
    if (!input) {
        diagnostic = "cannot open remote catalog fixture: " + path;
        return status::err_io_error;
    }
    try {
        json root{};
        input >> root;
        if (root.at("version").get<int>() != 1) {
            throw std::invalid_argument("unsupported fixture version");
        }
        if (!root.at("catalogs").is_array()) {
            throw std::invalid_argument("catalogs must be an array");
        }
        std::unordered_set<std::string> server_names{};
        for (auto const& server : servers) {
            unique(server_names, server.name);
        }
        remote_catalog_snapshot::catalogs_type catalogs{};
        remote_catalog_snapshot::locations_type locations{};
        std::unordered_set<std::string> catalog_names{};
        for (auto const& entry : root.at("catalogs")) {
            catalogs.emplace_back(fixture_catalog(entry, server_names, catalog_names, locations));
        }
        result = std::make_shared<remote_catalog_snapshot>(std::move(catalogs), std::move(locations));
        return status::ok;
    } catch (std::exception const& e) {
        diagnostic = "invalid remote catalog fixture: " + std::string{e.what()};
        return status::err_invalid_argument;
    }
#endif
}

} // namespace jogasaki::dist
