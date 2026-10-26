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
#include "remote_catalog_information.h"

#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <yugawara/aggregate/declaration.h>
#include <yugawara/aggregate/provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/function/provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/provider.h>
#include <yugawara/storage/table.h>

namespace jogasaki::dist {

namespace {
class catalog_description {
public:
    remote_catalog_information release_result() noexcept { return std::move(result); }
    std::string const& diagnostic() const noexcept { return error; }
    bool is_unsupported() const noexcept { return unsupported; }
    void enumeration_failed(std::string message) { error = std::move(message); unsupported = false; }
    void describe_schema(std::shared_ptr<yugawara::schema::declaration const> const& schema) {
        if (!error.empty()) return;
        if (!schema) { error = "null schema declaration"; return; }
        remote_catalog_information::schema out_schema{std::string{schema->name()}, {}, {}, {}, {}};
        schema->function_provider().each([&](std::shared_ptr<yugawara::function::declaration const> const& f) {
            if (!f || !function_indices.emplace(f.get(), result.functions.size()).second) return;
            remote_catalog_information::function out{
                f->definition_id(), std::string{f->name()}, {}, clone(f->return_type()), f->features()};
            for (auto const& parameter : f->parameter_types()) {
                out.parameter_types.emplace_back(clone(parameter));
            }
            out_schema.functions.emplace_back(result.functions.size());
            result.functions.push_back(std::move(out));
        });
        schema->set_function_provider().each([&](std::shared_ptr<yugawara::aggregate::declaration const> const& f) {
            if (!f || !function_indices.emplace(f.get(), result.aggregate_functions.size()).second) return;
            remote_catalog_information::aggregate_function out{
                f->definition_id(), std::string{f->name()}, {}, clone(f->return_type()), f->incremental()};
            for (auto const& parameter : f->parameter_types()) {
                out.parameter_types.emplace_back(clone(parameter));
            }
            out_schema.aggregate_functions.emplace_back(result.aggregate_functions.size());
            result.aggregate_functions.push_back(std::move(out));
        });
        auto const& storage = schema->storage_provider();
        // Provider callbacks run under a non-recursive shared_mutex. Retain the
        // declarations here, then enumerate their indexes after the callback ends.
        // This preserves declaration lifetime, not an atomic snapshot across DDL.
        std::vector<std::shared_ptr<yugawara::storage::table const>> tables{};
        storage.each_relation([&](std::string_view id, auto const& relation) {
            if (!error.empty()) return;
            auto table = std::dynamic_pointer_cast<yugawara::storage::table const>(relation);
            if (!table) {
                error = "unsupported relation kind: " + std::string{id};
                unsupported = true;
                return;
            }
            tables.emplace_back(std::move(table));
        });
        for (auto const& table : tables) {
            describe_table(storage, table, out_schema);
        }
        result.schemas.push_back(std::move(out_schema));
    }
private:
    using index_type = remote_catalog_information::index_type;
    remote_catalog_information result{};
    std::unordered_map<yugawara::storage::table const*, index_type> table_indices{};
    std::unordered_map<yugawara::storage::index const*, index_type> index_indices{};
    std::unordered_map<void const*, index_type> function_indices{};
    std::string error{};
    bool unsupported = false;
    static remote_catalog_information::type_ptr clone(takatori::type::data const& type) {
        return remote_catalog_information::type_ptr{type.clone()};
    }
    void describe_table(yugawara::storage::provider const& storage,
        std::shared_ptr<yugawara::storage::table const> const& table,
        remote_catalog_information::schema& out_schema) {
        if (!error.empty()) return;
        if (!table_indices.emplace(table.get(), result.tables.size()).second) return;
        remote_catalog_information::table out_table{
            table->definition_id(), std::string{table->simple_name()}, {}};
        std::unordered_map<yugawara::storage::column const*, index_type> columns{};
        for (auto const& column : table->columns()) {
            columns.emplace(&column, result.columns.size());
            out_table.columns.emplace_back(result.columns.size());
            result.columns.push_back({std::string{column.simple_name()},
                std::shared_ptr<takatori::type::data const>{column.type().clone()},
                column.criteria().nullity().nullable()});
        }
        out_schema.tables.emplace_back(result.tables.size());
        result.tables.push_back(std::move(out_table));
        storage.each_table_index(*table,
            [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& index) {
                describe_index(index, table, columns, out_schema);
            });
    }
    void describe_index(std::shared_ptr<yugawara::storage::index const> const& index,
        std::shared_ptr<yugawara::storage::table const> const& table,
        std::unordered_map<yugawara::storage::column const*, index_type> const& columns,
        remote_catalog_information::schema& out_schema) {
        if (!error.empty() || !index) return;
        if (!index_indices.emplace(index.get(), result.indices.size()).second) return;
        remote_catalog_information::index out_index{index->definition_id(),
            std::string{index->simple_name()}, table_indices.at(table.get()), index->features(), {}, {}};
        for (auto const& key : index->keys()) {
            auto it = columns.find(&key.column());
            if (it == columns.end()) { error = "index key refers to a foreign column"; return; }
            out_index.keys.push_back({it->second, key.direction()});
        }
        for (auto const& value : index->values()) {
            auto it = columns.find(&static_cast<yugawara::storage::column const&>(value));
            if (it == columns.end()) { error = "index value refers to a foreign column"; return; }
            out_index.values.emplace_back(it->second);
        }
        out_schema.indices.emplace_back(result.indices.size());
        result.indices.push_back(std::move(out_index));
    }
};
} // namespace

status describe_remote_catalog(
    yugawara::schema::catalog const& source,
    remote_catalog_information& destination,
    std::string& diagnostic
) {
    catalog_description description{};
    try {
        source.schema_provider().each([&](std::shared_ptr<yugawara::schema::declaration const> const& schema) {
            description.describe_schema(schema);
        });
    } catch (std::exception const& e) {
        description.enumeration_failed(e.what());
    }
    if (!description.diagnostic().empty()) {
        diagnostic = "cannot describe remote catalog: " + description.diagnostic();
        return description.is_unsupported() ? status::err_unsupported : status::err_unknown;
    }
    destination = description.release_result();
    return status::ok;
}

} // namespace jogasaki::dist
