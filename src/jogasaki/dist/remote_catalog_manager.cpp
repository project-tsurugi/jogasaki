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
#include "remote_catalog_manager.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <takatori/type/table.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/reference_vector.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/utils/finally.h>

namespace jogasaki::dist {

namespace {

using index_type = remote_catalog_information::index_type;

} // namespace

[[nodiscard]] static bool valid_utf8(std::string_view text) noexcept {
    std::size_t i = 0;
    while (i < text.size()) {
        auto c = static_cast<unsigned char>(text[i]);
        if (c < 0x80U) {
            ++i;
            continue;
        }
        std::size_t length{};
        std::uint32_t code{};
        std::uint32_t minimum{};
        if ((c & 0xE0U) == 0xC0U) {
            length = 2;
            code = c & 0x1FU;
            minimum = 0x80U;
        } else if ((c & 0xF0U) == 0xE0U) {
            length = 3;
            code = c & 0x0FU;
            minimum = 0x800U;
        } else if ((c & 0xF8U) == 0xF0U) {
            length = 4;
            code = c & 0x07U;
            minimum = 0x10000U;
        } else {
            return false;
        }
        if (text.size() - i < length) {
            return false;
        }
        for (std::size_t k = 1; k < length; ++k) {
            auto next = static_cast<unsigned char>(text[i + k]);
            if ((next & 0xC0U) != 0x80U) {
                return false;
            }
            code = (code << 6U) | (next & 0x3FU);
        }
        // overlong forms, surrogates, and values above U+10FFFF
        if (code < minimum || code > 0x10FFFFU || (code >= 0xD800U && code <= 0xDFFFU)) {
            return false;
        }
        i += length;
    }
    return true;
}

[[nodiscard]] static bool valid_name(std::string_view name) noexcept {
    return !name.empty() && std::find(name.begin(), name.end(), '\0') == name.end() && valid_utf8(name);
}

namespace {

// Column types accepted by local table definitions; remote execution does not widen them (Current Jogasaki Execution
// Constraints).
} // namespace

[[nodiscard]] static bool supported_column_type(takatori::type::data const& type) noexcept {
    using takatori::type::type_kind;
    switch (type.kind()) {
        case type_kind::int4:
        case type_kind::int8:
        case type_kind::float4:
        case type_kind::float8:
        case type_kind::decimal:
        case type_kind::character:
        case type_kind::octet:
        case type_kind::date:
        case type_kind::time_of_day:
        case type_kind::time_point:
        case type_kind::blob:
        case type_kind::clob:
            return true;
        default:
            return false;
    }
}

namespace {

/**
 * @brief parameter and return types of function declarations other than the table type.
 * @details Function declarations are name and type resolution metadata only (L3-02); they are not executed remotely,
 * so built-in signatures with types that table columns reject are accepted. Types outside remote execution
 * RemoteDataType (array, record, row_reference, declared, extension, row_id) and types Tsurugi does not support (bit,
 * interval) are not.
 */
} // namespace

[[nodiscard]] static bool supported_signature_type(takatori::type::data const& type) noexcept {
    using takatori::type::type_kind;
    switch (type.kind()) {
        case type_kind::boolean:
        case type_kind::int1:
        case type_kind::int2:
        case type_kind::unknown:
            return true;
        default:
            return supported_column_type(type);
    }
}

[[nodiscard]] static std::string validate_return_type(remote_catalog_information::type_ptr const& type,
                                                      bool table_valued) {
    if (!type) {
        return "missing return type";
    }
    if (!table_valued) {
        return supported_signature_type(*type) ? std::string{} : "unsupported return type";
    }
    if (type->kind() != takatori::type::type_kind::table) {
        return "table-valued function must return a table type";
    }
    std::unordered_set<std::string> names{};
    for (auto const& column : takatori::util::unsafe_downcast<takatori::type::table>(*type).columns()) {
        if (!valid_name(column.name())) {
            return "invalid table type column name";
        }
        if (!names.emplace(column.name()).second) {
            return "duplicate table type column name";
        }
        if (!supported_signature_type(column.type())) {
            return "unsupported table type column type";
        }
    }
    return {};
}

[[nodiscard]] static std::string
validate_parameter_types(std::vector<remote_catalog_information::type_ptr> const& types) {
    for (auto const& type : types) {
        if (!type) {
            return "missing parameter type";
        }
        if (!supported_signature_type(*type)) {
            return "unsupported parameter type";
        }
    }
    return {};
}

[[nodiscard]] static bool same_types(std::vector<remote_catalog_information::type_ptr> const& a,
                                     std::vector<remote_catalog_information::type_ptr> const& b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (!(*a[i] == *b[i])) {
            return false;
        }
    }
    return true;
}

namespace {

// regular function features: exactly one of scalar_function and table_valued_function
} // namespace

[[nodiscard]] static std::optional<bool> table_valued(yugawara::function::function_feature_set const& features) {
    using yugawara::function::function_feature;
    bool scalar = false;
    bool table = false;
    for (auto feature : features) {
        if (feature == function_feature::scalar_function) {
            scalar = true;
        } else if (feature == function_feature::table_valued_function) {
            table = true;
        } else {
            return std::nullopt;
        }
    }
    if (scalar == table) {
        return std::nullopt;
    }
    return table;
}

namespace {

/**
 * @brief validates function declarations of one kind in a schema.
 * @details Overloads are allowed; the same name with the same parameter types is ambiguous and rejected.
 */
} // namespace

template <class Function, class ReturnCheck>
[[nodiscard]] static std::string validate_functions(std::vector<index_type> const& members,
                                                    std::vector<Function> const& functions, ReturnCheck return_check,
                                                    char const* kind) {
    for (std::size_t i = 0; i < members.size(); ++i) {
        auto const& function = functions[members[i]];
        if (!valid_name(function.name)) {
            return std::string{"invalid "} + kind + " name";
        }
        if (auto error = validate_parameter_types(function.parameter_types); !error.empty()) {
            return error + " of " + kind + ": " + function.name;
        }
        if (auto error = return_check(function); !error.empty()) {
            return error + " of " + kind + ": " + function.name;
        }
        for (std::size_t j = 0; j < i; ++j) {
            auto const& other = functions[members[j]];
            if (other.name == function.name && same_types(other.parameter_types, function.parameter_types)) {
                return std::string{"duplicate "} + kind + " signature: " + function.name;
            }
        }
    }
    return {};
}

namespace {

/**
 * @brief assigns the single owner of each element referenced from owner lists.
 * @return empty on success, or the violation
 */
} // namespace

template <class Owner, class Members>
[[nodiscard]] static std::string
assign_owners(std::vector<Owner> const& owners, Members members, std::size_t element_count,
              std::vector<std::optional<std::size_t>>& result, char const* element_kind) {
    result.assign(element_count, std::nullopt);
    for (std::size_t owner = 0; owner < owners.size(); ++owner) {
        for (auto element : members(owners[owner])) {
            if (element >= element_count) {
                return std::string{element_kind} + " index out of range";
            }
            if (result[element]) {
                return std::string{element_kind} + (*result[element] == owner ? " referenced twice by one owner"
                                                                              : " referenced by multiple owners");
            }
            result[element] = owner;
        }
    }
    for (auto const& owner : result) {
        if (!owner) {
            return std::string{"orphan "} + element_kind;
        }
    }
    return {};
}

namespace {

using owners_type = std::vector<std::optional<std::size_t>>;

} // namespace

[[nodiscard]] static std::string validate_table(remote_catalog_information const& info, index_type t) {
    auto const& table = info.tables[t];
    if (!valid_name(table.name)) {
        return "invalid table name";
    }
    if (table.columns.empty()) {
        return "table has no columns: " + table.name;
    }
    std::unordered_set<std::string> column_names{};
    for (auto c : table.columns) {
        auto const& column = info.columns[c];
        if (!valid_name(column.name)) {
            return "invalid column name in table " + table.name;
        }
        if (!column_names.emplace(column.name).second) {
            return "duplicate column name: " + column.name;
        }
        if (!column.type) {
            return "missing column type: " + column.name;
        }
        if (!supported_column_type(*column.type)) {
            return "unsupported column type: " + column.name;
        }
    }
    return {};
}

[[nodiscard]] static std::string validate_index(remote_catalog_information const& info, index_type i,
                                                owners_type const& table_owner, owners_type const& index_owner,
                                                owners_type const& column_owner) {
    auto const& index = info.indices[i];
    if (!valid_name(index.name)) {
        return "invalid index name";
    }
    if (!index.table || *index.table >= info.tables.size()) {
        return "index table out of range: " + index.name;
    }
    if (table_owner[*index.table] != index_owner[i]) {
        return "index table is in another schema: " + index.name;
    }
    if (index.keys.empty()) {
        return "index has no keys: " + index.name;
    }
    std::unordered_set<index_type> used{};
    auto check = [&](std::optional<index_type> column) -> std::string {
        if (!column || *column >= info.columns.size()) {
            return "index column out of range: " + index.name;
        }
        if (column_owner[*column] != *index.table) {
            return "index column is not in its table: " + index.name;
        }
        if (!used.emplace(*column).second) {
            return "duplicate index column: " + index.name;
        }
        return {};
    };
    for (auto const& key : index.keys) {
        if (auto error = check(key.column); !error.empty()) {
            return error;
        }
    }
    for (auto value : index.values) {
        if (auto error = check(value); !error.empty()) {
            return error;
        }
    }
    return {};
}

[[nodiscard]] static std::string validate_schema(remote_catalog_information const& info,
                                                 remote_catalog_information::schema const& schema,
                                                 owners_type const& table_owner, owners_type const& index_owner,
                                                 owners_type const& column_owner) {
    std::unordered_set<std::string> table_names{};
    for (auto t : schema.tables) {
        if (!table_names.emplace(info.tables[t].name).second) {
            return "duplicate table name: " + info.tables[t].name;
        }
        if (auto error = validate_table(info, t); !error.empty()) {
            return error;
        }
    }
    std::unordered_set<std::string> index_names{};
    for (auto i : schema.indices) {
        if (!index_names.emplace(info.indices[i].name).second) {
            return "duplicate index name: " + info.indices[i].name;
        }
        if (auto error = validate_index(info, i, table_owner, index_owner, column_owner); !error.empty()) {
            return error;
        }
    }
    if (auto error = validate_functions(
            schema.functions, info.functions,
            [](remote_catalog_information::function const& f) -> std::string {
                if (f.remote_definition_id == yugawara::function::declaration::unresolved_definition_id) {
                    return "unresolved remote definition ID";
                }
                auto tvf = table_valued(f.features);
                if (!tvf) {
                    return "invalid function features";
                }
                return validate_return_type(f.return_type, *tvf);
            },
            "function");
        !error.empty()) {
        return error;
    }
    if (auto error = validate_functions(
            schema.aggregate_functions, info.aggregate_functions,
            [](remote_catalog_information::aggregate_function const& f) -> std::string {
                if (f.remote_definition_id == yugawara::aggregate::declaration::unresolved_definition_id) {
                    return "unresolved remote definition ID";
                }
                return validate_return_type(f.return_type, false);
            },
            "aggregate function");
        !error.empty()) {
        return error;
    }
    return {};
}

[[nodiscard]] static std::string validate(remote_catalog_information const& info) {
    std::vector<std::optional<std::size_t>> table_owner{};
    std::vector<std::optional<std::size_t>> index_owner{};
    std::vector<std::optional<std::size_t>> column_owner{};
    if (auto error = assign_owners(
            info.schemas, [](auto const& s) -> auto const& { return s.tables; }, info.tables.size(), table_owner,
            "table");
        !error.empty()) {
        return error;
    }
    if (auto error = assign_owners(
            info.schemas, [](auto const& s) -> auto const& { return s.indices; }, info.indices.size(), index_owner,
            "index");
        !error.empty()) {
        return error;
    }
    if (auto error = assign_owners(
            info.tables, [](auto const& t) -> auto const& { return t.columns; }, info.columns.size(), column_owner,
            "column");
        !error.empty()) {
        return error;
    }
    std::vector<std::optional<std::size_t>> function_owner{};
    std::vector<std::optional<std::size_t>> aggregate_owner{};
    if (auto error = assign_owners(
            info.schemas, [](auto const& s) -> auto const& { return s.functions; }, info.functions.size(),
            function_owner, "function");
        !error.empty()) {
        return error;
    }
    if (auto error = assign_owners(
            info.schemas, [](auto const& s) -> auto const& { return s.aggregate_functions; },
            info.aggregate_functions.size(), aggregate_owner, "aggregate function");
        !error.empty()) {
        return error;
    }

    std::unordered_set<std::string> schema_names{};
    for (auto const& schema : info.schemas) {
        if (!valid_name(schema.name)) {
            return "invalid schema name";
        }
        if (!schema_names.emplace(schema.name).second) {
            return "duplicate schema name: " + schema.name;
        }
        if (auto error = validate_schema(info, schema, table_owner, index_owner, column_owner); !error.empty()) {
            return error;
        }
    }
    return {};
}

namespace {

/**
 * @brief resolve and apply: builds unpublished Yugawara objects and their mappings.
 * @param lease coordinator-side definition IDs of info.functions and info.aggregate_functions, in the same order.
 * Every restored function declaration shares it, so the IDs are released only after the last declaration is gone.
 * Function declarations get no remote executable location.
 * @pre validate(info) succeeded
 */
} // namespace

[[nodiscard]] static std::shared_ptr<remote_catalog_snapshot const>
build(remote_catalog_reference const& reference, remote_catalog_information const& info,
      std::shared_ptr<remote_function_id_lease const> const& lease) {
    // resolved symbol tables: transfer index -> built object, used only during this call
    std::vector<std::shared_ptr<yugawara::storage::table>> tables(info.tables.size());
    std::vector<std::size_t> column_positions(info.columns.size());
    for (std::size_t t = 0; t < info.tables.size(); ++t) {
        auto const& source = info.tables[t];
        auto table = std::make_shared<yugawara::storage::table>(
            source.name, takatori::util::reference_vector<yugawara::storage::column>{});
        for (auto c : source.columns) {
            auto const& column = info.columns[c];
            column_positions[c] = table->columns().size();
            table->columns().emplace_back(column.name, column.type,
                                          yugawara::variable::criteria{yugawara::variable::nullity{column.nullable}},
                                          yugawara::storage::column_value{}, yugawara::storage::column_feature_set{});
        }
        tables[t] = std::move(table);
    }
    remote_catalog_snapshot::locations_type locations{};
    remote_catalog_snapshot::definitions_type definitions{};
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    for (auto const& schema : info.schemas) {
        auto storage = std::make_shared<yugawara::storage::configurable_provider>();
        for (auto t : schema.tables) {
            storage->add_table(tables[t]);
            locations.emplace(tables[t].get(), reference.remote_server_name);
            definitions.emplace(tables[t].get(), remote_declaration_identity{reference.remote_server_name,
                                                                             remote_declaration_kind::table,
                                                                             info.tables[t].remote_definition_id});
        }
        for (auto i : schema.indices) {
            auto const& source = info.indices[i];
            auto const& table = tables[*source.table];
            std::vector<yugawara::storage::index::key> keys{};
            keys.reserve(source.keys.size());
            for (auto const& key : source.keys) {
                keys.emplace_back(table->columns()[column_positions[*key.column]], key.direction);
            }
            std::vector<yugawara::storage::index::column_ref> values{};
            values.reserve(source.values.size());
            for (auto value : source.values) {
                values.emplace_back(table->columns()[column_positions[value]]);
            }
            storage->add_index(
                yugawara::storage::index{table, source.name, std::move(keys), std::move(values), source.features});
            auto added = storage->find_index(source.name);
            if (!added) {
                throw std::logic_error{"index was not registered: " + source.name};
            }
            definitions.emplace(added.get(), remote_declaration_identity{reference.remote_server_name,
                                                                         remote_declaration_kind::index,
                                                                         source.remote_definition_id});
        }
        auto functions = std::make_shared<yugawara::function::configurable_provider>();
        for (auto f : schema.functions) {
            auto const& source = info.functions[f];
            auto owner = std::make_shared<
                std::pair<std::shared_ptr<remote_function_id_lease const>, yugawara::function::declaration>>(
                lease, yugawara::function::declaration{
                           static_cast<yugawara::function::declaration::definition_id_type>(lease->functions()[f]),
                           source.name, source.return_type, source.parameter_types, source.features});
            // Aliasing ownership retains the lease until the last declaration reference is released.
            auto declaration = std::shared_ptr<yugawara::function::declaration>{owner, &owner->second};
            functions->add(declaration);
            definitions.emplace(declaration.get(), remote_declaration_identity{reference.remote_server_name,
                                                                               remote_declaration_kind::function,
                                                                               source.remote_definition_id});
        }
        auto aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
        for (auto f : schema.aggregate_functions) {
            auto const& source = info.aggregate_functions[f];
            auto owner = std::make_shared<
                std::pair<std::shared_ptr<remote_function_id_lease const>, yugawara::aggregate::declaration>>(
                lease,
                yugawara::aggregate::declaration{
                    static_cast<yugawara::aggregate::declaration::definition_id_type>(lease->aggregate_functions()[f]),
                    source.name, source.return_type, source.parameter_types, source.incremental});
            auto declaration = std::shared_ptr<yugawara::aggregate::declaration>{owner, &owner->second};
            aggregates->add(declaration);
            definitions.emplace(declaration.get(),
                                remote_declaration_identity{reference.remote_server_name,
                                                            remote_declaration_kind::aggregate_function,
                                                            source.remote_definition_id});
        }
        schemas->add(yugawara::schema::declaration{std::nullopt, schema.name, storage, {}, functions, aggregates});
    }
    auto catalog = std::make_shared<yugawara::schema::catalog>(reference.catalog_name, std::nullopt, schemas);
    return std::make_shared<remote_catalog_snapshot>(remote_catalog_snapshot::catalogs_type{std::move(catalog)},
                                                     std::move(locations), std::move(definitions));
}

bool valid_remote_catalog_identifier(std::string_view name) noexcept {
    return valid_name(name) && name.substr(0, 2) != "__";
}

remote_catalog_manager::remote_catalog_manager(std::shared_ptr<remote_catalog_fetcher> fetcher,
                                               std::chrono::milliseconds fetch_timeout,
                                               std::shared_ptr<remote_function_id_registry> const& function_ids)
    : fetcher_(std::move(fetcher)), fetch_timeout_(fetch_timeout), function_ids_(function_ids) {}

remote_catalog_manager::published
remote_catalog_manager::make_published(std::vector<remote_catalog_entry> entries,
                                       std::vector<std::shared_ptr<remote_catalog_snapshot const>> parts) {
    remote_catalog_snapshot::catalogs_type catalogs{};
    remote_catalog_snapshot::locations_type locations{};
    remote_catalog_snapshot::definitions_type definitions{};
    for (auto const& part : parts) {
        catalogs.insert(catalogs.end(), part->catalogs().begin(), part->catalogs().end());
        locations.insert(part->locations().begin(), part->locations().end());
        definitions.insert(part->definitions().begin(), part->definitions().end());
    }
    published result{};
    result.entries = std::make_shared<entry_list_type const>(std::move(entries));
    result.parts = std::move(parts);
    result.snapshot = std::make_shared<remote_catalog_snapshot const>(std::move(catalogs), std::move(locations),
                                                                      std::move(definitions));
    return result;
}

status remote_catalog_manager::start() {
    std::scoped_lock lock{mutex_};
    if (state_ != state::uninitialized) {
        return status::err_invalid_state;
    }
    state_ = state::initializing;
    try {
        published_ = make_published({}, {});
    } catch (std::exception const& e) {
        LOG_LP(ERROR) << "remote catalog manager failed to start: " << e.what();
        state_ = state::uninitialized;
        return status::err_unknown;
    }
    state_ = state::available;
    return status::ok;
}

status remote_catalog_manager::create(remote_catalog_reference const& reference,
                                      remote_server_channel_manager const& channel_manager) {
    std::string diagnostic{};
    return create(reference, channel_manager, diagnostic);
}

status remote_catalog_manager::create(remote_catalog_reference const& reference,
                                      remote_server_channel_manager const& channel_manager, std::string& diagnostic) {
    diagnostic.clear();
    bool name_reserved = false;
    // Construct the guard before reserving: even formatting a failure diagnostic may throw.
    utils::finally release_name{[this, &reference, &name_reserved] {
        if (name_reserved) {
            std::scoped_lock lock{mutex_};
            creating_.erase(reference.catalog_name);
        }
    }};
    {
        std::scoped_lock lock{mutex_};
        if (state_ != state::available) {
            diagnostic = "remote catalog manager is not available";
            return status::err_invalid_state;
        }
        if (reference.remote_server_name == "local") {
            diagnostic = "remote server name 'local' is reserved";
            return status::err_invalid_argument;
        }
        if (!valid_remote_catalog_identifier(reference.catalog_name) ||
            !valid_remote_catalog_identifier(reference.remote_server_name)) {
            diagnostic = "invalid remote catalog or server name";
            return status::err_invalid_argument;
        }
        bool registered = creating_.count(reference.catalog_name) != 0;
        for (auto const& entry : *published_.entries) {
            registered = registered || entry.reference.catalog_name == reference.catalog_name;
        }
        if (registered) {
            diagnostic = "remote catalog already exists: " + reference.catalog_name;
            return status::err_invalid_argument;
        }
        // reserves the name so that concurrent create() of the same name fails without RPC
        creating_.emplace(reference.catalog_name);
        name_reserved = true;
    }
    auto started = std::chrono::steady_clock::now();
    status st = status::ok;
    std::shared_ptr<remote_catalog_snapshot const> part{};
    remote_catalog_information info{};
    // released on any failure below, together with the unpublished declarations sharing it
    std::shared_ptr<remote_function_id_lease> lease{};
    try {
        // fetch
        if (auto channel = channel_manager.find_channel(reference.remote_server_name); !channel) {
            diagnostic = "remote server is not configured: " + reference.remote_server_name;
            st = status::err_unknown;
        } else if (!fetcher_) {
            diagnostic = "GetRemoteCatalog client is not implemented";
            st = status::err_not_implemented;
        } else if (auto rc = fetcher_->fetch(reference, channel, std::chrono::system_clock::now() + fetch_timeout_,
                                             info, diagnostic);
                   rc != status::ok) {
            diagnostic = "remote catalog fetch failed (" + std::string{to_string_view(rc)} + "): " + diagnostic;
            st = status::err_unknown;
        } else if (auto error = validate(info); !error.empty()) {
            // resolve: semantic validation
            diagnostic = "invalid remote catalog: " + error;
            st = status::err_unknown;
        } else if (auto reserved = reserve_function_ids(info, lease, diagnostic); reserved != status::ok) {
            // resolve: coordinator-side definition IDs
            st = reserved;
        } else {
            // resolve and apply: unpublished objects and mappings
            part = build(reference, info, lease);
        }
    } catch (std::exception const& e) {
        diagnostic = std::string{"remote catalog construction failed: "} + e.what();
        st = status::err_unknown;
        part.reset();
    } catch (...) {
        diagnostic = "remote catalog construction threw an unknown exception";
        st = status::err_unknown;
        part.reset();
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    std::scoped_lock lock{mutex_};
    creating_.erase(reference.catalog_name);
    name_reserved = false;
    if (st == status::ok && state_ != state::available) {
        diagnostic = "remote catalog manager stopped during create";
        st = status::err_invalid_state;
    }
    if (st == status::ok) {
        try {
            auto entries = *published_.entries;
            auto parts = published_.parts;
            entries.push_back({reference, part->catalogs().front()});
            parts.push_back(part);
            published_ = make_published(std::move(entries), std::move(parts));
        } catch (std::exception const& e) {
            diagnostic = std::string{"remote catalog publication failed: "} + e.what();
            st = status::err_unknown;
        }
    }
    if (st != status::ok) {
        LOG_LP(WARNING) << "create remote catalog failed catalog:" << reference.catalog_name
                        << " server:" << reference.remote_server_name << " status:" << st
                        << " elapsed_ms:" << elapsed.count() << " reason:" << diagnostic;
        return st;
    }
    LOG_LP(INFO) << "created remote catalog catalog:" << reference.catalog_name
                 << " server:" << reference.remote_server_name << " schemas:" << info.schemas.size()
                 << " tables:" << info.tables.size() << " columns:" << info.columns.size()
                 << " indices:" << info.indices.size() << " functions:" << info.functions.size()
                 << " aggregate_functions:" << info.aggregate_functions.size() << " elapsed_ms:" << elapsed.count();
    return status::ok;
}

status remote_catalog_manager::reserve_function_ids(remote_catalog_information const& info,
                                                    std::shared_ptr<remote_function_id_lease>& lease,
                                                    std::string& diagnostic) const {
    if (info.functions.empty() && info.aggregate_functions.empty()) {
        return status::ok;
    }
    auto registry = function_ids_.lock();
    if (!registry) {
        diagnostic = "remote function definition ID registry is not available";
        return status::err_invalid_state;
    }
    if (auto rc = registry->reserve(info.functions.size(), info.aggregate_functions.size(), lease); rc != status::ok) {
        diagnostic = "cannot reserve remote function definition IDs (" + std::string{to_string_view(rc)} + ")";
        return status::err_unknown;
    }
    return status::ok;
}

status remote_catalog_manager::drop(std::string_view catalog_name) {
    std::scoped_lock lock{mutex_};
    if (state_ != state::available) {
        return status::err_invalid_state;
    }
    auto const& current = *published_.entries;
    for (std::size_t i = 0; i < current.size(); ++i) {
        if (current[i].reference.catalog_name != catalog_name) {
            continue;
        }
        auto entries = current;
        auto parts = published_.parts;
        entries.erase(entries.begin() + static_cast<std::ptrdiff_t>(i));
        parts.erase(parts.begin() + static_cast<std::ptrdiff_t>(i));
        // snapshots obtained earlier keep the dropped catalog alive until released
        published_ = make_published(std::move(entries), std::move(parts));
        LOG_LP(INFO) << "dropped remote catalog catalog:" << catalog_name;
        return status::ok;
    }
    return status::err_invalid_argument;
}

std::shared_ptr<remote_catalog_manager::entry_list_type const> remote_catalog_manager::entries() const noexcept {
    std::scoped_lock lock{mutex_};
    if (state_ != state::available) {
        return {};
    }
    return published_.entries;
}

std::shared_ptr<::yugawara::schema::catalog const>
remote_catalog_manager::find_catalog(std::string_view catalog_name) const noexcept {
    std::scoped_lock lock{mutex_};
    if (state_ != state::available) {
        return {};
    }
    for (auto const& entry : *published_.entries) {
        if (entry.reference.catalog_name == catalog_name) {
            return entry.catalog;
        }
    }
    return {};
}

std::shared_ptr<remote_catalog_snapshot const> remote_catalog_manager::snapshot() const noexcept {
    std::scoped_lock lock{mutex_};
    if (state_ != state::available) {
        return {};
    }
    return published_.snapshot;
}

status remote_catalog_manager::stop() noexcept {
    std::scoped_lock lock{mutex_};
    if (state_ != state::uninitialized && state_ != state::available) {
        return status::err_invalid_state;
    }
    state_ = state::stopped;
    published_ = {};
    return status::ok;
}

} // namespace jogasaki::dist
