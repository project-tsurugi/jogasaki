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




#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/data.h>
#include <yugawara/function/declaration.h>
#include <yugawara/schema/catalog.h>
#include <yugawara/storage/index.h>

#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief Engine-side form of one complete L3-07 RemoteCatalog.
 * @details Keeps the repeated fields and 0-origin cross references of the wire message, but not its generated
 * protobuf classes, which Tateyama owns. remote_catalog_decoder produces this form from the wire message, and
 * remote_catalog_encoder uses describe_remote_catalog() as its encoding plan before producing the wire message.
 */
struct remote_catalog_information {
    using index_type = std::uint64_t;
    using type_ptr = std::shared_ptr<takatori::type::data const>;

    struct schema {
        std::string name{};
        std::vector<index_type> tables{};
        std::vector<index_type> indices{};
        std::vector<index_type> functions{};
        std::vector<index_type> aggregate_functions{};
    };

    struct column {
        std::string name{};
        std::shared_ptr<takatori::type::data const> type{};
        bool nullable{true};
    };

    struct table {
        std::uint64_t remote_definition_id{};
        std::string name{};
        std::vector<index_type> columns{};
    };

    struct index_key {
        std::optional<index_type> column{};
        takatori::relation::sort_direction direction{takatori::relation::sort_direction::ascendant};
    };

    struct index {
        std::uint64_t remote_definition_id{};
        std::string name{};
        std::optional<index_type> table{};
        yugawara::storage::index_feature_set features{};
        std::vector<index_key> keys{};
        std::vector<index_type> values{};
    };

    /// @brief RemoteFunction: regular (scalar or table-valued) function declaration
    struct function {
        std::uint64_t remote_definition_id{};
        std::string name{};
        std::vector<type_ptr> parameter_types{};
        type_ptr return_type{};
        yugawara::function::function_feature_set features{};
    };

    /// @brief RemoteAggregateFunction: aggregate function declaration
    struct aggregate_function {
        std::uint64_t remote_definition_id{};
        std::string name{};
        std::vector<type_ptr> parameter_types{};
        type_ptr return_type{};
        bool incremental{};
    };

    std::vector<schema> schemas{};
    std::vector<table> tables{};
    std::vector<column> columns{};
    std::vector<index> indices{};
    std::vector<function> functions{};
    std::vector<aggregate_function> aggregate_functions{};
};

/**
 * @brief Lists the tables, indices, and function declarations of a Yugawara catalog in remote_catalog_information form.
 * @details Serves as the encoding plan of remote_catalog_encoder: objects are enumerated once by identity, and
 * ownership, order and 0-origin transfer indices are fixed here. Development fetchers and tests also use it to
 * exercise the same resolve path as a real fetch. A missing source definition_id is transferred as 0.
 * @return status::ok on success, status::err_unsupported for a declaration outside remote execution, or
 * status::err_unknown if enumeration fails; destination is unchanged on failure
 */
[[nodiscard]] status describe_remote_catalog(
    yugawara::schema::catalog const& source,
    remote_catalog_information& destination,
    std::string& diagnostic);

}  // namespace jogasaki::dist
