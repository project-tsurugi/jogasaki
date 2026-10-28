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
#include <string_view>

#include <takatori/descriptor/relation.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/graph.h>
#include <takatori/scalar/expression.h>
#include <takatori/type/data.h>

#include <jogasaki/dist/remote_execution_block.h>

namespace jogasaki::dist {

/**
 * @brief resolve portable storage and external-output identities while restoring an execution graph.
 * @note External outputs use native external_relation_info descriptors. Their exchange
 * columns must retain their identities when connected to the runtime output sink.
 */
class remote_execution_graph_resolver {
public:
    remote_execution_graph_resolver() = default;
    remote_execution_graph_resolver(remote_execution_graph_resolver const&) = default;
    remote_execution_graph_resolver& operator=(remote_execution_graph_resolver const&) = default;
    remote_execution_graph_resolver(remote_execution_graph_resolver&&) = default;
    remote_execution_graph_resolver& operator=(remote_execution_graph_resolver&&) = default;
    virtual ~remote_execution_graph_resolver() = default;

    [[nodiscard]] virtual std::optional<takatori::descriptor::relation> resolve_storage(
        remote_source const& source) const = 0;

    [[nodiscard]] virtual std::optional<takatori::descriptor::variable> resolve_storage_column(
        remote_source const& source,
        remote_source_column const& column) const = 0;

    [[nodiscard]] virtual std::optional<takatori::descriptor::variable> resolve_storage_key_column(
        remote_source const& source,
        std::size_t position) const = 0;

    [[nodiscard]] virtual std::optional<takatori::descriptor::relation> resolve_external_relation(
        std::uint64_t relation_id) const = 0;

    [[nodiscard]] virtual std::optional<takatori::descriptor::variable> resolve_external_column(
        std::uint64_t relation_id,
        std::uint64_t column_id) const = 0;

    [[nodiscard]] virtual std::unique_ptr<takatori::scalar::expression> resolve_immediate(
        remote_scalar_expression const& expression) const = 0;

    [[nodiscard]] virtual std::shared_ptr<takatori::type::data const> resolve_type(
        std::string_view type) const = 0;
};

struct remote_execution_graph_build_result {
    // Result carrier intentionally exposes its payload and diagnostic.
    // NOLINTBEGIN(misc-non-private-member-variables-in-classes)
    std::unique_ptr<takatori::relation::graph_type> graph{};
    std::string error{};
    bool invalid_request{};
    // NOLINTEND(misc-non-private-member-variables-in-classes)

    [[nodiscard]] explicit operator bool() const noexcept {
        return graph != nullptr;
    }
};

[[nodiscard]] remote_execution_graph_build_result build_remote_execution_graph(
    remote_execution_block const& block,
    remote_execution_graph_resolver const& resolver);

} // namespace jogasaki::dist
