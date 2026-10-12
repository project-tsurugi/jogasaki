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
#include <optional>
#include <string>
#include <string_view>

#include <takatori/plan/graph.h>
#include <takatori/relation/graph.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/dist/remote_catalog_provider.h>

namespace jogasaki::dist {
/**
 * @brief select a remote server from bound table identities in an existing catalog snapshot.
 * @note This adapter is used only when compiler operation locations are unavailable.
 * @todo Route production plans using captured compiler locations rather than inferring placement here.
 */
[[nodiscard]] std::optional<std::string> remote_server(
    takatori::relation::graph_type const& graph, remote_catalog_snapshot const* catalogs,
    std::string_view fallback = {});
/**
 * @brief adapt remote output boundaries for compilers that inline emit into a process.
 * @details Mutates graph in place and returns replacement expression/variable type mappings.
 * @todo Remove this development adapter after the compiler supplies separated steps with operation locations.
 */
[[nodiscard]] yugawara::compiled_info adapt_remote_outputs(takatori::plan::graph_type& graph,
    yugawara::compiled_info const& info, remote_catalog_snapshot const* catalogs,
    std::string_view fallback = {});
/** @brief validate supported remote plans before RPC dispatch; an empty diagnostic means valid. */
[[nodiscard]] std::string validate_remote_plan(takatori::plan::graph_type const& graph,
    remote_catalog_snapshot const* catalogs, std::string_view fallback = {});
} // namespace jogasaki::dist
