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
#include <set>
#include <string>
#include <unordered_map>

#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step.h>

namespace jogasaki::dist {
class remote_catalog_snapshot;
/**
 * @brief Engine-side boundary for the compiler's operation location result (L3-03 [4], [5]).
 * @details Yugawara does not provide compiler_result::operation_location_query() yet, so only
 * explicit injections implement this. An adapter for the formal API wraps that query together with
 * the engine's location_info registry. The result is defined only for steps of the graph it was
 * computed for; steps added after compilation must never be passed.
 */
class operation_locations {
public:
    using location_id_type = std::uint64_t;
    using location_id_set_type = std::set<location_id_type>;
    static constexpr location_id_type local_location_id = 0;
    operation_locations() = default;
    operation_locations(operation_locations const&) = default;
    operation_locations& operator=(operation_locations const&) = default;
    operation_locations(operation_locations&&) = default;
    operation_locations& operator=(operation_locations&&) = default;
    virtual ~operation_locations() = default;
    [[nodiscard]] virtual location_id_set_type find(takatori::plan::step const& step) const = 0;
    // Throws std::out_of_range for an unregistered ID, as symbol_location_query::of() does.
    [[nodiscard]] virtual std::string location_name(location_id_type id) const = 0;
};

/** @brief Placement of one compiled process. An empty server means the coordinator. */
struct process_location {
    std::string server{};
};

/**
 * @brief Process placement copied out of an operation location result.
 * @details Keyed by the identity of the compiled process steps. No reference to the query is kept,
 * so the placement stays usable after the compiler result is released.
 */
class process_locations {
public:
    using entity_type = std::unordered_map<takatori::plan::process const*, process_location>;
    process_locations() = default;
    explicit process_locations(entity_type entity) noexcept;
    // nullptr if the process was not queried, i.e. it is foreign or was added after compilation.
    [[nodiscard]] process_location const* find(takatori::plan::process const& process) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept;
private:
    entity_type entity_{};
};

struct process_locations_result {
    std::shared_ptr<process_locations const> locations{};
    // Non-empty means the plan is unsupported in remote execution.
    std::string error{};
};

/**
 * @brief Captures the formal placement of each compiled process without changing the graph.
 * @details Each existing process is queried once. A remote process must already be separated by
 * the compiler (fence converted to forward) and offer only into its single downstream forward; a
 * remote process with inline emit is rejected instead of being rewritten, because new steps have no
 * operation location.
 */
[[nodiscard]] process_locations_result capture_process_locations(
    takatori::plan::graph_type const& graph, operation_locations const& query,
    remote_catalog_snapshot const* catalogs = nullptr);
} // namespace jogasaki::dist
