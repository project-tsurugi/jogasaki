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
#include "operation_location.h"

#include <stdexcept>
#include <utility>

#include <takatori/plan/exchange.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/util/downcast.h>
#include <yugawara/binding/extract.h>

#include <jogasaki/dist/remote_execution_block.h>
#include <jogasaki/dist/remote_process_execution_block_shape.h>

namespace jogasaki::dist {
// The remote task writes into exactly one downstream, and the worker rebuilds its output as a
// forward, which is also what the compiler makes of a fence.
[[nodiscard]] static bool has_single_forward_output(takatori::plan::process const& process) {
    auto downstreams = process.downstreams();
    if (downstreams.size() != 1) {
        return false;
    }
    for (auto const& relation : process.operators()) {
        if (relation.kind() != takatori::relation::expression_kind::offer) {
            continue;
        }
        auto const& offer = takatori::util::unsafe_downcast<takatori::relation::step::offer>(relation);
        auto const& exchange = yugawara::binding::extract<takatori::plan::exchange>(offer.destination());
        if (exchange.kind() != takatori::plan::step_kind::forward || &exchange != &downstreams[0]) {
            return false;
        }
    }
    return true;
}
process_locations::process_locations(entity_type entity) noexcept : entity_(std::move(entity)) {}
process_location const* process_locations::find(takatori::plan::process const& process) const noexcept {
    auto it = entity_.find(&process);
    return it == entity_.end() ? nullptr : &it->second;
}
std::size_t process_locations::size() const noexcept { return entity_.size(); }
process_locations_result capture_process_locations(takatori::plan::graph_type const& graph,
                                                   operation_locations const& query, remote_catalog_snapshot const* catalogs) {
    process_locations::entity_type entity{};
    std::size_t remotes{};
    // Current exchanges run on the coordinator, so only process steps are placed.
    for (auto const& step : graph) {
        if (step.kind() != takatori::plan::step_kind::process) {
            continue;
        }
        auto const& process = takatori::util::unsafe_downcast<takatori::plan::process>(step);
        auto ids = query.find(process);
        if (ids.empty()) {
            return {{}, "no executable location for a compiled process"};
        }
        if (ids.size() != 1) {
            return {{}, "remote execution requires exactly one executable location per process"};
        }
        auto id = *ids.begin();
        if (id == operation_locations::local_location_id) {
            entity.emplace(&process, process_location{});
            continue;
        }
        std::string server{};
        try {
            server = query.location_name(id);
        } catch (std::out_of_range const&) {
            return {{}, "unknown location ID " + std::to_string(id)};
        }
        if (server.empty()) {
            return {{}, "remote location ID " + std::to_string(id) + " has no server name"};
        }
        if (++remotes > 1) {
            return {{}, "remote execution permits at most one remote execution block per SQL execution"};
        }
        if (!is_remote_process_execution_block_shape(process.operators())) {
            return {{},
                    "the compiler did not separate the remote process from local operators; "
                    "remote execution does not rewrite a plan with operation locations"};
        }
        if (!has_single_forward_output(process)) {
            return {{}, "remote process output must be a single forward exchange in remote execution"};
        }
        try {
            if (!create_remote_execution_block(process.operators(), nullptr, nullptr, nullptr, catalogs, server)) {
                return {{}, "unsupported remote expression, host variable, or missing storage definition identity in remote execution"};
            }
        } catch (std::invalid_argument const& e) {
            return {{}, "unsupported remote type or literal in remote execution: " + std::string{e.what()}};
        }
        entity.emplace(&process, process_location{std::move(server)});
    }
    return {std::make_shared<process_locations const>(std::move(entity)), {}};
}
} // namespace jogasaki::dist
