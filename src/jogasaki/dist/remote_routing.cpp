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
#include "remote_routing.h"

#include <takatori/relation/find.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/relation/scan.h>
#include <takatori/util/downcast.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/index.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/request_context.h>
#include <jogasaki/dist/remote_catalog_provider.h>

namespace jogasaki::dist {
namespace {
// Inspection is only a fail-closed guard for internal callers without captured locations.
// Never select a destination from this metadata or run a remote declaration locally.
bool has_remote_source(takatori::relation::graph_type const& graph, remote_catalog_snapshot const* catalogs) {
    if (!catalogs) return false;
    for (auto const& relation : graph) {
        takatori::descriptor::relation const* source{};
        using kind = takatori::relation::expression_kind;
        switch (relation.kind()) {
            case kind::scan: source = &takatori::util::unsafe_downcast<takatori::relation::scan>(relation).source(); break;
            case kind::find: source = &takatori::util::unsafe_downcast<takatori::relation::find>(relation).source(); break;
            case kind::join_find: source = &takatori::util::unsafe_downcast<takatori::relation::join_find>(relation).source(); break;
            case kind::join_scan: source = &takatori::util::unsafe_downcast<takatori::relation::join_scan>(relation).source(); break;
            default: break;
        }
        if (!source) continue;
        auto const& index = yugawara::binding::extract<yugawara::storage::index>(*source);
        if (catalogs->remote_server(index.table()) || catalogs->remote_definition(index.table()) ||
            catalogs->remote_definition(index)) return true;
    }
    return false;
}
} // namespace

remote_execution_server_selection select_remote_execution_server(
    request_context const& context, executor::process::processor_info const& info) {
    auto dispatch_allowed = !context.remote_worker() && context.configuration()->distributed_query_coordinator_enabled();
    if (auto const& captured = info.captured_location()) {
        if (captured->empty()) return {};
        if (!dispatch_allowed) {
            return {{}, "Remote query execution is not available in this execution context."};
        }
        return {*captured, {}};
    }
    if (has_remote_source(info.relations(), context.remote_catalogs().get())) {
        return {{}, dispatch_allowed
            ? "Compiler-provided execution locations are required for a remote process."
            : "Remote query execution is not available in this execution context."};
    }
    return {};
}
} // namespace jogasaki::dist
