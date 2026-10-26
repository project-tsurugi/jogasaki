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

#include <stdexcept>
#include <vector>

#include <takatori/plan/forward.h>
#include <takatori/plan/process.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/find.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/util/downcast.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/extract.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/index.h>

#include <jogasaki/dist/remote_execution_block.h>
#include <jogasaki/dist/remote_process_execution_block_shape.h>

namespace jogasaki::dist {
std::optional<std::string> remote_server(takatori::relation::graph_type const& graph,
                                         remote_catalog_snapshot const* catalogs, std::string_view fallback) {
    if (catalogs) {
        for (auto const& relation : graph) {
            takatori::descriptor::relation const* source{};
            if (relation.kind() == takatori::relation::expression_kind::scan) {
                source = &takatori::util::unsafe_downcast<takatori::relation::scan>(relation).source();
            }
            if (relation.kind() == takatori::relation::expression_kind::find) {
                source = &takatori::util::unsafe_downcast<takatori::relation::find>(relation).source();
            }
            if (relation.kind() == takatori::relation::expression_kind::join_find) {
                source = &takatori::util::unsafe_downcast<takatori::relation::join_find>(relation).source();
            }
            if (relation.kind() == takatori::relation::expression_kind::join_scan) {
                source = &takatori::util::unsafe_downcast<takatori::relation::join_scan>(relation).source();
            }
            if (!source) {
                continue;
            }
            auto const& index = yugawara::binding::extract<yugawara::storage::index>(*source);
            if (auto* location = catalogs->remote_server(index.table())) {
                return *location;
            }
        }
    }
    if (!fallback.empty() && is_remote_process_execution_block_shape(graph)) {
        return std::string{fallback};
    }
    return {};
}
yugawara::compiled_info adapt_remote_outputs(takatori::plan::graph_type& graph, yugawara::compiled_info const& info,
                                             remote_catalog_snapshot const* catalogs, std::string_view fallback) {
    using kind = takatori::relation::expression_kind;
    std::vector<takatori::plan::process*> processes{};
    for (auto& step : graph) {
        if (step.kind() == takatori::plan::step_kind::process) {
            processes.push_back(&takatori::util::unsafe_downcast<takatori::plan::process>(step));
        }
    }
    auto variables = std::make_shared<yugawara::analyzer::variable_mapping>(info.variables());
    auto expressions = std::make_shared<yugawara::analyzer::expression_mapping>(info.expressions());
    yugawara::binding::factory bindings{};
    for (auto* process : processes) {
        takatori::relation::emit* emit{};
        std::size_t sources{};
        bool simple = true;
        for (auto& relation : process->operators()) {
            switch (relation.kind()) {
                case kind::scan:
                case kind::find:
                    ++sources;
                    break;
                case kind::filter:
                case kind::project:
                    break;
                case kind::emit:
                    if (emit != nullptr) {
                        simple = false;
                    }
                    emit = &takatori::util::unsafe_downcast<takatori::relation::emit>(relation);
                    break;
                default:
                    simple = false;
                    break;
            }
        }
        if (!simple || sources != 1 || (emit == nullptr) || !emit->input().opposite()) {
            continue;
        }
        if (!remote_server(process->operators(), catalogs) && fallback.empty()) {
            continue;
        }
        // Each emitted field gets a distinct exchange column and consumer variable,
        // including SELECT c, c. Neither labels nor column order are changed.
        std::vector<takatori::descriptor::variable> columns{};
        std::vector<takatori::relation::step::offer::column> offered{};
        std::vector<takatori::relation::step::take_flat::column> taken{};
        auto emitted = emit->columns();
        for (auto& column : emitted) {
            auto exchange = bindings.exchange_column();
            auto consumer = bindings.stream_variable();
            auto type = std::shared_ptr<takatori::type::data const>{info.type_of(column.source()).clone()};
            variables->bind(exchange, type);
            variables->bind(consumer, type);
            offered.emplace_back(column.source(), exchange);
            taken.emplace_back(exchange, consumer);
            columns.emplace_back(exchange);
            column.source() = consumer;
        }
        auto& forward = graph.insert(takatori::plan::forward{std::move(columns)});
        auto& consumer = graph.insert(takatori::plan::process{});
        auto& take = consumer.operators().insert(
            takatori::relation::step::take_flat{bindings.exchange(forward), std::move(taken)});
        auto& output = consumer.operators().insert(takatori::relation::emit{std::move(emitted)});
        take.output() >> output.input();
        auto* upstream = emit->input().opposite().get();
        emit->input().disconnect_all();
        process->operators().erase(*emit);
        auto& offer = process->operators().insert(
            takatori::relation::step::offer{bindings.exchange(forward), std::move(offered)});
        *upstream >> offer.input();
        *process >> forward;
        forward >> consumer;
    }
    return {std::move(expressions), std::move(variables)};
}
std::string validate_remote_plan(takatori::plan::graph_type const& graph, remote_catalog_snapshot const* catalogs,
                                 std::string_view fallback) {
    std::size_t count{};
    for (auto const& step : graph) {
        if (step.kind() != takatori::plan::step_kind::process) {
            continue;
        }
        auto const& process = takatori::util::unsafe_downcast<takatori::plan::process>(step);
        auto destination = remote_server(process.operators(), catalogs, fallback);
        if (!destination) {
            continue;
        }
        if (++count > 1) {
            return "remote execution permits at most one remote execution block per SQL execution";
        }
        if (!is_remote_process_execution_block_shape(process.operators())) {
            return "unsupported remote process shape in remote execution";
        }
        try {
            if (!create_remote_execution_block(process.operators(), nullptr, nullptr, nullptr, catalogs, *destination)) {
                return "unsupported remote expression, host variable, or missing storage definition identity in remote execution";
            }
        } catch (std::invalid_argument const& e) {
            // The development codec rejects unsupported types/literals by exception.
            return std::string{"unsupported remote type or literal in remote execution: "} + e.what();
        }
    }
    return {};
}
} // namespace jogasaki::dist
