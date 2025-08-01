/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "writer_count_calculator.h"

#include <glog/logging.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/transaction_context.h>
#include <mizugaki/analyzer/sql_analyzer.h>
#include <takatori/plan/process.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/statement.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>
#include <yugawara/compiler_result.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;
namespace statement = ::takatori::statement;
namespace impl {

const api::impl::executable_statement& get_impl(const api::executable_statement& stmt) {
    return unsafe_downcast<const api::impl::executable_statement>(stmt);
}
inline api::impl::database& get_impl(api::database& db) {
    return unsafe_downcast<api::impl::database>(db);
}

bool has_emit_operator(takatori::plan::step const& s) noexcept {
    bool has_emit = false;
    auto& process = unsafe_downcast<takatori::plan::process const>(s);
    takatori::relation::sort_from_upstream(
        process.operators(), [&has_emit](takatori::relation::expression const& op) {
            if (op.kind() == takatori::relation::expression_kind::emit) { has_emit = true; }
        });
    return has_emit;
}

size_t terminal_calculate_partition(
    takatori::plan::step const& s, const size_t partitions, bool is_rtx) noexcept {
    std::size_t result = global::config_pool()->default_partitions();
    auto& process      = unsafe_downcast<takatori::plan::process const>(s);
    takatori::relation::sort_from_upstream(
        process.operators(), [&](const takatori::relation::expression& op) {
            switch (op.kind()) {
                case takatori::relation::expression_kind::scan:
                    result = (is_rtx && partitions > 0) ? partitions : 1;
                    break;
                case takatori::relation::expression_kind::find: result = 1; break;
                default: break;
            }
        });
    return result;
}

size_t intermediate_calculate_partition(
    takatori::plan::step const& s, std::size_t partitions, bool is_rtx) noexcept {
    size_t sum = 0;
    switch (s.kind()) {
        case takatori::plan::step_kind::process: {
            auto& process         = unsafe_downcast<takatori::plan::process>(s);
            const auto& upstreams = process.upstreams();
            if (upstreams.empty()) { return terminal_calculate_partition(s, partitions, is_rtx); }
            for (auto&& t : upstreams) {
                auto par = intermediate_calculate_partition(t, partitions, is_rtx);
                if (sum != 0 && sum != par) {
                    VLOG_LP(log_error) << "two upstreams have different partitions " << sum << ", "
                                       << par << ", this should not happen normally";
                }
                sum = par;
            }
            break;
        }
        case takatori::plan::step_kind::group:
            return global::config_pool()->default_partitions();
            break;
        case takatori::plan::step_kind::aggregate:
            return global::config_pool()->default_partitions();
            break;
        case takatori::plan::step_kind::forward: {
            for (auto&& t : unsafe_downcast<takatori::plan::exchange>(s).upstreams()) {
                sum += intermediate_calculate_partition(t, partitions, is_rtx);
            }
            break;
        }
        default:
            VLOG_LP(log_error) << "unknown step_kind";
            return global::config_pool()->default_partitions();
            break;
    }
    return sum;
}

size_t calculate_partition(
    takatori::plan::step const& s, const std::size_t partitions, bool is_rtx) noexcept {
    auto& process  = unsafe_downcast<takatori::plan::process>(s);
    auto partition = global::config_pool()->default_partitions();
    if (!process.downstreams().empty()) {
        VLOG_LP(log_error) << "The bottom of graph_type must not have downstreams";
    } else {
        partition = intermediate_calculate_partition(s, partitions, is_rtx);
    }
    return partition;
}

size_t get_partitions(maybe_shared_ptr<statement::statement> const& statement,
    const std::size_t partitions, bool is_rtx) {
    if (!statement || statement->kind() != statement::statement_kind::execute) { return 0; }
    std::size_t result = 0;
    takatori::plan::enumerate_bottom(
        unsafe_downcast<takatori::statement::execute>(*statement).execution_plan(),
        [&](const takatori::plan::step& s) {
            if (s.kind() == takatori::plan::step_kind::process) {
                if (has_emit_operator(s)) {
                    result = calculate_partition(s, partitions, is_rtx);
                }
            } else {
                VLOG_LP(log_error) << "The bottom of graph_type must be process.";
            }
        });
    return result;
}

} // namespace impl

std::optional<std::size_t> calculate_max_writer_count(
    api::executable_statement const& stmt, transaction_context const& tx) {
    auto& s         = unsafe_downcast<api::impl::executable_statement>(stmt).body()->statement();
    auto partitions = global::config_pool()->scan_default_parallel();
    auto& option    = tx.option();
    if (option && option->scan_parallel().has_value()) {
        partitions = option->scan_parallel().value();
    }
    if (s->kind() == takatori::statement::statement_kind::execute) {
        partitions = impl::get_partitions(s, partitions, option->readonly());
        if (VLOG_IS_ON(log_debug)) {
            std::stringstream ss{};
            ss << "write_count:" << partitions << " Use calculate_partition";
            VLOG_LP(log_debug) << ss.str();
        }
        return partitions;
    }
    return std::nullopt;
}

} // namespace jogasaki::executor
