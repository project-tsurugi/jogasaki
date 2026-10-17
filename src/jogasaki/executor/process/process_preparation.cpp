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
#include "process_preparation.h"

#include <utility>

#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::process {
std::shared_ptr<processor_info>
prepare_processor(takatori::relation::graph_type const& graph, yugawara::compiled_info const& info,
                  variable_table const* host_variables,
                  takatori::util::maybe_shared_ptr<impl::variables_info_list const> variables,
                  takatori::util::maybe_shared_ptr<impl::block_indices const> blocks) {
    if (static_cast<bool>(variables) != static_cast<bool>(blocks)) {
        fail_with_exception_msg("process variables and blocks must be supplied together");
    }
    if (variables) {
        return std::make_shared<processor_info>(graph, info, std::move(variables), std::move(blocks), host_variables);
    }
    return std::make_shared<processor_info>(graph, info, host_variables);
}
step prepare_process(std::shared_ptr<processor_info> processor, std::shared_ptr<relation_io_map> relations) {
    if (!processor || !relations) {
        fail_with_exception_msg("missing process preparation input");
    }
    return {std::move(processor), std::move(relations)};
}
void bind_process_io(step& process, std::shared_ptr<io_exchange_map> exchanges) {
    if (!process.relation_io_map() || !exchanges ||
        exchanges->input_count() != process.relation_io_map()->input_count() ||
        exchanges->output_count() != process.relation_io_map()->output_count()) {
        fail_with_exception_msg("process relation and runtime I/O bindings disagree");
    }
    for (std::size_t i = 0; i < exchanges->input_count(); ++i) {
        if (!exchanges->input_at(i)) {
            fail_with_exception_msg("unbound process input slot");
        }
    }
    for (std::size_t i = 0; i < exchanges->output_count(); ++i) {
        if (!exchanges->output_at(i)) {
            fail_with_exception_msg("unbound process output slot");
        }
    }
    process.io_exchange_map(std::move(exchanges));
    process.prepare_io_info();
}
} // namespace jogasaki::executor::process
