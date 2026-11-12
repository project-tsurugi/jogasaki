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
#include "requires_single_partition.h"

#include <takatori/relation/emit.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/util/downcast.h>

#include <jogasaki/executor/exchange/forward/forward_info.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/model/step_kind.h>

namespace jogasaki::executor::process {

using takatori::util::unsafe_downcast;

bool requires_single_partition(processor_info const& info, io_exchange_map const& exchanges) {
    for(std::size_t i = 0, n = exchanges.output_count(); i < n; ++i) {
        auto* output = exchanges.output_at(i);
        if (output != nullptr && output->kind() == model::step_kind::forward) {
            auto& fi = unsafe_downcast<executor::exchange::forward::step>(*output).info();
            // preserve_output is the constraint on the downstream of the forward exchange (it must not rebalance the
            // records), but the downstream process has the same number of partitions as the upstream one. So the
            // constraint is satisfied by processing the upstream process by single partition.
            if (fi.respect_input() || fi.preserve_output()) {
                return true;
            }
        }
    }
    for(std::size_t i = 0, n = exchanges.input_count(); i < n; ++i) {
        auto* input = exchanges.input_at(i);
        // the downstream of the forward exchange with preserve_output receives the ordered relation
        if (input != nullptr && input->kind() == model::step_kind::forward &&
            unsafe_downcast<executor::exchange::forward::step>(*input).info().preserve_output()) {
            return true;
        }
    }
    for(auto&& e : info.relations()) {
        if (e.kind() == takatori::relation::expression_kind::emit &&
            unsafe_downcast<takatori::relation::emit>(e).ordering().contains(
                takatori::relation::emit_ordering::preserve_output)) {
            return true;
        }
    }
    return false;
}

}  // namespace jogasaki::executor::process
