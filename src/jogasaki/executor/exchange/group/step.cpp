/*
 * Copyright 2018-2020 tsurugi project.
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
#include "step.h"

#include <vector>

#include <jogasaki/model/port.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/shuffle/step.h>
#include <jogasaki/executor/process/step.h>
#include "shuffle_info.h"
#include "flow.h"

namespace jogasaki::executor::exchange::group {

step::step() : info_(std::make_shared<shuffle_info>()) {}

step::step(
    std::shared_ptr<shuffle_info> info,
    meta::variable_order input_column_order,
    meta::variable_order output_column_order
) : shuffle::step(info->record_meta(), std::move(input_column_order)),
    info_(std::move(info)),
    output_column_order_(std::move(output_column_order))
{}

step::step(
    maybe_shared_ptr<meta::record_meta> input_meta,
    std::vector<field_index_type> key_indices,
    meta::variable_order input_column_order,
    meta::variable_order output_column_order
) :
    step(
        std::make_shared<shuffle_info>(std::move(input_meta), std::move(key_indices)),
        std::move(input_column_order),
        std::move(output_column_order)
    )
{}

executor::common::step_kind step::kind() const noexcept {
    return executor::common::step_kind::group;
}

void step::activate() {
    auto* down = downstream(0);
    auto downstream_partitions = down ? down->partitions() : default_partitions;
    data_flow_object(std::make_unique<group::flow>(info_, context(), this, downstream_partitions));
}

process::step *step::downstream(std::size_t index) const noexcept {
    if (output_ports().empty()) return nullptr;
    if (output_ports()[0]->opposites().size() <= index) return nullptr;
    return dynamic_cast<process::step*>(output_ports()[0]->opposites()[index]->owner());
}

process::step *step::upstream(std::size_t index) const noexcept {
    if (input_ports().empty()) return nullptr;
    if (input_ports()[0]->opposites().size() <= index) return nullptr;
    return dynamic_cast<process::step*>(input_ports()[0]->opposites()[index]->owner());
}
}


