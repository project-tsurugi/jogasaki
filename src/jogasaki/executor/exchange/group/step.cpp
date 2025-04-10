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
#include "step.h"

#include <utility>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/shuffle/step.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

#include "flow.h"
#include "group_info.h"

namespace jogasaki::executor::exchange::group {

step::step() : info_(std::make_shared<group_info>()) {}

step::step(
    std::shared_ptr<group_info> info,
    meta::variable_order input_column_order,
    meta::variable_order output_column_order
) :
    shuffle::step(info->record_meta(), std::move(input_column_order)),
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
        std::make_shared<group_info>(std::move(input_meta), std::move(key_indices)),
        std::move(input_column_order),
        std::move(output_column_order)
    )
{}

void step::activate(request_context& rctx) {
    auto* down = downstream(0);
    auto downstream_partitions =
        down ? down->partitions() : global::config_pool()->default_partitions();
    data_flow_object(
        rctx,
        std::make_unique<group::flow>(info_, std::addressof(rctx), this, downstream_partitions)
    );
}

meta::variable_order const &step::output_order() const noexcept {
    return output_column_order_;
}

const maybe_shared_ptr<meta::group_meta> &step::output_meta() const noexcept {
    return info_->group_meta();
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

}  // namespace jogasaki::executor::exchange::group
