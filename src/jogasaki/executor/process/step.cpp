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

#include <memory>
#include "flow.h"

namespace jogasaki::executor::process {

step::step(std::shared_ptr<processor_info> info, step::number_of_ports inputs, step::number_of_ports outputs,
    step::number_of_ports subinputs) : common::step(inputs, outputs, subinputs), info_(std::move(info)) {}

common::step_kind step::kind() const noexcept {
    return common::step_kind::process;
}

std::size_t step::partitions() const noexcept {
    return partitions_;
}

void step::activate() {
    data_flow_object(std::make_unique<flow>(
        flow::record_meta_list {},
        flow::record_meta_list {},
        flow::record_meta_list {},
        context(),
        this,
        info_
    ));
}
}
