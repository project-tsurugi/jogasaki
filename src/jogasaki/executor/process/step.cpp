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
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include "flow.h"

namespace jogasaki::executor::process {

using jogasaki::executor::process::impl::ops::process_io;

std::shared_ptr<process_io> step::create_process_io() {
    auto io = std::make_shared<class process_io>();

    std::vector<impl::ops::process_input> inputs{};
    for(auto& in : input_ports()) {
        auto& xchg = *static_cast<exchange::step*>(in->opposites()[0]->owner());
        switch(xchg.kind()) {
            case common::step_kind::forward: {
                auto& fwd = static_cast<exchange::forward::step&>(xchg);
                inputs.emplace_back(
                    fwd.output_meta(),
                    fwd.output_order()
                );
                break;
            }
            case common::step_kind::group:
            case common::step_kind::aggregate:
            {
                auto& grp = static_cast<exchange::shuffle::step&>(xchg);
                inputs.emplace_back(
                    grp.output_meta(),
                    grp.output_order()
                );
                break;
            }
            default:
                fail();
        }
    }
    std::vector<impl::ops::process_output> outputs{};
    for(auto& out : output_ports()) {
        auto& xchg = *static_cast<exchange::step*>(out->opposites()[0]->owner());
        switch(xchg.kind()) {
            case common::step_kind::forward:
            case common::step_kind::group:
            case common::step_kind::aggregate: {
                auto& x = static_cast<exchange::step&>(xchg);
                outputs.emplace_back(
                    x.input_meta(),
                    x.input_order()
                );
                break;
            }
            default:
                fail();
        }
    }
    return std::make_shared<class process_io>(std::move(inputs), std::move(outputs), process_io::external_output_entity_type{});
}

step::step(
    std::shared_ptr<processor_info> info,
    step::number_of_ports inputs,
    step::number_of_ports outputs,
    step::number_of_ports subinputs
) : common::step(inputs, outputs, subinputs),
    info_(std::move(info)),
    process_io_(create_process_io())
{}

common::step_kind step::kind() const noexcept {
    return common::step_kind::process;
}

std::size_t step::partitions() const noexcept {
    return partitions_;
}

void step::activate() {
    data_flow_object(std::make_unique<flow>(
        context(),
        this,
        info_
    ));
}
}
