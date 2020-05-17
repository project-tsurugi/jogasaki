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
#pragma once

#include <memory>

#include <model/step.h>
#include <model/task.h>
#include <constants.h>
#include <executor/process/step.h>
#include "producer_task.h"
#include "producer_flow.h"
#include "params.h"

namespace jogasaki::group_cli {

class producer_process : public executor::process::step {
public:
    producer_process() = default;
    producer_process(model::graph* graph,
            std::shared_ptr<meta::record_meta> meta,
            params& c) : step(0, 1),
    meta_(std::move(meta)), context_(&c) {
        owner(graph);
    }

    void activate() override {
        auto p = dynamic_cast<executor::exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object(std::make_unique<producer_flow>(p, this, channel(), meta_, *context_));
    }

    void deactivate() override {
        meta_.reset();
        executor::process::step::deactivate();
    }
private:
    std::shared_ptr<meta::record_meta> meta_{};
    params* context_{};
};

}
