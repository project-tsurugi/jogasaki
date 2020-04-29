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

#include <model/step.h>
#include <model/task.h>
#include <constants.h>
#include <executor/process/step.h>
#include "simple_transform_process_task.h"
#include "simple_transform_process_pretask.h"
#include "simple_cogroup_process_flow.h"

namespace jogasaki::executor {

class simple_cogroup_process : public process::step {
public:
    simple_cogroup_process() : step(2, 1, 0) {};
    ~simple_cogroup_process() = default;
    simple_cogroup_process(simple_cogroup_process&& other) noexcept = default;
    simple_cogroup_process& operator=(simple_cogroup_process&& other) noexcept = default;
    simple_cogroup_process(model::graph* owner) {
        graph_ = owner;
    }

    void activate() override {
        auto ch = graph_ ? &graph_->get_channel() : nullptr;
//        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object_ = std::make_unique<simple_cogroup_process_flow>(nullptr, this, ch);
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

}
