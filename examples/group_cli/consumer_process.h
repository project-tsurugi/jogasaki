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
#include "consumer_flow.h"

namespace jogasaki::executor {

class consumer_process : public process::step {
public:
    consumer_process() : step(1, 1) {};
    consumer_process(model::graph* owner) {
        graph_ = owner;
    }

    std::size_t max_partitions() const override {
        return step::max_partitions();
    }
    void activate() override {
        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        auto ch = graph_ ? &graph_->get_channel() : nullptr;
        data_flow_object_ = std::make_unique<consumer_flow>(p, this, ch);
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

}
