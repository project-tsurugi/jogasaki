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
#include "context.h"

namespace jogasaki::group_cli {

class consumer_process : public executor::process::step {
public:
    consumer_process() = default;
    explicit consumer_process(model::graph* graph, std::shared_ptr<meta::group_meta> meta, context& c) :
            step(1, 1), meta_(std::move(meta)), context_(&c) {
        owner(graph);
    }

    void activate() override {
        auto p = dynamic_cast<executor::exchange::step*>(input_ports()[0]->opposites()[0]->owner());
        data_flow_object(std::make_unique<consumer_flow>(p, this, channel(), meta_, *context_));
    }

    [[nodiscard]] std::size_t partitions() const noexcept override {
        return context_->downstream_partitions_;
    }

private:
    std::shared_ptr<meta::group_meta> meta_{};
    context* context_{};
};

}
