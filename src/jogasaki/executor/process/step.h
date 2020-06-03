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

#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/task.h>
#include <jogasaki/executor/common/step.h>
#include "flow.h"

namespace jogasaki::executor::process {

class step : public common::step {
public:
    explicit step(number_of_ports inputs = 0, number_of_ports outputs = 0, number_of_ports subinputs = 0) : common::step(inputs, outputs, subinputs) {}

    void notify_prepared() override {
        // check if main inputs are already available
        // raise providing to start main tasks running soon
    }
    void notify_completed() override {
        // destroy process buffer
    }

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }

    /**
     * @brief declare the number of partitions
     * @details process step has the model level knowledge about the number of partitions this process can run.
     * E.g. the process contains some logic that forces to run it only on single partition.
     * This method is to calculate the information based on the graph information and to externalize the knowledge.
     * Subclass should override the default implementation to handle specific cases limiting the number of partitions.
     * @return the number of partitions
     */
    [[nodiscard]] virtual std::size_t partitions() const noexcept {
        return default_partitions;
    }

    void activate() override {
        data_flow_object(std::make_unique<flow>());
    }
};

}
