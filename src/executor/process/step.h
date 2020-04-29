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

#include <model/step.h>
#include <model/graph.h>
#include <model/task.h>
#include "executor/common/step.h"
//#include "flow.h"

namespace jogasaki::executor::process {

class step : public common::step {
public:
    explicit step(size_type inputs = 1, size_type outputs = 1, size_type subinputs = 0) : common::step(inputs, outputs, subinputs) {}

    void notify_prepared() override {
        // check if main inputs are already available
        // raise upstream_providing to start main tasks running soon
    }
    void notify_completed() override {
        // destroy process buffer
    }
    /*
     * @brief declare max number of partitons
     */
    [[nodiscard]] virtual std::size_t max_partitions() const = 0;

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }
};

}
