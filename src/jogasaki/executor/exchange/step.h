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

#include <jogasaki/model/step.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::exchange {

class step : public common::step {
public:
    step() : common::step(0, 0, 0) {}
    step(number_of_ports inputs, number_of_ports outputs) : common::step(inputs, outputs, 0) {}
    void notify_prepared() override {
        // no-op for exchange
    }
    void notify_completed() override {
        // no-op for exchange
    }
};

}
