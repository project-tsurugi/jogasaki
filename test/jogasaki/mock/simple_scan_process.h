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

#include "simple_scan_process_task.h"
#include "simple_scan_process_flow.h"

namespace jogasaki::executor {

class simple_scan_process : public process::step {
public:
    simple_scan_process() : step(0, 1) {};
    ~simple_scan_process() override = default;
    simple_scan_process(simple_scan_process&& other) noexcept = default;
    simple_scan_process& operator=(simple_scan_process&& other) noexcept = default;
    void activate() override {
        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object(std::make_unique<simple_scan_process_flow>(p, this, context()));
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

}
