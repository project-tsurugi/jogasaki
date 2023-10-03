/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>

#include "simple_transform_process_task.h"
#include "simple_transform_process_pretask.h"
#include "simple_transform_process_flow.h"

namespace jogasaki::executor {

class simple_transform_process : public process::step {
public:
    simple_transform_process() = default;
    ~simple_transform_process() override = default;
    simple_transform_process(simple_transform_process&& other) noexcept = default;
    simple_transform_process& operator=(simple_transform_process&& other) noexcept = default;

    void activate(request_context& rctx) override {
        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object(
            rctx,
            std::make_unique<simple_transform_process_flow>(p, this, std::addressof(rctx))
        );
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    std::vector<std::unique_ptr<model::task>> pretasks_{};
};

}
