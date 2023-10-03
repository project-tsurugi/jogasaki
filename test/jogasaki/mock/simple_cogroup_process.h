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
#include "simple_cogroup_process_flow.h"

namespace jogasaki::executor {

class simple_cogroup_process : public process::step {
public:
    simple_cogroup_process() = default;
    ~simple_cogroup_process() override = default;
    simple_cogroup_process(simple_cogroup_process&& other) noexcept = default;
    simple_cogroup_process& operator=(simple_cogroup_process&& other) noexcept = default;

    void activate(request_context& rctx) override {
        data_flow_object(
            rctx,
            std::make_unique<simple_cogroup_process_flow>(nullptr, this, std::addressof(rctx))
        );
    }
    std::size_t partitions() const noexcept override {
        return default_partitions;
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

}
