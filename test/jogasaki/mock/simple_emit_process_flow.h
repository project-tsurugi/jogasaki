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

#include <memory>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>

#include "simple_scan_process_task.h"

namespace jogasaki::executor {

class simple_emit_process_flow : public model::flow {
public:
    simple_emit_process_flow() = default;
    ~simple_emit_process_flow() = default;
    simple_emit_process_flow(exchange::step* downstream, model::step* step, request_context* context) : downstream_(downstream), step_(step), context_(context) {}
    takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        auto initial_count = tasks_.size();
        if (tasks_.size() < default_partitions) {
            for(std::size_t i = 0; i < default_partitions; ++i) {
                tasks_.emplace_back(std::make_unique<simple_emit_process_task>(context_, step_));
            }
        }
        return takatori::util::sequence_view{&*(tasks_.begin()+initial_count), &*(tasks_.end())};
    }

    takatori::util::sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }
    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::process;
    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    exchange::step* downstream_{};
    model::step* step_{};
    request_context* context_{};
};

}
