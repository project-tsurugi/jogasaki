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

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>

#include "simple_scan_process_task.h"

namespace jogasaki::executor {

class simple_scan_process_flow : public common::flow {
public:
    simple_scan_process_flow() = default;
    ~simple_scan_process_flow() = default;
    simple_scan_process_flow(exchange::step* downstream, model::step* step, request_context* context) :
            downstream_(downstream), step_(step), context_(context) {}
    takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        // process with scan creates only one task
        auto [sinks, srcs] = dynamic_cast<exchange::flow&>(downstream_->data_flow_object()).setup_partitions(1);
        (void)srcs;
        auto& writer = sinks[0].acquire_writer();
        tasks_.emplace_back(std::make_unique<simple_scan_process_task>(context_, step_, &writer));
        return takatori::util::sequence_view{&*(tasks_.begin()), &*(tasks_.end())};
    }
    takatori::util::sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }
    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    exchange::step* downstream_{};
    model::step* step_{};
    request_context* context_{};
};

}
