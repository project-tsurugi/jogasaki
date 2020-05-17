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
#include "simple_transform_process_task.h"
#include "simple_transform_process_pretask.h"
#include "simple_transform_process_flow.h"

namespace jogasaki::executor {

class simple_transform_process : public process::step {
public:
    simple_transform_process() : step(1, 1, 1) {};
    ~simple_transform_process() override = default;
    simple_transform_process(simple_transform_process&& other) noexcept = default;
    simple_transform_process& operator=(simple_transform_process&& other) noexcept = default;

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        std::size_t partitions = default_partitions;
        auto initial_count = tasks_.size();
        if (tasks_.size() < partitions) {
            for(std::size_t i = 0; i < partitions; ++i) {
                tasks_.emplace_back(std::make_unique<simple_transform_process_task>(context(), this));
            }
        }
        return takatori::util::sequence_view{&*(tasks_.begin()+initial_count), &*(tasks_.end())};
    }

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index subinput) override {
        if (subinput+1 > pretasks_.size()) {
            pretasks_.resize(subinput + 1);
        }
        pretasks_[subinput] = std::make_unique<simple_transform_process_pretask>(context(), this);
        return takatori::util::sequence_view{&pretasks_[subinput]};
    }
    void activate() override {
        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object(std::make_unique<simple_transform_process_flow>(p, this, context()));
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    std::vector<std::unique_ptr<model::task>> pretasks_{};
};

}
