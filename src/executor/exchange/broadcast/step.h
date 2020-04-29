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
#include <model/port.h>
#include <executor/exchange/step.h>
#include <executor/exchange/task.h>

namespace jogasaki::executor::exchange::broadcast {

class step : public exchange::step {
public:
    step() : exchange::step(1, 1) {}
    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        // exchange task is nop
        tasks_.emplace_back(std::make_unique<exchange::task>(&graph_->get_channel(), this));
        return tasks_;
    }
    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::broadcast;
    }
    void activate() override {
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

}

