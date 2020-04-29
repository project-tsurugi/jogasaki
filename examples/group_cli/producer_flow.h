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
#include "producer_task.h"

namespace jogasaki::executor {

class producer_flow : public common::flow {
public:
    producer_flow() = default;
    producer_flow(exchange::step* downstream, model::step* step, channel* ch, std::shared_ptr<meta::record_meta> meta) : downstream_(downstream), step_(step), channel_(ch), meta_(std::move(meta)){}

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        // process with scan creates only one task
        auto [sinks, srcs] = dynamic_cast<exchange::flow&>(downstream_->data_flow_object()).setup_partitions(1);
        (void)srcs;
        tasks_.emplace_back(std::make_unique<producer_task>(channel_, step_, &sinks[0], meta_));
        return takatori::util::sequence_view{&*(tasks_.begin()), &*(tasks_.end())};
    }

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }

private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    exchange::step* downstream_{};
    model::step* step_{};
    channel* channel_{};
    std::shared_ptr<meta::record_meta> meta_{};
};

}
