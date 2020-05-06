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

#include <vector>

#include <model/port.h>
#include <model/step.h>
#include "meta/record_meta.h"
#include <executor/exchange/step.h>
#include <executor/exchange/task.h>
#include <executor/process/step.h>
#include "shuffle_info.h"
#include "flow.h"

namespace jogasaki::executor::exchange::group {

/**
 * @brief group step
 */
class step : public exchange::step {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    step() : exchange::step(1, 1), info_(std::make_shared<shuffle_info>()) {}

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    explicit step(std::shared_ptr<shuffle_info> info) : exchange::step(1, 1), info_(std::move(info)) {}

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    step(std::shared_ptr<meta::record_meta> input_meta,
            std::vector<field_index_type> key_indices) :
            step(std::make_shared<shuffle_info>(std::move(input_meta), std::move(key_indices))) {}

    [[nodiscard]] executor::common::step_kind kind() const noexcept override {
        return executor::common::step_kind::group;
    }

    void activate() override {
        auto* down = downstream(0);
        auto downstream_partitions = down ? down->partitions() : default_partitions;
        auto ch = graph_ ? &graph_->get_channel() : nullptr;
        data_flow_object_ = std::make_unique<group::flow>(info_, ch, this, downstream_partitions);
    }
protected:
    [[nodiscard]] process::step* downstream(std::size_t index) const noexcept {
        if (output_ports_.empty()) return nullptr;
        if (output_ports_[0]->opposites().size() <= index) return nullptr;
        return dynamic_cast<process::step*>(output_ports_[0]->opposites()[index]->owner());
    }

    [[nodiscard]] process::step* upstream(std::size_t index) const noexcept {
        if (main_input_ports_.empty()) return nullptr;
        if (main_input_ports_[0]->opposites().size() <= index) return nullptr;
        return dynamic_cast<process::step*>(main_input_ports_[0]->opposites()[index]->owner());
    }

private:
    std::shared_ptr<shuffle_info> info_{};
};

}


