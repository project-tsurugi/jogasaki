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
#include <jogasaki/model/port.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include "flow.h"

namespace jogasaki::executor::exchange::forward {

class step : public exchange::step {
public:
    step() = default;

    explicit step(
        std::shared_ptr<meta::record_meta> meta
    ) :
        meta_(std::move(meta))
    {}

    step(
        std::shared_ptr<meta::record_meta> meta,
        meta::variable_order column_order
    ) :
        exchange::step(std::move(column_order)),
        meta_(std::move(meta))
    {}

    takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override {
        // exchange task is nop
        tasks_.emplace_back(std::make_shared<exchange::task>(context(), this));
        return tasks_;
    }

    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::forward;
    }

    void activate() override {
        // create data flow object
        data_flow_object(std::make_unique<forward::flow>(meta_, context(), this));
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& input_meta() const noexcept {
        return meta_;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& output_meta() const noexcept {
        return meta_;
    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
    std::shared_ptr<meta::record_meta> meta_{};
};

}

