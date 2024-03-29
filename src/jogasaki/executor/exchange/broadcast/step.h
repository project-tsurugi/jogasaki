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
#include <jogasaki/model/port.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>

namespace jogasaki::executor::exchange::broadcast {

class step : public exchange::step {
public:
    step() = default;

    [[nodiscard]] takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks(request_context& rctx) override {
        // exchange task is nop
        tasks_.emplace_back(std::make_shared<exchange::task>(std::addressof(rctx), this));
        return tasks_;
    }

    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::broadcast;
    }
    void activate(request_context&) override {

    }
private:
    std::vector<std::shared_ptr<model::task>> tasks_{};
};

}

