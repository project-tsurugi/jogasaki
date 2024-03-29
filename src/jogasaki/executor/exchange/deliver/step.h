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
#include "flow.h"

namespace jogasaki::executor::exchange::deliver {

class step : public exchange::step {
public:
    step() = default;

    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::deliver;
    }

    void activate(request_context& rctx) override {
        data_flow_object(rctx, std::make_unique<flow>(std::addressof(rctx), this));
    }
private:
};

}

