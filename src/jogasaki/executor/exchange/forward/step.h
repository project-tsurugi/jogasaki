/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

#include "flow.h"
#include "forward_info.h"

namespace jogasaki::executor::exchange::forward {

class step : public exchange::step {
public:
    step() = default;

    /**
     * @brief create new instance
     */
    step(
        std::shared_ptr<forward_info> info,
        meta::variable_order input_column_order
    );

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param limit the number of records to forward
     * @param input_column_order column ordering information for exchange input
     */
    explicit step(
        maybe_shared_ptr<meta::record_meta> input_meta,
        std::optional<std::size_t> limit = {},
        meta::variable_order input_column_order = {}
    );

    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::forward;
    }

    void activate(request_context& rctx) override;

    [[nodiscard]] meta::variable_order const& output_order() const noexcept;

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& output_meta() const noexcept;

private:
    std::shared_ptr<forward_info> info_{};
};

}  // namespace jogasaki::executor::exchange::forward
