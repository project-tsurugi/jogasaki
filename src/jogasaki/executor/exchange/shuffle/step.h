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

#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/meta/variable_order.h>

namespace jogasaki::executor::exchange::shuffle {

/**
 * @brief group step
 */
class step : public exchange::step {
public:
    using field_index_type = meta::record_meta::field_index_type;

    step() = default;

    step(
        std::shared_ptr<meta::record_meta> input_meta,
        meta::variable_order column_order
    ) : exchange::step(std::move(input_meta), std::move(column_order))
    {}

    [[nodiscard]] virtual meta::variable_order const& output_order() const noexcept = 0;

    [[nodiscard]] virtual std::shared_ptr<meta::group_meta> const& output_meta() const noexcept = 0;
};

}


