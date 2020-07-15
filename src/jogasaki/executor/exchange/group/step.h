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

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/task.h>
#include <jogasaki/meta/variable_order.h>
#include "shuffle_info.h"
#include "flow.h"

namespace jogasaki::executor::process {
class step;
}
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
    step();

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    explicit step(
        std::shared_ptr<shuffle_info> info
    );

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    step(
        std::shared_ptr<shuffle_info> info,
        meta::variable_order input_column_order,
        meta::variable_order output_column_order
    );

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     */
    step(
        std::shared_ptr<meta::record_meta> input_meta,
        std::vector<field_index_type> key_indices
    );

    [[nodiscard]] executor::common::step_kind kind() const noexcept override;

    void activate() override;

    [[nodiscard]] meta::variable_order const& input_column_order() const noexcept {
        return input_column_order_;
    }

    [[nodiscard]] meta::variable_order const& output_column_order() const noexcept {
        return output_column_order_;
    }
protected:
    [[nodiscard]] process::step* downstream(std::size_t index) const noexcept;

    [[nodiscard]] process::step* upstream(std::size_t index) const noexcept;

private:
    std::shared_ptr<shuffle_info> info_{};
    meta::variable_order input_column_order_{};
    meta::variable_order output_column_order_{};
};

}


