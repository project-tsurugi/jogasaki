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

#include <cstddef>
#include <memory>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/exchange/shuffle/step.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/request_context.h>

#include "flow.h"
#include "group_info.h"

namespace jogasaki::executor::process {
class step;
}

namespace jogasaki::executor::exchange::group {

/**
 * @brief group step
 */
class step : public shuffle::step {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    step();

    /**
     * @brief create new instance
     * @param info shuffle info for this object
     * @param input_column_order column ordering information for exchange input
     * @param output_column_order column ordering information for exchange output
     */
    explicit step(
        std::shared_ptr<group_info> info,
        meta::variable_order input_column_order,
        meta::variable_order output_column_order
    );

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     * @param key_indices indices for key fields
     * @param input_column_order column ordering information for exchange input
     * @param output_column_order column ordering information for exchange output
     */
    step(
        maybe_shared_ptr<meta::record_meta> input_meta,
        std::vector<field_index_type> key_indices,
        meta::variable_order input_column_order,
        meta::variable_order output_column_order
    );

    [[nodiscard]] model::step_kind kind() const noexcept override {
        return model::step_kind::group;
    }

    void activate(request_context& rctx) override;

    [[nodiscard]] meta::variable_order const& output_order() const noexcept override;

    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& output_meta() const noexcept override;

protected:
    [[nodiscard]] process::step* downstream(std::size_t index) const noexcept;

    [[nodiscard]] process::step* upstream(std::size_t index) const noexcept;

private:
    std::shared_ptr<group_info> info_{};
    meta::variable_order output_column_order_{};
};

}  // namespace jogasaki::executor::exchange::group
