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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::exchange {

using takatori::util::maybe_shared_ptr;

class step : public common::step {
public:
    step() = default;

    step(
        maybe_shared_ptr<meta::record_meta> input_meta,
        meta::variable_order column_order
    ) :
        input_meta_(std::move(input_meta)),
        column_order_(std::move(column_order))
    {}

    void notify_prepared() override {
        // no-op for exchange
    }
    void notify_completed() override {
        // no-op for exchange
    }

    /**
     * @brief accessor to column_order
     * @details column order used for input. Some exchanges (forward, broadcast) use this for output as well.
     * @return column order for exchange input
     */
    [[nodiscard]] meta::variable_order const& input_order() const noexcept {
        return column_order_;
    }

    /**
     * @brief accessor to input meta
     * @details returns record_meta used for input. Some exchanges (forward, broadcast) use this for output as well.
     * @return record_meta for exchange input
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& input_meta() const noexcept {
        return input_meta_;
    }

    [[nodiscard]] bool handles_group() const noexcept {
        return kind() == common::step_kind::group || kind() == common::step_kind::aggregate;
    }
private:
    maybe_shared_ptr<meta::record_meta> input_meta_{};
    meta::variable_order column_order_{};
};

}
