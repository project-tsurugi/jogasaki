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
#include "step.h"

#include <utility>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/shuffle/step.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

#include "flow.h"
#include "forward_info.h"

namespace jogasaki::executor::exchange::forward {

// step::step() : info_(std::make_shared<forward_info>()) {}

step::step(
    std::shared_ptr<forward_info> info,
    meta::variable_order input_column_order
) :
    exchange::step(info->record_meta(), std::move(input_column_order)),
    info_(std::move(info))
{}

step::step(
    maybe_shared_ptr<meta::record_meta> input_meta,
    std::optional<std::size_t> limit,
    meta::variable_order input_column_order
) :
    step(
        std::make_shared<forward_info>(std::move(input_meta), limit),
        std::move(input_column_order)
    )
{}

void step::activate(request_context& rctx) {
    data_flow_object(
        rctx,
        std::make_unique<forward::flow>(info_, std::addressof(rctx), this)
    );
}

meta::variable_order const& step::output_order() const noexcept {
    return exchange::step::input_order();
}

maybe_shared_ptr<meta::record_meta> const& step::output_meta() const noexcept {
    return exchange::step::input_meta();
}

}  // namespace jogasaki::executor::exchange::forward
