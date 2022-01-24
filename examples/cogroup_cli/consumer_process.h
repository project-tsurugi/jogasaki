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
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>
#include "consumer_flow.h"
#include "params.h"

namespace jogasaki::cogroup_cli {

using takatori::util::maybe_shared_ptr;

class consumer_process : public executor::process::step {
public:
    consumer_process() = default;
    explicit consumer_process(maybe_shared_ptr<meta::group_meta> meta, params& c) :
            meta_(std::move(meta)), params_(&c) {}

    void activate(request_context& rctx) override {
        auto l = dynamic_cast<executor::exchange::step*>(input_ports()[0]->opposites()[0]->owner());
        auto r = dynamic_cast<executor::exchange::step*>(input_ports()[1]->opposites()[0]->owner());
        data_flow_object(
            rctx,
            std::make_unique<consumer_flow>(l, r, this, std::addressof(rctx), meta_, *params_)
        );
    }

    [[nodiscard]] std::size_t partitions() const noexcept override {
        return params_->downstream_partitions_;
    }

private:
    maybe_shared_ptr<meta::group_meta> meta_{};
    params* params_{};
};

}
