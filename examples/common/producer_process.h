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

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/process/step.h>
#include "producer_task.h"
#include "producer_flow.h"

namespace jogasaki::common_cli {

using takatori::util::maybe_shared_ptr;

template <class Params>
class producer_process : public executor::process::step {
public:
    producer_process() = default;
    producer_process(maybe_shared_ptr<meta::record_meta> meta,
            Params& c) :
            meta_(std::move(meta)), params_(&c) {}

    void activate(request_context& rctx) override {
        auto p = dynamic_cast<executor::exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object(
            rctx,
            std::make_unique<producer_flow<Params>>(p, this, std::addressof(rctx), meta_, *params_)
        );
    }

    void deactivate(request_context& rctx) override {
        meta_.reset();
        executor::process::step::deactivate(rctx);
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    Params* params_{};
};

}
