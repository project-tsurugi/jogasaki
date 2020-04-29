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

#include <model/step.h>
#include <model/task.h>
#include <constants.h>
#include <executor/process/step.h>
#include "cogroup_flow.h"

namespace jogasaki::executor {

class cogroup_process : public process::step {
public:
    cogroup_process() : step(1, 1) {};
    explicit cogroup_process(model::graph* owner, std::shared_ptr<meta::group_meta> meta) : meta_(std::move(meta)) {
        graph_ = owner;
    }

    void activate() override {
        auto p = dynamic_cast<exchange::step*>(input_ports()[0]->opposites()[0]->owner());
        auto ch = graph_ ? &graph_->get_channel() : nullptr;
        data_flow_object_ = std::make_unique<cogroup_flow>(p, this, ch, meta_);
    }

private:
    std::shared_ptr<meta::group_meta> meta_{};
};

}
