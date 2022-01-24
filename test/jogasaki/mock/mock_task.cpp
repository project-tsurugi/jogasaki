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
#include "mock_task.h"

#include <memory>
#include <glog/logging.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/common/utils.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/utils/port_indices.h>

namespace jogasaki::executor {

model::task_result mock_task::operator()() {
    execute();
    if (count_ == 0) {
        notify_downstream();
    }
    ++count_;
    bool has_next = count_ < limit_;
    if (!has_next) {
        common::send_event(*context_, event_enum_tag<event_kind::task_completed>, src_->id(), id());
    }
    context()->scheduler()->schedule_task(
        scheduler::flat_task{scheduler::task_enum_tag<scheduler::flat_task_kind::dag_events>, context()});
    return has_next ? model::task_result::proceed : model::task_result::complete;
}

void mock_task::notify_downstream() {
    if (!src_->output_ports().empty()) {
        for(auto& oport : src_->output_ports()) {
            for(auto& o : oport->opposites()) {
                auto* downstream = o->owner();
                if(dynamic_cast<exchange::group::step*>(downstream)) {
                    // blocking exchange should not raise providing
                    continue;
                }
                model::step::port_index_type index = o->kind() == port_kind::main ? utils::input_port_index(*o->owner(), *o) : utils::subinput_port_index(
                    *o->owner(), *o);
                common::send_event(*context_, event_enum_tag<event_kind::providing>, downstream->id(), o->kind(), index);
            }
        }
    }
}
}
