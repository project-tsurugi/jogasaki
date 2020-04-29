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

#include <memory>
#include <glog/logging.h>

#include <model/task.h>
#include <model/step.h>
#include <executor/common/task.h>
#include <executor/exchange/group/step.h>
#include <channel.h>
#include <utils.h>

namespace jogasaki::executor {

class task_base : public common::task {
public:
    using writers_type = takatori::util::reference_list_view<takatori::util::universal_extractor<record_writer>>;
    using readers_type = takatori::util::reference_list_view<takatori::util::universal_extractor<group_reader>>;

    task_base() = default;

    task_base(channel* channel, model::step* src, bool is_pretask = false) : channel_(channel), src_(src), is_pretask_(is_pretask) {}

    model::task_result operator()() override {
        execute();
        ++count_;
        channel_->emplace(event_kind_tag<event_kind::task_completed>, src_->id(), id());
        return model::task_result::complete;
    };

    virtual void execute() = 0;

protected:
    channel* channel_{};
    model::step* src_{};
    bool is_pretask_{false};
    std::size_t count_{0};

    void notify_downstream() {
        if (!src_->output_ports().empty()) {
            for(auto& oport : src_->output_ports()) {
                for(auto& o : oport->opposites()) {
                    auto* downstream = o->owner();
                    if(dynamic_cast<exchange::group::step*>(downstream)) {
                        // blocking exchange should not raise upstream_providing
                        continue;
                    }
                    model::step::port_index_type index = o->kind() == port_kind::main ? input_port_index(*o->owner(), *o) : subinput_port_index(
                            *o->owner(), *o);
                    channel_->emplace(event_kind_tag<event_kind::upstream_providing>, downstream->id(), o->kind(), index);
                }
            }
        }
    }
};

}
