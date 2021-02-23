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

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/event_channel.h>
#include <jogasaki/utils/port_indices.h>

namespace jogasaki::executor {

class mock_task : public common::task {
public:
    using writers_type = takatori::util::reference_list_view<takatori::util::universal_extractor<record_writer>>;
    using readers_type = takatori::util::reference_list_view<takatori::util::universal_extractor<group_reader>>;

    mock_task() = default;
    ~mock_task() override = default;
    mock_task(mock_task&& other) noexcept = default;
    mock_task& operator=(mock_task&& other) noexcept = default;
    mock_task(request_context* context,
            model::step* src,
            bool is_pretask = false) : context_(context), src_(src), is_pretask_(is_pretask) {}

    model::task_result operator()() override {
        execute();
        if (count_ == 0) {
            notify_downstream();
        }
        ++count_;
        bool has_next = count_ < limit_;
        if (!has_next) {
            context_->channel()->emplace(event_enum_tag<event_kind::task_completed>, src_->id(), id());
        }
        return has_next ? model::task_result::proceed : model::task_result::complete;
    };
    virtual void execute() = 0;
protected:
    request_context* context_{};
    model::step* src_{};
    bool is_pretask_{false};
    std::size_t count_{0};
    std::size_t limit_{3};

    void notify_downstream() {
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
                    context_->channel()->emplace(event_enum_tag<event_kind::providing>, downstream->id(), o->kind(), index);
                }
            }
        }
    }
};

}
