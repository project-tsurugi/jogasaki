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
#include <jogasaki/channel.h>

namespace jogasaki::common_cli {

class task_base : public executor::common::task {
public:
    task_base() = default;
    task_base(std::shared_ptr<request_context> context, model::step* src, bool is_pretask = false) : context_(std::move(context)), src_(src), is_pretask_(is_pretask) {}

    model::task_result operator()() override {
        execute();
        ++count_;
        context_->channel()->emplace(event_kind_tag<event_kind::task_completed>, src_->id(), id());
        return model::task_result::complete;
    };

    virtual void execute() = 0;

protected:
    std::shared_ptr<request_context> context_{}; //NOLINT
    model::step* src_{}; //NOLINT
    bool is_pretask_{false}; //NOLINT
    std::size_t count_{0}; //NOLINT
};

}
