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

namespace jogasaki::executor::process {

class task : public common::task {
public:
    task() = default;
    task(std::shared_ptr<request_context> context,
            step_type* src,
            std::unique_ptr<processor_context> context,
            std::unique_ptr<processor> processor
            ) :
            common::task(std::move(context), src),
            processor_context_(std::move(processor_context)),
            processor_(std::move(processor))
            {}

    model::task_result operator()() override {
        VLOG(1) << *this << " process::task executed.";

        process_executor executor{context_, processor_};
        executor.run(*context_);

        context_->channel()->emplace(event_kind_tag<event_kind::task_completed>, src_->id(), id());
        return model::task_result::complete;
    }

private:
    std::unique_ptr<processor_context> processor_context_{};
    std::unique_ptr<processor> processor_{};
};

}



