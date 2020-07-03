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
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/request_context.h>

#include "process_executor.h"

namespace jogasaki::executor::process {

class task : public common::task {
public:
    task() = default;
    task(request_context* context,
        step_type* src,
        std::unique_ptr<abstract::task_context> task_context,
        std::shared_ptr<abstract::processor> processor
    ) :
        common::task(context, src),
        task_context_(std::move(task_context)),
        processor_(std::move(processor))
    {}

    model::task_result operator()() override {
        VLOG(1) << *this << " process::task executed.";

        // setup process_executor with the processor_

        // have process_executor setup the context

        // run processor with the context

        // map return code from the status code returned by processor::run()

        // raise appropriate event if needed
        context()->channel()->emplace(takatori::util::enum_tag<event_kind::task_completed>, id(), id());

        // TODO support sleep/yield
        return jogasaki::model::task_result::complete;
    }

private:
    std::unique_ptr<abstract::task_context> task_context_{};
    std::shared_ptr<abstract::processor> processor_{};
};

}



