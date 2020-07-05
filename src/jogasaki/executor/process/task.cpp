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
#include "task.h"

#include <takatori/util/enum_tag.h>

#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/process_executor.h>

namespace jogasaki::executor::process {

task::task(request_context *context, task::step_type *src, std::shared_ptr<impl::task_context_pool> task_contexts,
    std::shared_ptr<abstract::processor> processor) :
    common::task(context, src),
    task_contexts_(std::move(task_contexts)),
    processor_(std::move(processor))
{}

model::task_result task::operator()() {
    VLOG(1) << *this << " process::task executed.";
    process_executor executor{processor_, task_contexts_};
    auto status = executor.run();
    switch (status) {
        case abstract::status::completed:
            break;
        case abstract::status::completed_with_errors:
            LOG(WARNING) << *this << " task completed with errors";
            break;
        case abstract::status::to_sleep:
        case abstract::status::to_yield:
            // TODO support sleep/yield
            takatori::util::fail();
    }
    // raise appropriate event if needed
    context()->channel()->emplace(takatori::util::enum_tag<event_kind::task_completed>, id(), id());
    return jogasaki::model::task_result::complete;
}
}



