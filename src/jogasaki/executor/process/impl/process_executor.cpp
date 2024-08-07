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
#include "process_executor.h"

#include <utility>

#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>

namespace jogasaki::executor::process::impl {

abstract::process_executor_factory& default_process_executor_factory() {
    static abstract::process_executor_factory f = [](
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts
    ) {
        return std::make_shared<process_executor>(std::move(processor), std::move(contexts));
    };
    return f;
}

process_executor::status process_executor::run() {
    // assign context
    auto context = contexts_->pop();

    // execute task
    auto rc = processor_->run(context.get());
    switch(rc) {
        case status::completed:
        case status::completed_with_errors:
             // Do Nothing
            break;
        case status::to_sleep:
        case status::to_yield:
        default:
            // task is suspended in the middle, put the current context back
            contexts_->push(std::move(context));
            break;
    }
    return rc;
}

process_executor::process_executor(
    std::shared_ptr<abstract::processor> processor,
    std::vector<std::shared_ptr<abstract::task_context>> contexts
) :
    processor_(std::move(processor)),
    contexts_(std::make_shared<impl::task_context_pool>(std::move(contexts)))
{}

}


