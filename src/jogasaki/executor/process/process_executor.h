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

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include "impl/task_context_pool.h"

namespace jogasaki::executor::process {

/**
 * @brief process executor
 * @details process executor is responsible for set-up processor context and execute the processor in order to
 * complete the work assigned to a processor task.
 */
class process_executor {
    using status = abstract::status;
public:
    /**
     * @brief construct new instance
     */
    process_executor() = default;

    explicit process_executor(std::shared_ptr<abstract::processor> processor,
        std::shared_ptr<impl::task_context_pool> contexts) : processor_(std::move(processor)), contexts_(std::move(contexts)) {}

    status run() {
        // assign context
        auto context = contexts_->pop();

        // execute task
        auto rc = processor_->run(context.get());

        if (rc != status::completed && rc != status::completed_with_errors) {
            // task is suspended in the middle, put the current context back
            contexts_->push(context);
        }
        return rc;
    }

private:
    std::shared_ptr<abstract::processor> processor_{};
    std::shared_ptr<impl::task_context_pool> contexts_{};
};

}


