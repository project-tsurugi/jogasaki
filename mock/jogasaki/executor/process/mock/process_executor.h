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
#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/callback.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/utils/performance_tools.h>

#include "task_context.h"

namespace jogasaki::executor::process::mock {

using takatori::util::unsafe_downcast;

class process_executor : public abstract::process_executor {
public:
    process_executor(
        std::shared_ptr<abstract::processor> processor,
        std::shared_ptr<abstract::task_context> context
    ) noexcept :
        processor_(std::move(processor)),
        pool_(std::make_shared<impl::task_context_pool>(
            std::vector<std::shared_ptr<abstract::task_context>>{std::move(context)}
        ))
    {}

    process_executor(
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts
    ) noexcept :
        processor_(std::move(processor)),
        pool_(std::make_shared<impl::task_context_pool>(std::move(contexts)))
    {}

    [[nodiscard]] status run() override {
        // assign context
        auto context = pool_->pop();
        auto& impl = *unsafe_downcast<mock::task_context>(context.get());  //NOLINT
        callback_arg arg{impl.partition()};
        if (will_run()) {
            (*will_run())(&arg);
        }
        // execute task
        auto rc = processor_->run(context.get());
        if (did_run()) {
            (*did_run())(&arg);
        }
        if (rc != status::completed && rc != status::completed_with_errors) {
            // task is suspended in the middle, put the current context back
            pool_->push(std::move(context));
        }
        return rc;
    }

private:
    std::shared_ptr<abstract::processor> processor_{};
    std::shared_ptr<impl::task_context_pool> pool_{};
};

}


