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

#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include "task_context_pool.h"

namespace jogasaki::executor::process::impl {

/**
 * @brief default process executor implementation with naive task assignment logic
 */
class process_executor : public abstract::process_executor {
public:
    /**
     * @brief construct new instance
     */
    process_executor() = default;

    process_executor(
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts)
        : processor_(std::move(processor)), contexts_(std::make_shared<impl::task_context_pool>(std::move(contexts)))
    {}

    [[nodiscard]] status run() override;

private:
    std::shared_ptr<abstract::processor> processor_{};
    std::shared_ptr<impl::task_context_pool> contexts_{};
};

/**
 * @brief global constant accessor to default process executor factory
 * @return factory of default process_executor implementation
 */
[[nodiscard]] abstract::process_executor_factory& default_process_executor_factory();

}
