/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/process_executor.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::process {

class task : public common::task {
public:
    task() = default;
    task(
        request_context* context,
        step_type* src,
        std::shared_ptr<abstract::process_executor> exec,
        std::shared_ptr<abstract::processor> processor,
        model::task_transaction_kind transaction_capability
    );

    [[nodiscard]] model::task_result operator()() override;

    [[nodiscard]] model::task_transaction_kind transaction_capability() override;

private:
    std::shared_ptr<abstract::process_executor> executor_{};
    std::shared_ptr<abstract::processor> processor_{};
    model::task_transaction_kind transaction_capability_{};
};

}



