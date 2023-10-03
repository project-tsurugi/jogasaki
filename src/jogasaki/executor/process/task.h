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

#include <memory>

#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/process/impl/process_executor.h>

namespace jogasaki::executor::process {

class task : public common::task {
public:
    task() = default;
    task(
        request_context* context,
        step_type* src,
        std::shared_ptr<abstract::process_executor> exec,
        std::shared_ptr<abstract::processor> processor,
        bool has_transactional_io
    );

    [[nodiscard]] model::task_result operator()() override;

    [[nodiscard]] bool has_transactional_io() override;

private:
    std::shared_ptr<abstract::process_executor> executor_{};
    std::shared_ptr<abstract::processor> processor_{};
    bool has_transactional_io_{};
};

}



