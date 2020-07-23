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

#include <takatori/graph/graph.h>
#include <takatori/plan/graph.h>
#include <yugawara/compiler_result.h>

#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/ops/operator_executor.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/block_variables_info.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor implementation for production
 */
class processor : public process::abstract::processor {
public:
    processor() = default;

    explicit processor(std::shared_ptr<processor_info> info, plan::compiler_context const& compiler_ctx) ;

    abstract::status run(abstract::task_context* context) override;

    [[nodiscard]] ops::operator_container const& operators() const noexcept {
        return operators_;
    }
private:
    std::shared_ptr<processor_info> info_{};
    ops::operator_container operators_{};
};

}


