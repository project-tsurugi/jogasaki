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
#include <jogasaki/executor/process/impl/relop/operators_executor.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/block_variables_info.h>
#include <jogasaki/executor/process/impl/relop/relational_operators.h>

namespace jogasaki::executor::process::impl {

namespace graph = takatori::graph;
namespace relation = takatori::relation;

/**
 * @brief processor implementation for production
 */
class processor : public process::abstract::processor {
public:
    processor() = default;

    explicit processor(std::shared_ptr<processor_info> info) noexcept;

    abstract::status run(abstract::task_context* context) override;

private:
    std::shared_ptr<processor_info> info_{};
    std::vector<block_variables_info> variables_info_{};
    relop::relational_operators operators_{};
};

}


