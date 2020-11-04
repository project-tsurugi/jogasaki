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
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/block_scope_info.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/relation_io_map.h>

namespace jogasaki::executor::process {
class step;
}
namespace jogasaki::executor::process::impl {

/**
 * @brief processor implementation
 */
class processor : public process::abstract::processor {
public:
    processor() = default;

    explicit processor(
        std::shared_ptr<processor_info> info,
        plan::compiler_context const& compiler_ctx,
        std::shared_ptr<ops::io_info> io_info,
        std::shared_ptr<relation_io_map> relation_io_map,
        std::shared_ptr<kvs::database> database
    );

    [[nodiscard]] abstract::status run(abstract::task_context* context) override;

    [[nodiscard]] ops::operator_container const& operators() const noexcept {
        return operators_;
    }

private:
    std::shared_ptr<processor_info> info_{};
    ops::operator_container operators_{};
    std::shared_ptr<relation_io_map> relation_io_map_{};
    std::shared_ptr<kvs::database> database_{};
};

}


