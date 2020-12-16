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

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/plan/compiler_context.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief processor implementation
 * @details this object holds structural information about the process, e.g. operators logic object,
 * and shared by multiple threads or tasks. Immutable after construction.
 */
class processor : public process::abstract::processor {
public:
    processor() = default;

    /**
     * @brief create new object
     * @param info processor information
     * @param compiler_ctx compiler context
     * @param io_info input/output information
     * @param relation_io_map mapping from relation to input/output indices
     * @param io_exchange_map map from input/output to exchange operator
     * @param resource the memory resource to build the structures needed by this processor
     */
    processor(
        std::shared_ptr<processor_info> info,
        plan::compiler_context const& compiler_ctx,
        std::shared_ptr<ops::io_info> io_info,
        std::shared_ptr<relation_io_map> relation_io_map,
        io_exchange_map& io_exchange_map,
        memory::lifo_paged_memory_resource* resource
    );

    /**
     * @brief run the process logic on the given context
     * @param context the context object of the task
     * @return task execution status
     */
    [[nodiscard]] abstract::status run(abstract::task_context* context) override;

    /**
     * @brief accessor to the operators contained in the processor
     * @return the operators container
     */
    [[nodiscard]] ops::operator_container const& operators() const noexcept;

private:
    std::shared_ptr<processor_info> info_{};
    ops::operator_container operators_{};
    std::shared_ptr<relation_io_map> relation_io_map_{};
};

}


