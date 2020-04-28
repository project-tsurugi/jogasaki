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

#include <vector>

#include <executor/common/step_kind.h>

namespace jogasaki::executor::common {

/**
 * @brief group step data flow
 */
class flow {
public:
    /**
     * @brief index used to identify the port attached to this step
     * Each set of Main input ports, Sub input ports, and Output ports consists a category and indexes are 0-origin unique number within each category.
     */
    using port_index_type = std::size_t;

    virtual ~flow() = default;

    [[nodiscard]] virtual step_kind kind() const noexcept = 0;

    /**
     * @brief request step to create main tasks required
     * @return list of 0 or more tasks that should be newly executed to process main input
     * The tasks are owned by the step.
     */
    virtual takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() = 0;

    /**
     * @brief request step to create prepare task to process input to the given sub-input port
     * @return list of 0 or a task that should be newly executed to process sub input
     * The tasks are owned by the step.
     */
    virtual takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index_type subinput) = 0;
};

}


