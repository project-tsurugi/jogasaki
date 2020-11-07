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
#include <memory>

#include <jogasaki/request_context.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/process/impl/processor.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include "task.h"

namespace jogasaki::executor::process {

using ::takatori::util::sequence_view;
class step;

/**
 * @brief process step data flow
 */
class flow : public common::flow {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    flow() = default;

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     */
    flow(
        request_context* context,
        process::step* step,
        std::shared_ptr<processor_info> info
    );

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type subinput) override;

    [[nodiscard]] common::step_kind kind() const noexcept override;

private:
    request_context* context_{};
    std::vector<std::shared_ptr<model::task>> tasks_{};
    step* step_{};
    std::shared_ptr<processor_info> info_{};

    [[nodiscard]] std::shared_ptr<impl::task_context> create_task_context(std::size_t partition, impl::ops::operator_container const& operators);
};

}


