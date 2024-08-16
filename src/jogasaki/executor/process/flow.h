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

#include <cstddef>
#include <memory>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/impl/processor.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/interference_size.h>

#include "task.h"

namespace jogasaki::executor::process {

namespace impl {
class task_context;
}

using ::takatori::util::sequence_view;
class step;

/**
 * @brief process step data flow
 */
class cache_align flow : public model::flow {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    flow() = default;

    /**
     * @brief create new instance
     * @param context the request context containing this process
     * @param step the associated step
     * @param info the processor information
     */
    flow(
        request_context* context,
        process::step* step,
        std::shared_ptr<processor_info> info
    );

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type subinput) override;

    [[nodiscard]] model::step_kind kind() const noexcept override;

private:
    request_context* context_{};
    std::vector<std::shared_ptr<model::task>> tasks_{};
    step* step_{};
    std::shared_ptr<processor_info> info_{};
    bool empty_input_from_shuffle_{};

    [[nodiscard]] std::shared_ptr<impl::task_context> create_task_context(
        std::size_t partition,
        impl::ops::operator_container const& operators,
        std::size_t sink_index
    );
    std::size_t check_empty_input_and_calculate_partitions();
};

}


