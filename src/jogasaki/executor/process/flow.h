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
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/process/impl/processor.h>
#include <jogasaki/executor/process/impl/processor_info.h>
#include "task.h"

namespace jogasaki::executor::process {

using namespace jogasaki::model;
/**
 * @brief process step data flow
 */
class flow : public common::flow {
public:

    using field_index_type = meta::record_meta::field_index_type;

    using record_meta_list = std::vector<std::shared_ptr<meta::record_meta>>;

    /**
     * @brief create new instance with empty schema (for testing)
     */
    flow() = default;

    /**
     * @brief create new instance
     * @param input_meta input record metadata
     */
    flow(
            record_meta_list input_meta,
            record_meta_list subinput_meta,
            record_meta_list output_meta,
            request_context* context,
            common::step* step,
            std::shared_ptr<impl::processor_info> info
    );

    takatori::util::sequence_view<std::shared_ptr<model::task>> create_tasks() override;

    takatori::util::sequence_view<std::shared_ptr<model::task>> create_pretask(port_index_type subinput) override;

    [[nodiscard]] common::step_kind kind() const noexcept override;

private:
    record_meta_list input_meta_{};
    record_meta_list subinput_meta_{};
    record_meta_list output_meta_{};
    record_meta_list external_meta_{};
    request_context* context_{};
    std::vector<std::shared_ptr<model::task>> tasks_{};
    common::step* step_{};
    std::shared_ptr<impl::processor_info> info_{};
};

}


