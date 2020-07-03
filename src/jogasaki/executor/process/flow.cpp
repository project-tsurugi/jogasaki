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
#include "flow.h"

namespace jogasaki::executor::process {

flow::flow(flow::record_meta_list input_meta, flow::record_meta_list subinput_meta, flow::record_meta_list output_meta,
    request_context *context, common::step *step, std::shared_ptr<impl::processor_info> info) :
    input_meta_(std::move(input_meta)),
    subinput_meta_(std::move(subinput_meta)),
    output_meta_(std::move(output_meta)),
    context_(context),
    step_(step),
    info_(std::move(info))
{}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    auto& res = context_->compiler_context()->compiler_result();
    auto& stmt = res.statement();
    std::unique_ptr<abstract::task_context> task_context{};
    std::shared_ptr<impl::processor> proc{};
    switch(stmt.kind()) {
        case takatori::statement::statement_kind::execute:
            proc = std::make_shared<impl::processor>(info_);
            break;
        case takatori::statement::statement_kind::write:
            //FIXME
        default:
            takatori::util::fail();
    }
    tasks_.emplace_back(std::make_unique<task>(context_, step_, std::move(task_context), std::move(proc)));
    return tasks_;
}

takatori::util::sequence_view<std::shared_ptr<model::task>> flow::create_pretask(flow::port_index_type subinput) {
    (void)subinput;
    // TODO create prepare task for the index
    return {};
}

common::step_kind flow::kind() const noexcept {
    return common::step_kind::process;
}
}


