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

#include <memory>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/scan_info.h>

namespace jogasaki::executor::process {

flow::flow(
    request_context *context,
    process::step* step,
    std::shared_ptr<processor_info> info
) :
    context_(context),
    step_(step),
    info_(std::move(info))
{}

sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    auto& stmt = context_->compiler_context()->statement();
    std::shared_ptr<impl::processor> proc{};
    switch(stmt.kind()) {
        case takatori::statement::statement_kind::execute:
            proc = std::make_shared<impl::processor>(info_, *context_->compiler_context(), step_->io_info(), step_->relation_io_map(), context_->database());
            break;
        case takatori::statement::statement_kind::write:
            //FIXME
        default:
            takatori::util::fail();
    }
    // create process executor
    auto& factory = step_->executor_factory() ? *step_->executor_factory() : impl::default_process_executor_factory();

    std::vector<std::shared_ptr<abstract::task_context>> contexts{};
    auto partitions = step_->partitions();
    contexts.reserve(partitions);
    for (std::size_t i=0; i < partitions; ++i) {
        contexts.emplace_back(create_task_context(i, proc->operators().io_exchange_map()));
    }
    auto exec = factory(proc, contexts);
    for (std::size_t i=0; i < partitions; ++i) {
        tasks_.emplace_back(std::make_unique<task>(context_, step_, exec, proc));
    }
    return tasks_;
}

sequence_view<std::shared_ptr<model::task>> flow::create_pretask(flow::port_index_type subinput) {
    (void)subinput;
    // TODO create prepare task for the index
    return {};
}

common::step_kind flow::kind() const noexcept {
    return common::step_kind::process;
}

std::shared_ptr<impl::task_context> flow::create_task_context(std::size_t partition, impl::details::io_exchange_map const& io_exchange_map) {
    auto ctx = std::make_shared<impl::task_context>(
        partition,
        io_exchange_map,
        std::make_unique<impl::scan_info>() // TODO retrieve back from processor
    );
    ctx->work_context(std::make_unique<impl::work_context>());
    return ctx;
}
}


