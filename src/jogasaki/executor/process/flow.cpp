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
            proc = std::make_shared<impl::processor>(
                info_,
                *context_->compiler_context(),
                step_->io_info(),
                step_->relation_io_map(),
                step_->io_exchange_map().get(),
                context_->request_resource()
            );
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
        contexts.emplace_back(create_task_context(i, proc->operators()));
    }
    auto& exchange_map = step_->io_exchange_map();
    for(std::size_t i=0, n=exchange_map->output_count(); i < n; ++i) {
        (void)dynamic_cast<executor::exchange::flow&>(exchange_map->output_at(i)->data_flow_object()).setup_partitions(partitions);
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

std::shared_ptr<impl::task_context> flow::create_task_context(std::size_t partition, impl::ops::operator_container const& operators) {
    auto external_output = operators.io_exchange_map().external_output_count();
    auto* stores = context_->stores();
    if (stores) {
        stores->resize(stores->size() + external_output);
    }
    auto ctx = std::make_shared<impl::task_context>(
        partition,
        operators.io_exchange_map(),
        operators.scan_info(), // simply pass back the scan info. In the future, scan can be parallel and different scan info are created and filled into the task context.
        stores,
        context_->record_resource(), // TODO for now only one task within a request emits, fix when multiple emits happen
        context_->varlen_resource()  // TODO for now only one task within a request emits, fix when multiple emits happen
    );

    auto resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto varlen_resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());

    // FIXME work-around lifo_paged_memory_resource repeating acquiring/releasing pages to pool
    resource->allocate(64);
    varlen_resource->allocate(64);
    // FIXME end of work-around

    ctx->work_context(std::make_unique<impl::work_context>(
        operators.size(),
        info_->scopes_info().size(),
        std::move(resource),
        std::move(varlen_resource),
        context_->database(),
        context_->transaction()
    ));
    return ctx;
}
}


