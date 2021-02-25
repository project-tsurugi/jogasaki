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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/impl/ops/emit.h>

namespace jogasaki::executor::process {

using takatori::util::unsafe_downcast;

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
    auto proc = std::make_shared<impl::processor>(
        info_,
        step_->io_info(),
        step_->relation_io_map(),
        *step_->io_exchange_map(),
        context_->request_resource()
    );
    // create process executor
    auto& factory = step_->executor_factory() ? *step_->executor_factory() : impl::default_process_executor_factory();

    std::vector<std::shared_ptr<abstract::task_context>> contexts{};
    auto partitions = step_->partitions();
    auto& operators = proc->operators();
    auto external_output = operators.io_exchange_map().external_output_count();
    // for now only one task within a request emits, fix when multiple emits happen
    BOOST_ASSERT(external_output <= 1);  //NOLINT
    auto* result = context_->result();
    if (result && external_output > 0) {
        auto& emit = unsafe_downcast<impl::ops::emit>(operators.io_exchange_map().external_output_at(0));
        result->initialize(partitions, emit.meta());
    }
    contexts.reserve(partitions);
    for (std::size_t i=0; i < partitions; ++i) {
        contexts.emplace_back(create_task_context(i, operators));
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
    auto ctx = std::make_shared<impl::task_context>(
        partition,
        operators.io_exchange_map(),
        operators.scan_info(), // simply pass back the scan info. In the future, scan can be parallel and different scan info are created and filled into the task context.
        (context_->result() && external_output > 0) ? &context_->result()->store(partition) : nullptr
    );

    ctx->work_context(
        std::make_unique<impl::work_context>(
            context_,
            operators.size(),
            info_->scopes_info().size(),
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            context_->database(),
            context_->transaction()
        )
    );
    return ctx;
}
}


