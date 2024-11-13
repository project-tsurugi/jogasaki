/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/exchange/flow.h>
#include <jogasaki/executor/exchange/shuffle/flow.h>
#include <jogasaki/executor/exchange/shuffle/run_info.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/emit.h>
#include <jogasaki/executor/process/impl/process_executor.h>
#include <jogasaki/executor/process/impl/processor.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/task.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/assert.h>

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

std::size_t flow::check_empty_input_and_calculate_partitions() {
    // If upstreams are all empty shuffles, make single partition.
    auto& exchange_map = step_->io_exchange_map();
    bool empty = true;
    bool shuffle_input = false;
    empty_input_from_shuffle_ = false;
    for(std::size_t i=0, n=exchange_map->input_count(); i < n; ++i) {
        auto& flow = exchange_map->input_at(i)->data_flow_object(*context_);
        if(flow.kind() == model::step_kind::forward) {
            // if forward, downstream partition must be same as upstream partitions
            // for now, at most one input forward exchange exists
            return unsafe_downcast<executor::exchange::forward::flow>(flow).sink_count();
        }
        if(flow.kind() != model::step_kind::group && flow.kind() != model::step_kind::aggregate) {
            shuffle_input = false;
            break;
        }
        auto& shuffle_flow = unsafe_downcast<executor::exchange::shuffle::flow>(flow);
        shuffle_input = true;
        if (! shuffle_flow.info().empty_input()) {
            empty = false;
            break;
        }
    }
    empty_input_from_shuffle_ = shuffle_input && empty;
    return empty_input_from_shuffle_ ? 1 : step_->partitions();
}

sequence_view<std::shared_ptr<model::task>> flow::create_tasks() {
    std::shared_ptr<impl::processor> proc{};
    try {
        proc = std::make_shared<impl::processor>(
            info_,
            step_->io_info(),
            step_->relation_io_map(),
            *step_->io_exchange_map(),
            context_
        );
    } catch (plan::impl::compile_exception const& e) {
        error::set_error_info(*context_, e.info());
        return {};
    }
    // create process executor
    auto& factory = step_->executor_factory() ? *step_->executor_factory() : impl::default_process_executor_factory();
    std::vector<std::shared_ptr<abstract::task_context>> contexts{};
    auto partitions = check_empty_input_and_calculate_partitions();
    auto& operators = proc->operators();
    auto external_output = operators.io_exchange_map().external_output();
    auto ch = context_->record_channel();
    if (ch && external_output != nullptr) {
        auto& emit = unsafe_downcast<impl::ops::emit>(*external_output);
        ch->meta(emit.meta());
    }
    std::size_t sink_idx_base = 0;
    auto& exchange_map = step_->io_exchange_map();

    // currently at most one output exchange exists
    assert_with_exception(exchange_map->output_count() <= 1, exchange_map->output_count());
    for(std::size_t i=0, n=exchange_map->output_count(); i < n; ++i) {
        auto& f = dynamic_cast<executor::exchange::flow&>(exchange_map->output_at(i)->data_flow_object(*context_));
        f.setup_partitions(partitions);
        sink_idx_base = f.sink_count() - partitions;
    }

    contexts.reserve(partitions);
    for (std::size_t i=0; i < partitions; ++i) {
        contexts.emplace_back(create_task_context(i, operators, sink_idx_base + i));
    }

    bool is_rtx = context_->transaction()->option()->type()
        ==  kvs::transaction_option::transaction_type::read_only;
    auto& d = info_->details();
    auto exec = factory(proc, contexts);
    for (std::size_t i=0; i < partitions; ++i) {
        tasks_.emplace_back(std::make_unique<task>(
            context_,
            step_,
            exec,
            proc,
                !(is_rtx && global::config_pool()->rtx_parallel_scan() ) &&
                (
                    d.has_write_operations() ||
                    d.has_find_operator() ||
                    d.has_scan_operator() ||
                    d.has_join_find_or_scan_operator()
                )
        ));
    }
    return tasks_;
}

sequence_view<std::shared_ptr<model::task>> flow::create_pretask(flow::port_index_type subinput) {
    (void)subinput;
    // TODO create prepare task for the index
    return {};
}

model::step_kind flow::kind() const noexcept {
    return model::step_kind::process;
}

std::shared_ptr<impl::task_context> flow::create_task_context(
    std::size_t partition,
    impl::ops::operator_container const& operators,
    std::size_t sink_index
) {
    auto external_output = operators.io_exchange_map().external_output();
    auto ctx = std::make_shared<impl::task_context>(
        *context_,
        partition,
        operators.io_exchange_map(),
        operators.range(),
        (context_->record_channel() && external_output != nullptr) ? context_->record_channel().get() : nullptr,
        sink_index
    );

    ctx->work_context(
        std::make_unique<impl::work_context>(
            context_,
            operators.size(),
            info_->vars_info_list().size(),
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            context_->database(),
            context_->transaction(),
            empty_input_from_shuffle_
        )
    );
    return ctx;
}

}  // namespace jogasaki::executor::process
