/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "process_executor.h"

#include <utility>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/io/writer_pool.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/processor.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/task_context_pool.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::process::impl {

abstract::process_executor_factory& default_process_executor_factory() {
    static abstract::process_executor_factory f = [](
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts
    ) {
        return std::make_shared<process_executor>(std::move(processor), std::move(contexts));
    };
    return f;
}

process_executor::status process_executor::run() {
    // assign context
    auto context = contexts_->pop();

    // use impl_ctx only when needs_seat=true. Mock task_context does not work with writer pool for now.
    auto* impl_ctx = dynamic_cast<impl::task_context*>(context.get());
    auto* rctx = impl_ctx ? impl_ctx->req_context() : nullptr;;

    // check if the task contains emit operator and needs a writer seat
    bool needs_seat = false;
    if (impl_ctx) {  // impl_ctx can be nullptr when testing
        if (auto* proc = dynamic_cast<impl::processor*>(processor_.get())) {
            if (proc->info() && proc->info()->details().has_emit_operator()) {
                // emit exists, so writer_pool must exist (even if channel is null_record_channel)
                // this check is just for safety
                if (rctx && rctx->writer_pool()) {
                    needs_seat = true;
                }
            }
        }
    }

    // acquire seat if needed
    if (needs_seat && ! impl_ctx->writer_seat().reserved()) {
        io::writer_seat seat{};
        if (! rctx->writer_pool()->acquire(seat)) {
            // failed to acquire seat, yield the task
            VLOG_LP(log_debug) << "writer_pool::acquire() failed, yielding task";
            contexts_->push(std::move(context));
            return status::to_yield;
        }
        VLOG_LP(log_trace) << "writer_pool::acquire() success";
        impl_ctx->writer_seat() = std::move(seat);
    }

    // execute task
    auto rc = processor_->run(context.get());

    // release seat if acquired
    if (needs_seat) {
        rctx->writer_pool()->release(std::move(impl_ctx->writer_seat()));
        VLOG_LP(log_trace) << "writer_pool::release() success";
    }

    switch(rc) {
        case status::completed:
        case status::completed_with_errors:
             // Do Nothing
            break;
        case status::to_sleep:
        case status::to_yield:
        default:
            // task is suspended in the middle, put the current context back
            contexts_->push(std::move(context));
            break;
    }
    return rc;
}

process_executor::process_executor(
    std::shared_ptr<abstract::processor> processor,
    std::vector<std::shared_ptr<abstract::task_context>> contexts
) :
    processor_(std::move(processor)),
    contexts_(std::make_shared<impl::task_context_pool>(std::move(contexts)))
{}

}


