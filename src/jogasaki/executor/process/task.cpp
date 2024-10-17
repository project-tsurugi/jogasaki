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
#include "task.h"

#include <functional>
#include <ostream>
#include <type_traits>
#include <utility>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/callback.h>
#include <jogasaki/event.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/common/utils.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::process {

task::task(
    request_context *context,
    task::step_type *src,
    std::shared_ptr<abstract::process_executor> exec,
    std::shared_ptr<abstract::processor> processor,
    bool has_transactional_io
) :
    common::task(context, src),
    executor_(std::move(exec)),
    processor_(std::move(processor)),
    has_transactional_io_(has_transactional_io)
{}

model::task_result task::operator()() {
    VLOG_LP(log_debug) << *this << " process::task executed.";
    if(auto&& cb = step()->did_start_task(); cb) {
        callback_arg arg{ id() };
        (*cb)(&arg);
    }

    auto status = executor_->run();
    switch (status) {
        case abstract::status::completed:
            VLOG_LP(log_debug) << *this << " process::task completed.";
            // raise appropriate event if needed
            common::send_event(*context(), event_enum_tag<event_kind::task_completed>, step()->id(), id());
            break;
        case abstract::status::completed_with_errors:
            VLOG_LP(log_warning) << *this << " task completed with errors";
            // raise appropriate event if needed
            common::send_event(*context(), event_enum_tag<event_kind::task_completed>, step()->id(), id());
            break;
        case abstract::status::to_sleep:
            // TODO support sleep/yield

            fail_with_exception();
            break;
        case abstract::status::to_yield:
            VLOG_LP(log_warning) << *this << " process::task to_yield.";
            break;
        default:
            fail_with_exception();
            break;
    }

    context()->scheduler()->schedule_task(
        scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::dag_events>,
                context()
        }
    );

    if(auto&& cb = step()->will_end_task(); cb) {
        callback_arg arg{ id() };
        (*cb)(&arg);
    }
    switch (status) {
        case abstract::status::completed:
        case abstract::status::completed_with_errors:
        case abstract::status::to_sleep:
            return jogasaki::model::task_result::complete;
        case abstract::status::to_yield:
            return jogasaki::model::task_result::yield;
        default:
            return jogasaki::model::task_result::complete;
    }
}

bool task::has_transactional_io() {
    return has_transactional_io_;
}

} // namespace jogasaki::executor::process



