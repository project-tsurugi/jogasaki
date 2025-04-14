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

#include <ostream>
#include <type_traits>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/event.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/common/utils.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/utils/cancel_request.h>

namespace jogasaki::executor::exchange {

model::task_result task::operator()() {
    VLOG_LP(log_debug) << *this << " exchange_task executed.";

    if(utils::request_cancel_enabled(request_cancel_kind::group)) {
        auto& res_src = context()->req_info().response_source();
        if(res_src && res_src->check_cancel()) {
            cancel_request(*context());
            scheduler::submit_teardown(*context());
            return model::task_result::complete;
        }
    }
    common::send_event(*context(), event_enum_tag<event_kind::task_completed>, step()->id(), id());

    if(global::config_pool()->inplace_dag_schedule()) {
        scheduler::dag_schedule(*context());
        return model::task_result::complete;
    }

    context()->scheduler()->schedule_task(
        scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::dag_events>,
                context()
        }
    );
    return model::task_result::complete;
}

}  // namespace jogasaki::executor::exchange
