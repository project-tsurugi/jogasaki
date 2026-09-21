/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "task_completion.h"

#include <memory>

#include <jogasaki/event.h>
#include <jogasaki/executor/common/utils.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>

namespace jogasaki::executor::common {

void complete_dag_task(
    request_context& context,
    model::step::identity_type step_id,
    model::task::identity_type task_id
) {
    send_event(context, event_enum_tag<event_kind::task_completed>, step_id, task_id);

    if (global::config_pool()->inplace_dag_schedule()) {
        scheduler::dag_schedule(context);
        return;
    }
    context.scheduler()->schedule_task(
        scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::dag_events>,
            std::addressof(context)
        }
    );
}

}
