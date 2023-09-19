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
#include "task_scheduler.h"

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

void task_scheduler::schedule_task(flat_task&& t, schedule_option opt) {
    if(t.kind() != flat_task_kind::teardown) {
        // teardown task is not counted as its existence is checked by job_context::completing()
        auto cnt = ++t.job()->task_count();
        (void)cnt;
        //VLOG(log_debug) << "incremented job " << t.job()->id() << " task count to " << cnt;
    }
    do_schedule_task(std::move(t), opt);
}

void task_scheduler::schedule_conditional_task(conditional_task&& t) {
    auto cnt = ++t.job()->task_count();
    (void)cnt;
    //VLOG(log_debug) << "incremented job " << t.job()->id() << " task count to " << cnt;
    do_schedule_conditional_task(std::move(t));
}

}



