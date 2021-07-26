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
#include "flat_task.h"

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <tateyama/task_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <tateyama/context.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

void flat_task::operator()(tateyama::context& ctx) {
    (void)ctx;
    if (dag_scheduling_) {
        auto& sc = scheduler::statement_scheduler::impl::get_impl(*request_context_->dag_scheduler());
        auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
        dc.process(false);
        return;
    }
    auto res = (*origin_)();
    (void)res;
}

flat_task::identity_type flat_task::id() const {
    if (origin_) {
        return origin_->id();
    }
    return undefined_id;
}

}



