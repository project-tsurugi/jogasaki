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

#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <tateyama/context.h>

namespace jogasaki::scheduler {

void flat_task::bootstrap() {
    VLOG(1) << *this << " bootstrap task executed.";
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*job_context_->dag_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(*graph_);
}

void flat_task::dag_schedule() {
    VLOG(1) << *this << " dag scheduling task executed.";
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*job_context_->dag_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process(false);
}

void flat_task::teardown() {
    VLOG(1) << *this << " teardown task executed.";
    job_context_->completion_latch().open();
}

void flat_task::operator()(tateyama::context& ctx) {
    (void)ctx;
    switch(kind_) {
        using kind = flat_task_kind;
        case kind::dag_events: dag_schedule(); return;
        case kind::bootstrap: bootstrap(); return;
        case kind::teardown: teardown(); return;
        case kind::wrapped: {
            model::task_result res{};
            while((res = (*origin_)()) == model::task_result::proceed) {}
            return;
        }
    }
}

flat_task::identity_type flat_task::id() const {
    if (origin_) {
        return origin_->id();
    }
    return undefined_id;
}

}



