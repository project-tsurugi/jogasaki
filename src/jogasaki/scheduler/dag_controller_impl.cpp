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
#include "dag_controller_impl.h"

#include <map>
#include <queue>
#include <thread>
#include <glog/logging.h>

#include <takatori/util/enum_tag.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/graph.h>
#include <jogasaki/event.h>
#include <jogasaki/event_channel.h>
#include <jogasaki/internal_event.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/step_state_table.h>
#include <jogasaki/utils/interference_size.h>
#include "serial_task_scheduler.h"
#include "parallel_task_scheduler.h"
#include "step_state.h"
#include "dag_controller.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;

using step = model::step;
using task_kind = model::task_kind;

template<auto Kind>
using enum_tag_t = takatori::util::enum_tag_t<Kind>;

void dag_controller::impl::operator()(enum_tag_t<event_kind::providing>, event& e) {
    if (auto v = graph_->find_step(e.target())) {
        DVLOG(1) << *v << " got notified upstream started providing";
        if (e.source_port_kind() == port_kind::sub) {
            // start prepare task for the providing port
            start_pretask(*v, e.source_port_index());        // no-op if task already running for the port
            step_state(*v, step_state_kind::preparing); // no-op if already preparing
        } else if (steps_[v->id()].state_ == step_state_kind::prepared && output_ports_activated(*v)) {
            // upstream providing indicates this step never cogroups, so you can start if preparation completed
            start_running(*v);
        }
        return;
    }
    throw std::domain_error("invalid event target");
}

void dag_controller::impl::operator()(enum_tag_t<event_kind::task_completed>, event& e) {
    DVLOG(1) << "task[id=" << e.task() << "] completed";
    if (auto v = graph_->find_step(e.target())) {
        auto& tasks = steps_[v->id()];
        auto k = tasks.task_state(e.task(), task_state_kind::completed);
        if (tasks.completed(k)) {
            step_state(*v, k == task_kind::main ? step_state_kind::completed : step_state_kind::prepared);
        }
        return;
    }
    throw std::domain_error("invalid event target");
}

void dag_controller::impl::operator()(enum_tag_t<event_kind::completion_instructed>, event& e) {
    (void)e;
}

void dag_controller::impl::operator()(enum_tag_t<internal_event_kind::activate>, internal_event&, step* s) {
    auto& step = steps_[s->id()];
    if(step.state_ == step_state_kind::created) {
        s->activate();
        step.assign_slot(task_kind::pre, s->subinput_ports().size());
    }
    if (s->has_subinput()) {
        step_state(*s, step_state_kind::activated);
    } else {
        step_state(*s, step_state_kind::prepared);
    }
}

void dag_controller::impl::operator()(enum_tag_t<internal_event_kind::prepare>, internal_event&, step* s) {
    start_preparing(*s);
}

void dag_controller::impl::operator()(enum_tag_t<internal_event_kind::consume>, internal_event&, step* s) {
    start_running(*s);
}

void dag_controller::impl::operator()(enum_tag_t<internal_event_kind::deactivate>, internal_event&, step* s) {
    auto& st = steps_[s->id()].state_;
    if(st == step_state_kind::completed) {
        s->deactivate();
        step_state(*s, step_state_kind::deactivated);
    }
}

void dag_controller::impl::operator()(
    enum_tag_t<internal_event_kind::propagate_downstream_completing>,
    internal_event& ie,
    step* s
) {
    (void)s;
    (void)ie;
}

} // namespace
