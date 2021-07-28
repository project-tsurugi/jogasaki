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
#include <jogasaki/internal_event.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/step_state_table.h>
#include <jogasaki/utils/interference_size.h>
#include "serial_task_scheduler.h"
#include "parallel_task_scheduler.h"
#include "step_state.h"
#include "dag_controller.h"
#include "thread_params.h"
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/statement_scheduler.h>

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;  //NOLINT clang-tidy failed to find its usage

using step = model::step;
using task_kind = model::task_kind;

template<auto Kind>
using enum_tag_t = takatori::util::enum_tag_t<Kind>;

void dag_controller::impl::operator()(enum_tag_t<event_kind::providing>, event& e) {
    std::lock_guard guard{mutex_};
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
    std::lock_guard guard{mutex_};
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
    std::lock_guard guard{mutex_};
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

dag_controller::impl::impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler, dag_controller* parent) :
    cfg_(std::move(cfg)),
    executor_(std::addressof(scheduler)),
    parent_(parent)
{}

dag_controller::impl::impl(std::shared_ptr<configuration> cfg, dag_controller* parent) :
    cfg_(std::move(cfg)),
    executor_(cfg_->single_thread() ?
        std::shared_ptr<class task_scheduler>(std::make_shared<serial_task_scheduler>()) :
        std::shared_ptr<class task_scheduler>(
            std::make_shared<parallel_task_scheduler>(thread_params(cfg_))
        )
    ),
    parent_(parent)
{}

bool dag_controller::impl::all_steps_deactivated(model::graph& g) {
    for(auto&& v: g.steps()) {
        if (steps_[v->id()].state_ < step_state_kind::deactivated) {
            return false;
        }
    }
    return true;
}

bool dag_controller::impl::all_upstream_steps_past(step const& s, step_state_kind st) {
    for(auto&& iport : s.input_ports()) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            if (steps_[o->id()].state_ < st) {
                return false;
            }
        }
    }
    for(auto&& iport : s.subinput_ports()) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            if (steps_[o->id()].state_ < st) {
                return false;
            }
        }
    }
    return true;
}

bool dag_controller::impl::all_downstream_steps_past(step const& s, step_state_kind st) {
    for(auto&& oport : s.output_ports()) {
        for(auto&& opposite : oport->opposites()) {
            auto o = opposite->owner();
            if (steps_[o->id()].state_ < st) {
                return false;
            }
        }
    }
    return true;
}

bool dag_controller::impl::ports_completed(takatori::util::sequence_view<std::unique_ptr<model::port> const> ports) {
    // used only for input ports
    for(auto&& iport : ports) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            if (steps_[o->id()].state_ < step_state_kind::completed) {
                return false;
            }
        }
    }
    return true;
}

bool dag_controller::impl::output_ports_activated(step const& s) {
    for(auto&& oport : s.output_ports()) {
        for(auto&& opposite : oport->opposites()) {
            auto o = opposite->owner();
            if (steps_[o->id()].state_ < step_state_kind::activated) {
                return false;
            }
        }
    }
    return true;
}

bool dag_controller::impl::main_input_completed(step const& s) {
    return ports_completed(s.input_ports());
}

bool dag_controller::impl::sub_input_completed(step const& s) {
    return ports_completed(s.subinput_ports());
}

bool dag_controller::impl::input_completed(step const& s) {
    return main_input_completed(s) && sub_input_completed(s);
}

void dag_controller::impl::on_state_change(step const& s) {
    // first check neighborhood steps
    for(auto&& iport : s.input_ports()) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            check_and_generate_internal_events(*o);
        }
    }
    for(auto&& iport : s.subinput_ports()) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            check_and_generate_internal_events(*o);
        }
    }
    for(auto&& iport : s.output_ports()) {
        for(auto&& opposite : iport->opposites()) {
            auto o = opposite->owner();
            check_and_generate_internal_events(*o);
        }
    }
    // check myself
    check_and_generate_internal_events(s);
}

void dag_controller::impl::check_and_generate_internal_events(step const& s) {
    auto& st = steps_[s.id()].state_;
    switch(st) {
        case step_state_kind::uninitialized:
            // no-op
            break;
        case step_state_kind::created:
            if(!all_upstream_steps_past(s, step_state_kind::activated)) {
                break;
            }
            internal_events_.emplace(internal_event_kind::activate, s.id());
            break;
        case step_state_kind::activated: {
            if(!all_upstream_steps_past(s, step_state_kind::completed)) {
                break;
            }
            internal_events_.emplace(internal_event_kind::prepare, s.id());
            break;
        }
        case step_state_kind::preparing:
            // no-op
            break;
        case step_state_kind::prepared: {
            // start work task
            if(!output_ports_activated(s) || !all_upstream_steps_past(s, step_state_kind::completed)) {
                break;
            }
            internal_events_.emplace(internal_event_kind::consume, s.id());
            break;
        }
        case step_state_kind::running:
            // no-op
            break;
        case step_state_kind::completing:
            // TODO propagate
            break;
        case step_state_kind::completed:
            if(!all_upstream_steps_past(s, step_state_kind::completed) ||
                !all_downstream_steps_past(s, step_state_kind::completed)) {
                break;
            }
            internal_events_.emplace(internal_event_kind::deactivate, s.id());
            break;
        case step_state_kind::deactivated:
            if(all_steps_deactivated(*graph_)) {
                graph_deactivated_ = true;

                // make sure teardown task is submitted only once
                auto completing = job().completing().load();
                if (!completing && job().completing().compare_exchange_strong(completing, true)) {
                    executor_->schedule_task(flat_task{task_enum_tag<flat_task_kind::teardown>, std::addressof(job())});
                }
            }
            break;
    }
}

void dag_controller::impl::step_state(step const& v, step_state_kind new_state) {
    auto& current = steps_[v.id()].state_;
    if (current == new_state) {
        return;
    }
    DVLOG(1) <<
        v <<
        " state " <<
        to_string_view(current) <<
        " -> " <<
        to_string_view(new_state);

    current = new_state;
    on_state_change(v);
}

void dag_controller::impl::process_internal_events() {
    std::lock_guard guard{mutex_};
    while(!internal_events_.empty()) {
        auto& ie = internal_events_.front();
        auto v = graph_->find_step(ie.target());
        dispatch(*this, ie.kind(), ie, v.get());
        internal_events_.pop();
    }
}

void dag_controller::impl::init(model::graph& g) {
    std::lock_guard guard{mutex_};
    // assuming one graph per scheduler
    graph_ = &g;
    steps_.clear();
    while(! internal_events_.empty()) {
        internal_events_.pop();
    }
    for(auto&& v: g.steps()) {
        step_state(*v, step_state_kind::created);
    }
    graph_deactivated_ = false;
}

job_context& dag_controller::impl::job() noexcept {
    return *graph_->context()->job();  //TODO avoid request context from graph
}

void dag_controller::impl::schedule(model::graph& g) {
    init(g);
    if (! graph_->context()->job()) {
        g.context()->job(
            std::make_shared<job_context>(
                std::make_shared<scheduler::statement_scheduler>(maybe_shared_ptr{parent()})
            )
        );
    } else {
        // assuming no latch is used yet (it's done in wait_for_progress below), so it's safe to reset here.
        graph_->context()->job()->reset();
    }
    executor_->schedule_task(flat_task{task_enum_tag<scheduler::flat_task_kind::dag_events>, std::addressof(job())});

    // pass serial scheduler the control, or block waiting for parallel schedulers to proceed
    executor_->wait_for_progress(job());
}

void dag_controller::impl::start_running(step& v) {
    auto task_list = v.create_tasks();
    auto& tasks = steps_[v.id()];
    tasks.assign_slot(task_kind::main, task_list.size());
    step_state_table::slot_index slot = 0;
    for(auto& t : task_list) {
        executor_->schedule_task(flat_task{task_enum_tag<flat_task_kind::wrapped>, t, std::addressof(job())});
        tasks.register_task(task_kind::main, slot, t->id());
        tasks.task_state(t->id(), task_state_kind::running);
        ++slot;
    }
    step_state(v, step_state_kind::running);
}

void dag_controller::impl::start_pretask(step& v, step_state_table::slot_index index) {
    auto& tasks = steps_[v.id()];
    if (!tasks.uninitialized_slot(task_kind::pre, index)) {
        // task already started
        return;
    }
    if(auto view = v.create_pretask(index);!view.empty()) {
        auto& t = view.front();
        executor_->schedule_task(flat_task{task_enum_tag<flat_task_kind::wrapped>, t, std::addressof(job())});
        tasks.register_task(task_kind::pre, index, t->id());
        tasks.task_state(t->id(), task_state_kind::running);
    }
}

void dag_controller::impl::start_preparing(step& v) {
    auto& tasks = steps_[v.id()];
    std::vector<step_state_table::slot_index> not_started{tasks.list_uninitialized(task_kind::pre)};
    for(auto i : not_started) {
        start_pretask(v, i);
    }
    step_state(v, step_state_kind::preparing);
}

dag_controller::impl& dag_controller::impl::get_impl(dag_controller& arg) {
    return *arg.impl_;
}

class task_scheduler& dag_controller::impl::get_task_scheduler() {
    return *executor_;
}

dag_controller* dag_controller::impl::parent() const noexcept {
    return parent_;
}

} // namespace
