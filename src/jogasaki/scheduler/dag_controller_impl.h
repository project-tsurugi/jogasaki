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
#pragma once

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

/**
 * @brief Dependency Graph Scheduler
 */
class cache_align dag_controller::impl {
public:
    using steps_status = std::unordered_map<step::identity_type, step_state_table>;

    impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler) :
        cfg_(std::move(cfg)),
        executor_(std::addressof(scheduler))
    {}

    explicit impl(std::shared_ptr<configuration> cfg) :
        cfg_(std::move(cfg)),
        executor_(cfg_->single_thread() ?
            std::shared_ptr<task_scheduler>(std::make_shared<serial_task_scheduler>()) :
            std::shared_ptr<task_scheduler>(
                std::make_shared<parallel_task_scheduler>(thread_params(cfg_))
            )
        )
    {}

    /*
     * @brief handles providing event
     */
    void operator()(enum_tag_t<event_kind::providing>, event& e);

    /*
     * @brief handles main_completed event
     */
    void operator()(enum_tag_t<event_kind::task_completed>, event& e);

    /*
     * @brief handles completion_instructed event
     */
    void operator()(enum_tag_t<event_kind::completion_instructed>, event& e);

    /*
     * @brief handles activate event
     */
    void operator()(enum_tag_t<internal_event_kind::activate>, internal_event& ie, step* s);

    /*
     * @brief handles prepare event
     */
    void operator()(enum_tag_t<internal_event_kind::prepare>, internal_event& ie, step* s);

    /*
     * @brief handles consume event
     */
    void operator()(enum_tag_t<internal_event_kind::consume>, internal_event& ie, step* s);

    /*
     * @brief handles deactivate event
     */
    void operator()(enum_tag_t<internal_event_kind::deactivate>, internal_event& ie, step* s);

    /*
     * @brief handles propagate_downstream_completing event
     */
    void operator()(enum_tag_t<internal_event_kind::propagate_downstream_completing>, internal_event& ie, step* s);

    bool all_steps_deactivated(model::graph& g) {
        for(auto&& v: g.steps()) {
            if (steps_[v->id()].state_ < step_state_kind::deactivated) {
                return false;
            }
        }
        return true;
    }

    // no upstreams or upstream equals or past st
    bool all_upstream_steps_past(step const& s, step_state_kind st) {
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
    bool all_downstream_steps_past(step const& s, step_state_kind st) {
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

    bool ports_completed(takatori::util::sequence_view<std::unique_ptr<model::port> const> ports) {
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

    bool output_ports_activated(step const& s) {
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

    bool main_input_completed(step const& s) {
        return ports_completed(s.input_ports());
    }

    bool sub_input_completed(step const& s) {
        return ports_completed(s.subinput_ports());
    }

    bool input_completed(step const& s) {
        return main_input_completed(s) && sub_input_completed(s);
    }

    void on_state_change(step const& s) {
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

    // generate internal events on step state change
    void check_and_generate_internal_events(step const& s) {
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
                    graph_->context()->channel()->close();
                }
                break;
        }
    }

    /*
     * @brief transition state for the given step
     */
    void step_state(step const& v, step_state_kind new_state) {
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

    void schedule(model::graph &g) {
        // assuming one graph per scheduler
        graph_ = &g;
        steps_.clear();
        for(auto&& v: g.steps()) {
            step_state(*v, step_state_kind::created);
        }
        graph_deactivated_ = all_steps_deactivated(g);
        auto& ch = *g.context()->channel();
        while(!graph_deactivated_) {
            while(!internal_events_.empty()) {
                auto& ie = internal_events_.front();
                auto v = g.find_step(ie.target());
                dispatch(*this, ie.kind(), ie, v.get());
                internal_events_.pop();
            }
            // watch external event channel after internal ones complete
            event ev{};
            if(ch.pop(ev)) {
                dispatch(*this, ev.kind(), ev);
            }

            // For serial scheduler, give control here in order to
            // simulate tasks execution background so that state changes and proceeds
            executor_->wait_for_progress();
        }
    }

    void start_running(step& v) {
        auto task_list = v.create_tasks();
        auto& tasks = steps_[v.id()];
        tasks.assign_slot(task_kind::main, task_list.size());
        step_state_table::slot_index slot = 0;
        for(auto& t : task_list) {
            executor_->schedule_task(t);
            tasks.register_task(task_kind::main, slot, t->id());
            tasks.task_state(t->id(), task_state_kind::running);
            ++slot;
        }
        step_state(v, step_state_kind::running);
    }

    void start_pretask(step& v, step_state_table::slot_index index) {
        auto& tasks = steps_[v.id()];
        if (!tasks.uninitialized_slot(task_kind::pre, index)) {
            // task already started
            return;
        }
        if(auto view = v.create_pretask(index);!view.empty()) {
            auto& t = view.front();
            executor_->schedule_task(t);
            tasks.register_task(task_kind::pre, index, t->id());
            tasks.task_state(t->id(), task_state_kind::running);
        }
    }

    void start_preparing(step& v) {
        auto& tasks = steps_[v.id()];
        std::vector<step_state_table::slot_index> not_started{tasks.list_uninitialized(task_kind::pre)};
        for(auto i : not_started) {
            start_pretask(v, i);
        }
        step_state(v, step_state_kind::preparing);
    }

private:
    std::shared_ptr<configuration> cfg_{};
    model::graph *graph_{};
    steps_status steps_{};
    std::queue<internal_event> internal_events_{};
    bool graph_deactivated_{false};
    maybe_shared_ptr<task_scheduler> executor_{};
};

} // namespace
