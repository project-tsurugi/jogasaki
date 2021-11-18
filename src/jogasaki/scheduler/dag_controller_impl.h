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
#include <jogasaki/internal_event.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/step_state_table.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/latch.h>
#include "serial_task_scheduler.h"
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

    /**
     * @brief create new object
     */
    impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler, dag_controller* parent);

    /**
     * @brief create new object
     */
    explicit impl(std::shared_ptr<configuration> cfg, dag_controller* parent);

    /**
     * @brief handles providing event
     */
    void operator()(enum_tag_t<event_kind::providing>, event& e);

    /**
     * @brief handles main_completed event
     */
    void operator()(enum_tag_t<event_kind::task_completed>, event& e);

    /**
     * @brief handles completion_instructed event
     */
    void operator()(enum_tag_t<event_kind::completion_instructed>, event& e);

    /**
     * @brief handles activate event
     */
    void operator()(enum_tag_t<internal_event_kind::activate>, internal_event& ie, step* s);

    /**
     * @brief handles prepare event
     */
    void operator()(enum_tag_t<internal_event_kind::prepare>, internal_event& ie, step* s);

    /**
     * @brief handles consume event
     */
    void operator()(enum_tag_t<internal_event_kind::consume>, internal_event& ie, step* s);

    /**
     * @brief handles deactivate event
     */
    void operator()(enum_tag_t<internal_event_kind::deactivate>, internal_event& ie, step* s);

    /**
     * @brief handles propagate_downstream_completing event
     */
    void operator()(enum_tag_t<internal_event_kind::propagate_downstream_completing>, internal_event& ie, step* s);

    /**
     * @brief check internal events and process all of them
     */
    void process_internal_events();

    /**
     * @brief schedule the dag
     * @note this function is deprecated and will be used solely for testing
     * @param g the dag to be processed
     */
    void schedule(model::graph &g);

    /**
     * @brief accessor to the task scheduler
     */
    task_scheduler& get_task_scheduler();

    /**
     * @brief accessor to the impl
     */
    static dag_controller::impl& get_impl(dag_controller& arg);

    /**
     * @brief set the graph to run as the job
     * @param g the dag object to process
     */
    void init(model::graph& g);

    /**
     * @brief accessor to the owner object that holds this impl.
     */
    dag_controller* parent() const noexcept;

    /**
     * @brief accessor to the configuration
     */
    [[nodiscard]] configuration const& cfg() const noexcept;

private:
    std::shared_ptr<configuration> cfg_{};
    model::graph *graph_{};
    steps_status steps_{};
    std::queue<internal_event> internal_events_{};
    bool graph_deactivated_{false};
    maybe_shared_ptr<task_scheduler> executor_{};
    std::mutex mutex_{};
    dag_controller* parent_{};

    bool all_steps_deactivated(model::graph& g);
    // no upstreams or upstream equals or past st
    bool all_upstream_steps_past(step const& s, step_state_kind st);
    bool all_downstream_steps_past(step const& s, step_state_kind st);
    bool ports_completed(takatori::util::sequence_view<std::unique_ptr<model::port> const> ports);
    bool output_ports_activated(step const& s);
    bool main_input_completed(step const& s);
    bool sub_input_completed(step const& s);
    bool input_completed(step const& s);
    void on_state_change(step const& s);
    // generate internal events on step state change
    void check_and_generate_internal_events(step const& s);
    void step_state(step const& v, step_state_kind new_state);
    void start_running(step& v);
    void start_pretask(step& v, step_state_table::slot_index index);
    void start_preparing(step& v);
    job_context& job() noexcept;
};

} // namespace
