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
#include "dag_controller_impl.h"

namespace jogasaki::scheduler {

using step = model::step;
using task_kind = model::task_kind;

template<auto Kind>
using enum_tag_t = takatori::util::enum_tag_t<Kind>;

dag_controller::dag_controller() : dag_controller(std::make_shared<configuration>()) {}
dag_controller::dag_controller(
    std::shared_ptr<configuration> cfg,
    task_scheduler& scheduler
) :
    impl_(std::make_unique<impl>(std::move(cfg), scheduler, this))
{}

dag_controller::dag_controller(std::shared_ptr<configuration> cfg) :
    impl_(std::make_unique<impl>(std::move(cfg), this))
{};

dag_controller::~dag_controller() = default;

void dag_controller::schedule(model::graph &g) {
    return impl_->schedule(g);
}

task_scheduler& dag_controller::get_task_scheduler() noexcept {
    return impl_->get_task_scheduler();
}

} // namespace
