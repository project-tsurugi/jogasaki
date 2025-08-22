/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include "dag_controller.h"

#include <utility>

#include <takatori/util/enum_tag.h>

#include <jogasaki/configuration.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/task_scheduler.h>

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

void dag_controller::schedule(model::graph &g, request_context& rctx) {
    impl_->schedule(g, rctx);
}

task_scheduler& dag_controller::get_task_scheduler() noexcept {
    return impl_->get_task_scheduler();
}

} // namespace
