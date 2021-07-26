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
#include "statement_scheduler_impl.h"

#include <takatori/util/downcast.h>

#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include "statement_scheduler.h"

namespace jogasaki::scheduler {

using takatori::util::unsafe_downcast;

statement_scheduler::impl::impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler) :
    dag_controller_(cfg, scheduler),
    cfg_(std::move(cfg))
{}

statement_scheduler::impl::impl(std::shared_ptr<configuration> cfg) :
    dag_controller_(cfg),
    cfg_(std::move(cfg))
{}

void statement_scheduler::impl::schedule(model::statement const& s, request_context& context) {
    using kind = model::statement_kind;
    switch(s.kind()) {
        case kind::execute: {
            auto& g = unsafe_downcast<executor::common::execute>(s).operators();
            dag_controller_.schedule(g);
            break;
        }
        case kind::write: {
            auto& w = unsafe_downcast<executor::common::write>(s);
            w(context);
            break;
        }
    }
}

dag_controller& statement_scheduler::impl::controller() noexcept {
    return dag_controller_;
}

statement_scheduler::impl& statement_scheduler::impl::get_impl(statement_scheduler& arg) {
    return *arg.impl_;
}
} // namespace
