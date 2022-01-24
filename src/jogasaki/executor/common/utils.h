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

#include <atomic>
#include <ostream>
#include <string_view>

#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>

namespace jogasaki::executor::common {

// common utility functions

template <class ...Args>
void send_event(request_context& context, Args...args) {
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*context.stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process_internal_events();  // let's handle internal event first
    event ev{args...};
    dispatch(dc, ev.kind(), ev);
}

}



