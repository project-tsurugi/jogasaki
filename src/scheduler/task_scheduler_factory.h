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

#include <scheduler/task_scheduler.h>
#include <scheduler/single_thread_task_scheduler.h>
#include <scheduler/multi_thread_task_scheduler.h>

namespace dc::scheduler {

class task_scheduler_factory {
    using kind = task_scheduler_kind;
public:
    static std::unique_ptr<task_scheduler> create(kind k) {
        switch(k) {
            case kind::single_thread:
                return std::make_unique<single_thread_task_scheduler>();
            case kind::multi_thread:
                return std::make_unique<multi_thread_task_scheduler>();
        }
        std::abort();
    }
};

}

