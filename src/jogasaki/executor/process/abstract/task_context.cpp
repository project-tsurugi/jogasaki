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
#include "task_context.h"

#include <jogasaki/executor/process/abstract/work_context.h>

namespace jogasaki::executor::process::abstract {

void task_context::work_context(std::unique_ptr<class work_context> work_context) {
    work_context_ = std::move(work_context);
}

class work_context* task_context::work_context() const {
    return work_context_.get();
}

std::unique_ptr<class work_context> task_context::release_work() {
    return std::move(work_context_);
}

}


