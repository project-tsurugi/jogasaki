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
#include "task_context_pool.h"

#include <memory>
#include <utility>
#include <vector>

#include <jogasaki/executor/process/abstract/task_context.h>

namespace jogasaki::executor::process::impl {

task_context_pool::task_context_pool(std::vector<std::shared_ptr<abstract::task_context>> contexts) {
    for(auto&& c : contexts) {
        push(std::move(c));
    }
}

void task_context_pool::push(std::shared_ptr<abstract::task_context> context) {
    contexts_.emplace(std::move(context));
}

std::shared_ptr<abstract::task_context> task_context_pool::pop() {
    std::shared_ptr<abstract::task_context> context{};
    contexts_.pop(context);
    return context;
}

}


