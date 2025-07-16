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
#pragma once

#include <memory>
#include <glog/logging.h>

#include <jogasaki/executor/common/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::exchange {

class task : public common::task {
public:
    task() = default;

    task(
        request_context* context,
        step_type* src
    ) :
        common::task(context, src)
    {}

    [[nodiscard]] model::task_result operator()() override;
};

}



