/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>

#include <jogasaki/mock/mock_task.h>

namespace jogasaki::executor {

class simple_emit_process_task : public mock_task {
public:
    simple_emit_process_task() = default;
    ~simple_emit_process_task() override = default;
    simple_emit_process_task(simple_emit_process_task&& other) noexcept = default;
    simple_emit_process_task& operator=(simple_emit_process_task&& other) noexcept = default;
    simple_emit_process_task(request_context* context, model::step* src) : mock_task(context, src) {}
    void execute() override {
        LOG(INFO) << *this << " simple_emit_process_main_task executed. count: " << count_;
    }
};

}



