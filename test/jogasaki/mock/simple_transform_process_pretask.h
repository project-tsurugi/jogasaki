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

#include <glog/logging.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>

#include <jogasaki/mock/mock_task.h>

namespace jogasaki::executor {

class simple_transform_process_pretask : public mock_task {
public:

    simple_transform_process_pretask() = default;
    ~simple_transform_process_pretask() override = default;
    simple_transform_process_pretask(simple_transform_process_pretask&& other) noexcept = default;
    simple_transform_process_pretask& operator=(simple_transform_process_pretask&& other) noexcept = default;

    simple_transform_process_pretask(request_context* context, model::step* src) : mock_task(context, src, true) {}
    void execute() override {
        LOG(INFO) << *this << " simple_transform_process_pretask executed. count: " << count_;
    }
};

}

