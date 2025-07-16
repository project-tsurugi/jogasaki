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

#include <cstddef>
#include <memory>
#include <glog/logging.h>

#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/port_indices.h>

namespace jogasaki::executor {

class mock_task : public common::task {
public:

    mock_task() = default;
    ~mock_task() override = default;
    mock_task(mock_task&& other) noexcept = default;
    mock_task& operator=(mock_task&& other) noexcept = default;
    mock_task(
        request_context* context,
        model::step* src,
        bool is_pretask = false
    ) :
        context_(context),
        src_(src),
        is_pretask_(is_pretask)
    {}

    model::task_result operator()() override;
    virtual void execute() = 0;
protected:
    request_context* context_{};
    model::step* src_{};
    bool is_pretask_{false};
    std::size_t count_{0};
    std::size_t limit_{3};

    void notify_downstream();
};

}
