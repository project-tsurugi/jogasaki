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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <tateyama/context.h>
#include "thread_params.h"

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;

class flat_task {
public:
    using identity_type = std::size_t;
    static constexpr identity_type undefined_id = static_cast<identity_type>(-1);

    flat_task() = default;
    ~flat_task() = default;
    flat_task(flat_task const& other) = default;
    flat_task& operator=(flat_task const& other) = default;
    flat_task(flat_task&& other) noexcept = default;
    flat_task& operator=(flat_task&& other) noexcept = default;

    explicit flat_task(std::shared_ptr<model::task> origin) noexcept :
        origin_(std::move(origin))
    {}

    explicit flat_task(maybe_shared_ptr<request_context> request_context) noexcept :
        dag_scheduling_(true),
        request_context_(std::move(request_context))
    {}

    [[nodiscard]] std::shared_ptr<model::task> const& origin() const noexcept {
        return origin_;
    }

    void operator()(tateyama::context& ctx);

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] identity_type id() const;
private:
    std::shared_ptr<model::task> origin_{};
    bool dag_scheduling_{false};
    maybe_shared_ptr<request_context> request_context_{};
};

}



