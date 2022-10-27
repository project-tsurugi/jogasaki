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

#include <memory>

#include <jogasaki/scheduler/flat_task.h>

namespace jogasaki::scheduler {

using callback = std::function<void(status, std::string_view)>;

using task_body_type = std::function<void()>;

namespace details {

class custom_task : public model::task {
public:
    custom_task();

    explicit custom_task(task_body_type body, bool transactional_io = false);

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] identity_type id() const override;

    /**
     * @brief task body
     * @return task_result to instruct scheduler
     */
    [[nodiscard]] model::task_result operator()() override;

    /**
     * @brief accessor to I/O operation property of the task
     * @return whether the task contains transactional I/O operations that requires special handling in scheduling
     */
    [[nodiscard]] bool has_transactional_io() override;

protected:
    std::ostream& write_to(std::ostream& out) const override;

private:
    cache_align static inline std::atomic_size_t id_src = 20000;
    identity_type id_{id_src++};
    task_body_type body_{};
    bool transactional_io_{};
};

} // namespace details

flat_task create_custom_task(
    request_context* rctx,
    task_body_type body,
    bool has_transaction_io = false
);

} // namespace jogasaki::scheduler
