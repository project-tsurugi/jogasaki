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

#include <jogasaki/utils/interference_size.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/statement_scheduler.h>

namespace jogasaki::scheduler {

class cache_align statement_scheduler::impl {
public:
    impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler);

    explicit impl(maybe_shared_ptr<dag_controller> controller);

    explicit impl(std::shared_ptr<configuration> cfg);

    void schedule(
        model::statement const& s,
        request_context& context
    );

    [[nodiscard]] dag_controller& controller() noexcept;

    static statement_scheduler::impl& get_impl(statement_scheduler& arg);

private:
    maybe_shared_ptr<dag_controller> dag_controller_{};
    std::shared_ptr<configuration> cfg_{};
};

} // namespace
