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

#include <unordered_map>
#include <memory>

#include <jogasaki/model/task.h>
#include <jogasaki/utils/interference_size.h>
#include "task_scheduler.h"

namespace jogasaki::scheduler {

/*
 * @brief task scheduler using multiple threads
 */
class cache_align single_thread_task_scheduler : public task_scheduler {
public:
    using entity_type = std::unordered_map<model::task::identity_type, std::weak_ptr<model::task>>;

    void schedule_task(std::shared_ptr<model::task> const& t) override;

    void wait_for_progress() override;

    void start() override;

    void stop() override;

    [[nodiscard]] task_scheduler_kind kind() const noexcept override;
private:
    entity_type tasks_{};
};

}



