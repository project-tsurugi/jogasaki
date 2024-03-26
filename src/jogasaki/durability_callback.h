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

#include <atomic>
#include <memory>

#include <sharksfin/api.h>

#include <jogasaki/durability_manager.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>

namespace jogasaki {

namespace api::impl {
class database;
}

/**
 * @brief durability callback
 */
class durability_callback {
public:
    using marker_type = ::sharksfin::durability_marker_type;

    using element_reference_type = durability_manager::element_reference_type;

    durability_callback() = default;
    ~durability_callback() = default;
    durability_callback(durability_callback const& other) = default;
    durability_callback& operator=(durability_callback const& other) = default;
    durability_callback(durability_callback&& other) noexcept = default;
    durability_callback& operator=(durability_callback&& other) noexcept = default;

    explicit durability_callback(api::impl::database& db);

    void operator()(marker_type marker);

private:
    api::impl::database* db_{};
    durability_manager* manager_{};
    scheduler::task_scheduler* scheduler_{};

};

}

