/*
 * Copyright 2018-2022 tsurugi project.
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

#include <tbb/concurrent_priority_queue.h>

#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/durability_manager.h>

namespace jogasaki {

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

    durability_callback(
        durability_manager& mgr,
        scheduler::task_scheduler& scheduler
    ) :
        manager_(std::addressof(mgr)),
        scheduler_(std::addressof(scheduler))
    {}

    void operator()(marker_type marker) {
        create_request_context();
        scheduler_->schedule_task(
            scheduler::create_custom_task(
                nullptr,
                [mgr=manager_, marker](){
                    if(mgr->update_current_marker(
                        marker,
                        [](element_reference_type e){
                            auto& tx = e.first;
                            (void) tx;
                            auto& rctx = e.second;
                            scheduler::submit_teardown(*rctx.get(), false, true);
                        })) {
                        return model::task_result::complete;
                    }
                    return model::task_result::yield;
                },
                false,
                false
            ),
            scheduler::schedule_option{scheduler::schedule_policy_kind::suspended_worker}
        );
    }

private:
    durability_manager* manager_{};
    scheduler::task_scheduler* scheduler_{};

};

}

