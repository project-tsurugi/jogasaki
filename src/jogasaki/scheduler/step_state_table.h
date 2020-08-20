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

#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/step_state.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

/*
 * @brief state table responsible for managing step status by gathering tasks completion state
 * and decide if prepare/run phase completes.
 * @details Each task status is stored in a slot. Slots can be assigned before the tasks are available.
 * Completion is decided based on whether the tasks in all slots for run/prepare completed.
 */
class cache_align step_state_table {
public:
    using entity_type = std::unordered_map<model::task::identity_type, task_state_kind>;
    using slots_type = std::vector<model::task::identity_type>;
    using slot_index = std::size_t;
    using kind = model::task_kind;

    inline static constexpr model::task::identity_type uninitialized_task_identity = -1;

    /*
     * @brief primary state of the step
     */
    step_state_kind state_{step_state_kind::uninitialized}; //NOLINT

    /*
     * @brief reserve more n slots to keep the task state
     */
    void assign_slot(kind k, std::size_t n);

    [[nodiscard]] std::size_t slots(kind k) const;

    [[nodiscard]] std::vector<slot_index> list_uninitialized(kind k) const;

    [[nodiscard]] bool uninitialized_slot(kind k, slot_index ind) const;

    void register_task(kind k, slot_index slot, model::task::identity_type id);

    kind task_state(model::task::identity_type id, task_state_kind st);

    [[nodiscard]] bool completed(kind k) const;

private:
    slots_type main_slots_{};
    slots_type sub_slots_{};
    entity_type main_status_{};
    entity_type sub_status_{};

    [[nodiscard]] bool tasks_completed(entity_type const& status, std::size_t count) const;

    void register_task(slots_type & slots, slot_index slot, model::task::identity_type id);

    [[nodiscard]] std::vector<slot_index> list_uninitialized(slots_type const& slots) const;

    [[nodiscard]] bool uninitialized_slot(slots_type const& slots, slot_index ind) const;
};

}
