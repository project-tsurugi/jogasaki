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

#include <map>

#include <model/step.h>
#include <model/task.h>
#include <scheduler/task_scheduler.h>
#include <scheduler/step_state.h>

namespace jogasaki::scheduler {

/*
 * @brief responsible for managing step status gathering tasks completion state and decide if prepare/run completes.
 * Each task status is stored in a slot. Slots can be assigned before the tasks are available.
 * Completion is decided based on whether the tasks in all slots for run/prepare completed.
 */
class step_state_table {
public:
    using entity_type = std::unordered_map<model::task::identity_type, task_state_kind>;
    using slots_type = std::vector<model::task::identity_type>;
    using slot_index = std::size_t;
    using kind = model::task_kind;

    inline static constexpr model::task::identity_type uninitialized_task_identity = -1;

    /*
     * @brief primary state of the step
     */
    step_state_kind state_;

    /*
     * @brief reserve more n slots to keep the task state
     */
    void assign_slot(kind k, std::size_t n) {
        if (k == kind::main) {
            main_slots_.resize(main_slots_.size() + n, uninitialized_task_identity);
        } else {
            sub_slots_.resize(sub_slots_.size() + n, uninitialized_task_identity);
        }
    }

    std::size_t slots(kind k) const {
        return k == kind::main ? main_slots_.size() : sub_slots_.size();
    }

    std::vector<slot_index> list_uninitialized(kind k) const {
        return list_uninitialized(k == kind::main ? main_slots_ : sub_slots_);
    }

    bool uninitialized_slot(kind k, slot_index ind) const {
        return uninitialized_slot(k == kind::main ? main_slots_ : sub_slots_, ind);
    }

    void register_task(kind k, slot_index slot, model::task::identity_type id) {
        register_task(k == kind::main ? main_slots_ : sub_slots_, slot, id);
    }

    kind task_state(model::task::identity_type id, task_state_kind st) {
        auto it = std::find(main_slots_.begin(), main_slots_.end(), id);
        if (it != main_slots_.end()) {
            main_status_[*it] = st;
            return kind::main;
        }
        it = std::find(sub_slots_.begin(), sub_slots_.end(), id);
        if (it != sub_slots_.end()) {
            sub_status_[*it] = st;
            return kind::pre;
        }
        throw std::domain_error("invalid identity");
    }

    bool completed(kind k) const {
        return k == kind::main ?
               tasks_completed(main_status_, main_slots_.size()) :
               tasks_completed(sub_status_, sub_slots_.size());
    }

private:
    slots_type main_slots_{};
    slots_type sub_slots_{};
    entity_type main_status_{};
    entity_type sub_status_{};

    bool tasks_completed(entity_type const& status, std::size_t count) const {
        if (status.size() != count) {
            return false;
        }
        for(auto& p : status) {
            if (p.second != task_state_kind::completed) {
                return false;
            }
        }
        return true;
    }

    void register_task(slots_type & slots, slot_index slot, model::task::identity_type id) {
        if (slot >= slots.size()) {
            throw std::domain_error("insufficient slots");
        }
        slots[slot] = id;
    }

    std::vector<slot_index> list_uninitialized(slots_type const& slots) const {
        std::vector<slot_index> v{};
        v.reserve(slots.size());
        for(slot_index i = 0; i < slots.size(); ++i) {
            if (slots[i] == uninitialized_task_identity) {
                v.emplace_back(i);
            }
        }
        return v;
    }

    bool uninitialized_slot(slots_type const& slots, slot_index ind) const {
        if (ind >= slots.size()) {
            return true;
        }
        return slots[ind] == uninitialized_task_identity;
    }
};

}
